// Command reach is a passive Swarm mainnet reachability probe.
//
// It joins the network as a light node, mounts the hive protocol to
// collect gossiped peer records, then dials a neighborhood-spread
// sample of them once each, recording dial+handshake wall time.
package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/addressbook"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/hive"
	"github.com/ethersphere/bee/v2/pkg/keystore/file"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/pricing"
	"github.com/ethersphere/bee/v2/pkg/statestore/leveldb"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	networkID = 1 // Swarm mainnet

	// Guards a throwaway probe identity holding no funds; constant on
	// purpose so the key survives restarts without prompting.
	keyPassword = "directswarm-reach"

	overlayNonceKey = "overlayV2_nonce" // same key bee uses, so a data-dir is portable

	defaultBootnode = "/dnsaddr/mainnet.ethswarm.org"

	dialTimeout = 20 * time.Second
)

type stringsFlag []string

func (f *stringsFlag) String() string { return strings.Join(*f, ",") }

func (f *stringsFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}

// collector receives overlays from hive's AddPeers path (where bee
// would hand them to kademlia) and just remembers them.
type collector struct {
	mu   sync.Mutex
	seen map[string]struct{}
	list []swarm.Address
}

func (c *collector) add(addrs ...swarm.Address) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range addrs {
		if _, ok := c.seen[a.ByteString()]; ok {
			continue
		}
		c.seen[a.ByteString()] = struct{}{}
		c.list = append(c.list, a)
	}
}

func (c *collector) snapshot() []swarm.Address {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]swarm.Address(nil), c.list...)
}

type record struct {
	overlay   swarm.Address
	underlays []ma.Multiaddr
	prefix    uint16 // first 9 bits of the overlay: neighborhood at depth 9
}

type result struct {
	rec    record
	ok     bool
	rtt    time.Duration
	errStr string
}

func main() {
	var (
		dataDir    = flag.String("data-dir", "./reach-data", "directory for keys and statestore")
		listen     = flag.String("listen", ":0", "libp2p listen address (host:port)")
		listenSecs = flag.Int("listen-secs", 90, "seconds to passively collect hive records")
		sampleN    = flag.Int("sample", 50, "max records to dial")
		dialRate   = flag.Float64("dial-rate", 2, "dial attempts per second")
		out        = flag.String("out", "reach.csv", "CSV output path")
		maxMins    = flag.Int("max-mins", 15, "global timeout in minutes")
		bootnodes  stringsFlag
	)
	flag.Var(&bootnodes, "bootnode", "bootnode multiaddr (repeatable)")
	flag.Parse()
	if len(bootnodes) == 0 {
		bootnodes = stringsFlag{defaultBootnode}
	}
	if *dialRate <= 0 {
		*dialRate = 2
	}

	if err := run(*dataDir, *listen, bootnodes, *listenSecs, *sampleN, *dialRate, *out, *maxMins); err != nil {
		fmt.Fprintln(os.Stderr, "reach:", err)
		os.Exit(1)
	}
}

// the libp2p service dereferences its notifier unconditionally in the
// reachability worker (and the incoming-connection path), so even a
// kademlia-less client must install one
type noopNotifier struct{}

func (noopNotifier) Pick(p2p.Peer) bool                             { return true }
func (noopNotifier) Connected(context.Context, p2p.Peer, bool) error { return nil }
func (noopNotifier) Disconnected(p2p.Peer)                          {}
func (noopNotifier) Announce(context.Context, swarm.Address, bool) error { return nil }
func (noopNotifier) AnnounceTo(context.Context, swarm.Address, swarm.Address, bool) error {
	return nil
}
func (noopNotifier) UpdateReachability(p2p.ReachabilityStatus)      {}
func (noopNotifier) Reachable(swarm.Address, p2p.ReachabilityStatus) {}

type noopThresholdObserver struct{ log log.Logger }

func (o noopThresholdObserver) NotifyPaymentThreshold(peer swarm.Address, threshold *big.Int) error {
	o.log.Debug("payment threshold announced", "peer", peer, "threshold", threshold)
	return nil
}

func run(dataDir, listen string, bootnodes []string, listenSecs, sampleN int, dialRate float64, out string, maxMins int) error {
	logger := log.NewLogger("reach")

	// Lifecycle context for the libp2p service is separate from the
	// deadline context so the summary/CSV phase still runs after the
	// global timeout fires.
	svcCtx, svcCancel := context.WithCancel(context.Background())
	defer svcCancel()
	runCtx, runCancel := context.WithTimeout(svcCtx, time.Duration(maxMins)*time.Minute)
	defer runCancel()

	keys := file.New(filepath.Join(dataDir, "keys"))
	swarmKey, _, err := keys.Key("swarm", keyPassword, crypto.EDGSecp256_K1)
	if err != nil {
		return fmt.Errorf("swarm key: %w", err)
	}
	libp2pKey, _, err := keys.Key("libp2p_v2", keyPassword, crypto.EDGSecp256_R1)
	if err != nil {
		return fmt.Errorf("libp2p key: %w", err)
	}
	signer := crypto.NewDefaultSigner(swarmKey)

	stateStore, err := leveldb.NewStateStore(filepath.Join(dataDir, "statestore"), logger)
	if err != nil {
		return fmt.Errorf("statestore: %w", err)
	}
	defer stateStore.Close()

	nonce := make([]byte, 32)
	if err := stateStore.Get(overlayNonceKey, &nonce); err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("read nonce: %w", err)
		}
		if _, err := rand.Read(nonce); err != nil {
			return fmt.Errorf("generate nonce: %w", err)
		}
		if err := stateStore.Put(overlayNonceKey, nonce); err != nil {
			return fmt.Errorf("persist nonce: %w", err)
		}
	}

	overlay, err := crypto.NewOverlayAddress(swarmKey.PublicKey, networkID, nonce)
	if err != nil {
		return fmt.Errorf("overlay address: %w", err)
	}
	logger.Info("probe identity", "overlay", overlay)

	ab := addressbook.New(stateStore)
	lightNodes := lightnode.NewContainer(overlay)

	// Enabled:false yields a noop tracer.
	tracer, tracerCloser, err := tracing.NewTracer(&tracing.Options{Enabled: false})
	if err != nil {
		return fmt.Errorf("tracer: %w", err)
	}
	defer tracerCloser.Close()

	p2ps, err := libp2p.New(svcCtx, signer, networkID, overlay, listen, ab, stateStore, lightNodes, logger, tracer, libp2p.Options{
		PrivateKey: libp2pKey,
		FullNode:   false,
		Nonce:      nonce,
		// needed to dial the co-resident seed node on loopback; without it
		// bee's dialer drops private/loopback underlays as unsupported
		AllowPrivateCIDRs: true,
	})
	if err != nil {
		return fmt.Errorf("p2p service: %w", err)
	}
	defer p2ps.Close()
	p2ps.SetPickyNotifier(noopNotifier{})

	// Hive writes verified gossip records into the addressbook itself;
	// the AddPeers handler (kademlia's slot in bee) only tells us which
	// overlays arrived. No kademlia: we collect, we do not manage topology.
	coll := &collector{seen: make(map[string]struct{})}
	hiveSvc := hive.New(p2ps, ab, networkID, overlay, logger, hive.Options{})
	hiveSvc.SetAddPeersHandler(coll.add)
	hiveSpec := hiveSvc.Protocol()
	// diagnostic: log every inbound hive stream before delegating, to
	// distinguish "never sent" from "arrived but dropped"
	for i := range hiveSpec.StreamSpecs {
		inner := hiveSpec.StreamSpecs[i].Handler
		name := hiveSpec.StreamSpecs[i].Name
		hiveSpec.StreamSpecs[i].Handler = func(ctx context.Context, p p2p.Peer, s p2p.Stream) error {
			logger.Info("hive stream arrived", "stream", name, "from", p.Address)
			err := inner(ctx, p, s)
			if err != nil {
				logger.Info("hive stream handler error", "stream", name, "from", p.Address, "error", err)
			}
			return err
		}
	}
	if err := p2ps.AddProtocol(hiveSpec); err != nil {
		return fmt.Errorf("mount hive: %w", err)
	}
	defer hiveSvc.Close()

	// stock peers open a pricing stream immediately after connect to
	// announce their payment threshold; a peer that can't accept that
	// stream is treated as broken and disconnected — so the probe must
	// mount pricing even though it never settles (values = bee light-node
	// defaults: threshold 13.5M, lightFactor 10, 2×lightRefreshRate min)
	paymentThreshold := big.NewInt(13_500_000)
	lightPaymentThreshold := new(big.Int).Div(paymentThreshold, big.NewInt(10))
	minThreshold := big.NewInt(2 * 450_000)
	pricingSvc := pricing.New(p2ps, logger, paymentThreshold, lightPaymentThreshold, minThreshold)
	pricingSvc.SetPaymentThresholdObserver(noopThresholdObserver{logger})
	if err := p2ps.AddProtocol(pricingSvc.Protocol()); err != nil {
		return fmt.Errorf("mount pricing: %w", err)
	}

	if err := p2ps.Ready(); err != nil {
		return fmt.Errorf("p2p ready: %w", err)
	}

	connectBootnodes(runCtx, logger, p2ps, bootnodes)

	logger.Info("collecting hive records", "seconds", listenSecs)
	listenDone := time.After(time.Duration(listenSecs) * time.Second)
listenLoop:
	for {
		select {
		case <-time.After(10 * time.Second):
			// diagnostic: are our seed connections surviving, or are we
			// being kicked to make room for other light peers?
			logger.Info("still listening", "connected_peers", len(p2ps.Peers()), "records", len(coll.snapshot()))
		case <-listenDone:
			break listenLoop
		case <-runCtx.Done():
			break listenLoop
		}
	}

	records := resolveRecords(ab, coll.snapshot(), logger)
	sampled := sampleAcrossNeighborhoods(records, sampleN)
	neighborhoods := countNeighborhoods(records)

	results := probe(runCtx, logger, p2ps, sampled, dialRate)

	if err := writeCSV(out, results); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}
	summarize(os.Stdout, len(records), neighborhoods, results)
	return nil
}

func connectBootnodes(ctx context.Context, logger log.Logger, p2ps *libp2p.Service, bootnodes []string) {
	connected := 0
	for _, bn := range bootnodes {
		if connected >= 3 || ctx.Err() != nil {
			return
		}
		addr, err := ma.NewMultiaddr(bn)
		if err != nil {
			logger.Warning("invalid bootnode address", "addr", bn, "error", err)
			continue
		}
		// p2p.Discover resolves /dnsaddr records recursively.
		_, err = p2p.Discover(ctx, addr, func(a ma.Multiaddr) (bool, error) {
			dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
			defer cancel()
			bzzAddr, err := p2ps.Connect(dialCtx, []ma.Multiaddr{a})
			if err != nil {
				if errors.Is(err, p2p.ErrAlreadyConnected) {
					return false, nil
				}
				logger.Debug("bootnode connect failed", "addr", a, "error", err)
				return false, nil
			}
			logger.Info("connected to bootnode", "overlay", bzzAddr.Overlay)
			connected++
			return connected >= 3, nil
		})
		if err != nil {
			logger.Warning("bootnode discovery failed", "addr", bn, "error", err)
		}
	}
	if connected == 0 {
		logger.Warning("no bootnode connections established")
	}
}

// depth9Prefix returns the first 9 bits of the overlay address.
func depth9Prefix(a swarm.Address) uint16 {
	b := a.Bytes()
	if len(b) < 2 {
		return 0
	}
	return uint16(b[0])<<1 | uint16(b[1])>>7
}

func resolveRecords(ab addressbook.Interface, overlays []swarm.Address, logger log.Logger) []record {
	records := make([]record, 0, len(overlays))
	for _, o := range overlays {
		addr, _, err := ab.Get(o)
		if err != nil {
			logger.Debug("addressbook lookup failed", "overlay", o, "error", err)
			continue
		}
		if len(addr.Underlays) == 0 {
			continue
		}
		records = append(records, record{
			overlay:   o,
			underlays: addr.Underlays,
			prefix:    depth9Prefix(o),
		})
	}
	return records
}

func countNeighborhoods(records []record) int {
	set := make(map[uint16]struct{})
	for _, r := range records {
		set[r.prefix] = struct{}{}
	}
	return len(set)
}

// sampleAcrossNeighborhoods picks up to n records round-robin across
// depth-9 prefixes so the sample covers as many neighborhoods as possible.
func sampleAcrossNeighborhoods(records []record, n int) []record {
	groups := make(map[uint16][]record)
	for _, r := range records {
		groups[r.prefix] = append(groups[r.prefix], r)
	}
	prefixes := make([]uint16, 0, len(groups))
	for p := range groups {
		prefixes = append(prefixes, p)
	}
	sort.Slice(prefixes, func(i, j int) bool { return prefixes[i] < prefixes[j] })

	var sampled []record
	for len(sampled) < n {
		progress := false
		for _, p := range prefixes {
			if len(groups[p]) == 0 {
				continue
			}
			sampled = append(sampled, groups[p][0])
			groups[p] = groups[p][1:]
			progress = true
			if len(sampled) == n {
				break
			}
		}
		if !progress {
			break
		}
	}
	return sampled
}

func probe(ctx context.Context, logger log.Logger, p2ps *libp2p.Service, sampled []record, rate float64) []result {
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	results := make([]result, 0, len(sampled))
	for i, rec := range sampled {
		if i > 0 {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				logger.Warning("global timeout during probe phase", "done", i, "sampled", len(sampled))
				return results
			}
		}
		if ctx.Err() != nil {
			return results
		}

		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		start := time.Now()
		bzzAddr, err := p2ps.Connect(dialCtx, rec.underlays)
		rtt := time.Since(start)
		cancel()

		res := result{rec: rec, rtt: rtt}
		switch {
		case err == nil:
			res.ok = true
			_ = p2ps.Disconnect(bzzAddr.Overlay, "reach probe complete")
		case errors.Is(err, p2p.ErrAlreadyConnected):
			// Bootnode gossip can include peers we are already connected
			// to; count reachable but the timing is not a fresh handshake.
			res.ok = true
			res.errStr = "already_connected"
		default:
			res.errStr = err.Error()
		}
		results = append(results, res)
	}
	return results
}

func writeCSV(path string, results []result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write([]string{"overlay_hex", "underlay", "neighborhood_prefix_hex", "dial_ok", "rtt_ms", "error"}); err != nil {
		return err
	}
	for _, r := range results {
		underlays := make([]string, len(r.rec.underlays))
		for i, u := range r.rec.underlays {
			underlays[i] = u.String()
		}
		ok := "0"
		if r.ok {
			ok = "1"
		}
		row := []string{
			r.rec.overlay.String(),
			strings.Join(underlays, "|"),
			fmt.Sprintf("%03x", r.rec.prefix),
			ok,
			strconv.FormatInt(r.rtt.Milliseconds(), 10),
			r.errStr,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func summarize(w *os.File, nodesSeen, neighborhoods int, results []result) {
	reachable := 0
	var rtts []time.Duration
	for _, r := range results {
		if r.ok {
			reachable++
			if r.errStr == "" { // exclude already_connected from timing
				rtts = append(rtts, r.rtt)
			}
		}
	}
	fraction := 0.0
	if len(results) > 0 {
		fraction = float64(reachable) / float64(len(results))
	}
	median := time.Duration(0)
	if len(rtts) > 0 {
		sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
		median = rtts[len(rtts)/2]
	}
	fmt.Fprintf(w, "nodes seen:          %d\n", nodesSeen)
	fmt.Fprintf(w, "neighborhoods seen:  %d\n", neighborhoods)
	fmt.Fprintf(w, "sampled:             %d\n", len(results))
	fmt.Fprintf(w, "reachable:           %d (%.1f%%)\n", reachable, fraction*100)
	fmt.Fprintf(w, "median dial+hs rtt:  %d ms\n", median.Milliseconds())
}
