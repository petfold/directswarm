// Command svcrate is the Phase-0 settlement-active service-rate probe.
//
// Modes:
//
//	addr    — print the probe identity (eth address, overlay, chequebook state); no network
//	init    — one-time chain setup: deploy/verify chequebook, deposit xBZZ (cribs bee deploy)
//	chunks  — enumerate the payload's chunk addresses with bee's file pipeline; no network
//	measure — dial reach.csv targets and measure retrieval service rate, always settling
package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/accounting"
	"github.com/ethersphere/bee/v2/pkg/addressbook"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/keystore/file"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/node"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/pricer"
	"github.com/ethersphere/bee/v2/pkg/hive"
	"github.com/ethersphere/bee/v2/pkg/pricing"
	"github.com/ethersphere/bee/v2/pkg/retrieval/pb"
	"github.com/ethersphere/bee/v2/pkg/settlement/pseudosettle"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/chequebook"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/statestore/leveldb"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	networkID     = 1 // Swarm mainnet
	gnosisChainID = 100

	// same password as cmd/reach so the existing reach-data identity opens
	keyPassword     = "directswarm-reach"
	overlayNonceKey = "overlayV2_nonce"

	// bee statestore key for the deployed chequebook address
	// (pkg/settlement/swap/chequebook/init.go, unexported there)
	chequebookKey = "swap_chequebook"

	// accounting constants copied from pkg/node/node.go
	refreshRate         = int64(4_500_000)
	lightFactor         = int64(10)
	lightRefreshRate    = refreshRate / lightFactor
	basePrice           = uint64(10_000)
	paymentThresholdStr = "13500000"
	paymentTolerance    = int64(25)
	paymentEarly        = int64(50)

	// retrieval wire identifiers, mirroring the unexported constants in
	// pkg/retrieval/retrieval.go
	retrievalProtocolName    = "retrieval"
	retrievalProtocolVersion = "1.4.0"
	retrievalStreamName      = "retrieval"

	// known mainnet root reference of the 1 GiB phase-0 payload
	expectedRootHex = "842efaa92f86fe67dd7bd244a7c7935cade4da1eee41ea558f49c00da90a759a"

	plurPerBZZStr = "10000000000000000" // xBZZ has 16 decimals

	dialTimeout    = 20 * time.Second
	requestTimeout = 30 * time.Second
)

func main() {
	var (
		mode    = flag.String("mode", "", "addr | init | chunks | measure")
		dataDir = flag.String("data-dir", "/home/test/projects/directswarm/.phase0/reach-data", "directory for keys and statestore (shared with cmd/reach)")

		// init
		rpc     = flag.String("rpc", "https://rpc.gnosischain.com", "gnosis chain rpc endpoint")
		deposit = flag.String("deposit", "1.0", "xBZZ to deposit into the chequebook (init mode)")

		// chunks
		payload   = flag.String("payload", "/home/test/projects/directswarm/.phase0/payload.bin", "payload file to split")
		chunksCSV = flag.String("chunks", "/home/test/projects/directswarm/.phase0/chunks.csv", "chunk list CSV (written by chunks mode, read by measure)")

		// measure
		targets  = flag.String("targets", "/home/test/projects/directswarm/.phase0/reach.csv", "reach.csv from milestone 1")
		peersN   = flag.Int("peers", 10, "peers to measure")
		depths   = flag.String("depths", "1,8,32,100", "comma-separated concurrency depths")
		secs     = flag.Int("secs", 60, "seconds per (peer,depth) run")
		maxBytes = flag.Int64("max-bytes-per-peer", 100_000_000, "byte cap per (peer,depth) run")
		out      = flag.String("out", "svcrate.csv", "measurement CSV (appended)")
		maxMins  = flag.Int("max-mins", 90, "global cap in minutes (measure)")
		pause    = flag.Int("pause", 10, "seconds to pause between peers")
		minPO    = flag.Int("min-po", 9, "minimum proximity order of chunk to target overlay")
		seed     = flag.Int64("seed", 1, "deterministic shuffle seed")
		listen   = flag.String("listen", ":0", "libp2p listen address")
		conc     = flag.Bool("concurrent", false, "measure all picked peers in parallel (aggregate mode)")
	)
	flag.Parse()

	var err error
	switch *mode {
	case "addr":
		err = runAddr(*dataDir)
	case "init":
		err = runInit(*dataDir, *rpc, *deposit)
	case "chunks":
		err = runChunks(*payload, *chunksCSV)
	case "measure":
		err = runMeasure(measureOpts{
			dataDir:  *dataDir,
			rpc:      *rpc,
			targets:  *targets,
			chunks:   *chunksCSV,
			peersN:   *peersN,
			depths:   *depths,
			secs:     *secs,
			maxBytes: *maxBytes,
			out:      *out,
			maxMins:  *maxMins,
			pause:    *pause,
			minPO:      uint8(*minPO),
			seed:       *seed,
			listen:     *listen,
			concurrent: *conc,
		})
	default:
		err = fmt.Errorf("unknown --mode %q (want addr|init|chunks|measure)", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "svcrate:", err)
		os.Exit(1)
	}
}

// --- identity ---

func loadState(dataDir string, logger log.Logger) (storage.StateStorer, error) {
	return leveldb.NewStateStore(filepath.Join(dataDir, "statestore"), logger)
}

func loadIdentity(dataDir string, stateStore storage.StateStorer) (crypto.Signer, swarm.Address, []byte, common.Address, error) {
	keys := file.New(filepath.Join(dataDir, "keys"))
	swarmKey, _, err := keys.Key("swarm", keyPassword, crypto.EDGSecp256_K1)
	if err != nil {
		return nil, swarm.ZeroAddress, nil, common.Address{}, fmt.Errorf("swarm key: %w", err)
	}
	signer := crypto.NewDefaultSigner(swarmKey)

	nonce := make([]byte, 32)
	if err := stateStore.Get(overlayNonceKey, &nonce); err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, swarm.ZeroAddress, nil, common.Address{}, fmt.Errorf("read nonce: %w", err)
		}
		if _, err := rand.Read(nonce); err != nil {
			return nil, swarm.ZeroAddress, nil, common.Address{}, fmt.Errorf("generate nonce: %w", err)
		}
		if err := stateStore.Put(overlayNonceKey, nonce); err != nil {
			return nil, swarm.ZeroAddress, nil, common.Address{}, fmt.Errorf("persist nonce: %w", err)
		}
	}

	overlay, err := crypto.NewOverlayAddress(swarmKey.PublicKey, networkID, nonce)
	if err != nil {
		return nil, swarm.ZeroAddress, nil, common.Address{}, fmt.Errorf("overlay: %w", err)
	}
	ethAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, swarm.ZeroAddress, nil, common.Address{}, fmt.Errorf("eth address: %w", err)
	}
	return signer, overlay, nonce, ethAddress, nil
}

// --- mode addr ---

func runAddr(dataDir string) error {
	logger := log.NewLogger("svcrate")
	stateStore, err := loadState(dataDir, logger)
	if err != nil {
		return err
	}
	defer stateStore.Close()

	_, overlay, _, ethAddress, err := loadIdentity(dataDir, stateStore)
	if err != nil {
		return err
	}

	var cb common.Address
	cbState := "none stored — run --mode init after funding"
	if err := stateStore.Get(chequebookKey, &cb); err == nil {
		cbState = cb.Hex()
	} else if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("read chequebook key: %w", err)
	}

	fmt.Printf("ethereum address: %s\n", ethAddress.Hex())
	fmt.Printf("overlay (network %d): %s\n", networkID, overlay)
	fmt.Printf("chequebook: %s\n", cbState)
	fmt.Printf("fund the ethereum address with xBZZ (deposit) and xDAI (gas) before --mode init\n")
	return nil
}

// --- mode init ---

func plurFromBZZ(s string) (*big.Int, error) {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("cannot parse xBZZ amount %q", s)
	}
	unit, _ := new(big.Int).SetString(plurPerBZZStr, 10)
	r.Mul(r, new(big.Rat).SetInt(unit))
	if !r.IsInt() {
		return nil, fmt.Errorf("amount %q has more than 16 decimals", s)
	}
	return r.Num(), nil
}

func runInit(dataDir, rpc, depositBZZ string) error {
	logger := log.NewLogger("svcrate")
	stateStore, err := loadState(dataDir, logger)
	if err != nil {
		return err
	}
	defer stateStore.Close()

	signer, overlay, _, ethAddress, err := loadIdentity(dataDir, stateStore)
	if err != nil {
		return err
	}
	logger.Info("probe identity", "overlay", overlay, "eth_address", ethAddress)

	depositPlur, err := plurFromBZZ(depositBZZ)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// crib bee's own sequence: cmd/bee/cmd/deploy.go
	backend, _, chainID, monitor, txService, err := node.InitChain(
		ctx, logger, stateStore, gnosisChainID, signer, 5*time.Second, true,
		0, 500_000,
		node.BlockchainRPCConfig{
			Endpoint:    rpc,
			DialTimeout: 30 * time.Second,
			TLSTimeout:  10 * time.Second,
			IdleTimeout: 90 * time.Second,
			Keepalive:   30 * time.Second,
		},
		10,
	)
	if err != nil {
		return fmt.Errorf("init chain: %w", err)
	}
	defer backend.Close()
	defer monitor.Close()
	defer txService.Close()

	// "" selects config.Mainnet.CurrentFactoryAddress for chain id 100
	factory, err := node.InitChequebookFactory(logger, backend, chainID, txService, "")
	if err != nil {
		return fmt.Errorf("init chequebook factory: %w", err)
	}

	erc20Address, err := factory.ERC20Address(ctx)
	if err != nil {
		return fmt.Errorf("factory erc20 address: %w", err)
	}
	erc20Service := erc20.New(txService, erc20Address)

	var existing common.Address
	preExisted := stateStore.Get(chequebookKey, &existing) == nil

	// on a fresh deploy bee itself deposits swapInitialDeposit and waits
	chequebookService, err := node.InitChequebookService(
		ctx, logger, stateStore, signer, chainID, backend, ethAddress,
		txService, factory, depositPlur.String(), erc20Service,
	)
	if err != nil {
		return fmt.Errorf("init chequebook service: %w", err)
	}

	// deviation from bee (which only deposits at deploy time): top up an
	// already-deployed chequebook so init stays idempotent but effective
	if preExisted && depositPlur.Sign() > 0 {
		bal, err := chequebookService.Balance(ctx)
		if err != nil {
			return fmt.Errorf("chequebook balance: %w", err)
		}
		if bal.Cmp(depositPlur) < 0 {
			diff := new(big.Int).Sub(depositPlur, bal)
			logger.Info("topping up existing chequebook", "amount_plur", diff)
			txHash, err := chequebookService.Deposit(ctx, diff)
			if err != nil {
				return fmt.Errorf("deposit: %w", err)
			}
			if err := chequebookService.WaitForDeposit(ctx, txHash); err != nil {
				return fmt.Errorf("wait for deposit: %w", err)
			}
		} else {
			logger.Info("chequebook already funded at or above target", "balance_plur", bal)
		}
	}

	bal, err := chequebookService.Balance(ctx)
	if err != nil {
		return fmt.Errorf("chequebook balance: %w", err)
	}
	avail, err := chequebookService.AvailableBalance(ctx)
	if err != nil {
		return fmt.Errorf("chequebook available balance: %w", err)
	}
	walletBZZ, err := erc20Service.BalanceOf(ctx, ethAddress)
	if err != nil {
		return fmt.Errorf("wallet bzz balance: %w", err)
	}

	fmt.Printf("chequebook address:  %s\n", chequebookService.Address().Hex())
	fmt.Printf("chequebook balance:  %s PLUR (1 xBZZ = 1e16 PLUR)\n", bal)
	fmt.Printf("available balance:   %s PLUR\n", avail)
	fmt.Printf("wallet xBZZ balance: %s PLUR\n", walletBZZ)
	return nil
}

// --- mode chunks ---

type chunkRow struct {
	addr    swarm.Address
	dataLen int
}

func runChunks(payloadPath, chunksPath string) error {
	f, err := os.Open(payloadPath)
	if err != nil {
		return fmt.Errorf("open payload: %w", err)
	}
	defer f.Close()

	outF, err := os.Create(chunksPath)
	if err != nil {
		return fmt.Errorf("create chunks csv: %w", err)
	}
	defer outF.Close()
	w := csv.NewWriter(outF)
	if err := w.Write([]string{"index", "address_hex", "data_len"}); err != nil {
		return err
	}

	// chunk-capturing putter: the pipeline pushes every chunk it produces —
	// data chunks in payload order plus intermediate (parent) chunks as
	// each BMT level completes
	var (
		mu    sync.Mutex
		index int
	)
	putter := storage.PutterFunc(func(_ context.Context, ch swarm.Chunk) error {
		mu.Lock()
		defer mu.Unlock()
		err := w.Write([]string{
			strconv.Itoa(index),
			ch.Address().String(),
			strconv.Itoa(len(ch.Data())),
		})
		index++
		if index%65536 == 0 {
			w.Flush()
		}
		return err
	})

	pipe := builder.NewPipelineBuilder(context.Background(), putter, false, redundancy.NONE)
	root, err := builder.FeedPipeline(context.Background(), pipe, f)
	if err != nil {
		return fmt.Errorf("split payload: %w", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	fmt.Printf("chunks written: %d -> %s\n", index, chunksPath)
	fmt.Printf("computed root:  %s\n", root)
	fmt.Printf("expected root:  %s\n", expectedRootHex)
	if root.String() == expectedRootHex {
		fmt.Println("ROOT CHECK: PASS")
		return nil
	}
	fmt.Println("ROOT CHECK: FAIL — do not measure against these addresses")
	return errors.New("root mismatch")
}

// --- mode measure ---

type measureOpts struct {
	dataDir    string
	rpc        string
	targets    string
	chunks     string
	peersN     int
	depths     string
	secs       int
	maxBytes   int64
	out        string
	maxMins    int
	pause      int
	minPO      uint8
	seed       int64
	listen     string
	concurrent bool
}

type target struct {
	overlay   swarm.Address
	underlays []ma.Multiaddr
	prefix    uint16
}

// depth9Prefix returns the first 9 bits of the overlay (same as cmd/reach)
func depth9Prefix(a swarm.Address) uint16 {
	b := a.Bytes()
	if len(b) < 2 {
		return 0
	}
	return uint16(b[0])<<1 | uint16(b[1])>>7
}

func parseTargets(path string) ([]target, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, errors.New("targets csv has no data rows")
	}
	col := map[string]int{}
	for i, name := range rows[0] {
		col[name] = i
	}
	for _, need := range []string{"overlay_hex", "underlay", "dial_ok"} {
		if _, ok := col[need]; !ok {
			return nil, fmt.Errorf("targets csv missing column %q", need)
		}
	}
	seen := map[string]bool{}
	var out []target
	for _, row := range rows[1:] {
		if row[col["dial_ok"]] != "1" {
			continue
		}
		overlay, err := swarm.ParseHexAddress(row[col["overlay_hex"]])
		if err != nil || seen[overlay.String()] {
			continue
		}
		var underlays []ma.Multiaddr
		for _, u := range strings.Split(row[col["underlay"]], "|") {
			if u == "" {
				continue
			}
			addr, err := ma.NewMultiaddr(u)
			if err == nil {
				underlays = append(underlays, addr)
			}
		}
		if len(underlays) == 0 {
			continue
		}
		seen[overlay.String()] = true
		out = append(out, target{overlay: overlay, underlays: underlays, prefix: depth9Prefix(overlay)})
	}
	return out, nil
}

// pickTargets picks up to n targets round-robin across distinct depth-9
// neighborhoods (same policy as cmd/reach sampling)
func pickTargets(all []target, n int) []target {
	groups := map[uint16][]target{}
	for _, t := range all {
		groups[t.prefix] = append(groups[t.prefix], t)
	}
	prefixes := make([]uint16, 0, len(groups))
	for p := range groups {
		prefixes = append(prefixes, p)
	}
	sort.Slice(prefixes, func(i, j int) bool { return prefixes[i] < prefixes[j] })

	var picked []target
	for len(picked) < n {
		progress := false
		for _, p := range prefixes {
			if len(groups[p]) == 0 {
				continue
			}
			picked = append(picked, groups[p][0])
			groups[p] = groups[p][1:]
			progress = true
			if len(picked) == n {
				break
			}
		}
		if !progress {
			break
		}
	}
	return picked
}

func parseChunks(path string) ([]swarm.Address, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	addrCol := -1
	for i, name := range header {
		if name == "address_hex" {
			addrCol = i
		}
	}
	if addrCol < 0 {
		return nil, errors.New("chunks csv missing address_hex column")
	}
	var out []swarm.Address
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		a, err := swarm.ParseHexAddress(row[addrCol])
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func parseDepths(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		d, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || d < 1 {
			return nil, fmt.Errorf("bad depth %q", part)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, errors.New("no depths")
	}
	return out, nil
}

// trackingNotifier is the mandatory PickyNotifier (the libp2p service
// dereferences it unconditionally); it additionally counts disconnects.
type trackingNotifier struct {
	mu           sync.Mutex
	disconnected map[string]int
	log          log.Logger
}

func newTrackingNotifier(logger log.Logger) *trackingNotifier {
	return &trackingNotifier{disconnected: map[string]int{}, log: logger}
}

func (t *trackingNotifier) Pick(p2p.Peer) bool                              { return true }
func (t *trackingNotifier) Connected(_ context.Context, p p2p.Peer, _ bool) error {
	t.log.Info("notifier: peer connected", "peer", p.Address, "full_node", p.FullNode)
	return nil
}
func (t *trackingNotifier) Disconnected(p p2p.Peer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.disconnected[p.Address.ByteString()]++
	t.log.Info("notifier: peer disconnected", "peer", p.Address, "count", t.disconnected[p.Address.ByteString()])
}
func (t *trackingNotifier) Announce(context.Context, swarm.Address, bool) error { return nil }
func (t *trackingNotifier) AnnounceTo(context.Context, swarm.Address, swarm.Address, bool) error {
	return nil
}
func (t *trackingNotifier) UpdateReachability(p2p.ReachabilityStatus)       {}
func (t *trackingNotifier) Reachable(swarm.Address, p2p.ReachabilityStatus) {}

func (t *trackingNotifier) disconnects(overlay swarm.Address) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.disconnected[overlay.ByteString()]
}

// countingChequebook wraps bee's chequebook service to count cheques the
// swap protocol issues through it (Issue amounts are BZZ base units, PLUR).
type countingChequebook struct {
	chequebook.Service
	mu    sync.Mutex
	count int64
	total *big.Int
}

func (c *countingChequebook) Issue(ctx context.Context, beneficiary common.Address, amount *big.Int, send chequebook.SendChequeFunc) (*big.Int, error) {
	bal, err := c.Service.Issue(ctx, beneficiary, amount, send)
	if err == nil {
		c.mu.Lock()
		c.count++
		c.total.Add(c.total, amount)
		c.mu.Unlock()
	}
	return bal, err
}

func (c *countingChequebook) snapshot() (int64, *big.Int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count, new(big.Int).Set(c.total)
}

type runResult struct {
	peer           swarm.Address
	prefix         uint16
	depth          int
	wallSecs       float64
	ok             int64
	errs           int64
	refusals       int64
	overdraftWaits int64
	bytes        int64
	p50          time.Duration
	p95          time.Duration
	acctDebited  *big.Int
	chequeCount  int64
	chequePlur   *big.Int
	pseudoAcct   *big.Int
	disconnects  int
	chunkSetSize int
	chunkSetUsed int
	stopReason   string
	terminal     bool // abort remaining depths for this peer
}

func (r runResult) chunksPerSec() float64 {
	if r.wallSecs <= 0 {
		return 0
	}
	return float64(r.ok) / r.wallSecs
}

func (r runResult) mbPerSec() float64 {
	if r.wallSecs <= 0 {
		return 0
	}
	return float64(r.bytes) / 1e6 / r.wallSecs
}

func runMeasure(o measureOpts) error {
	logger := log.NewLogger("svcrate")

	depths, err := parseDepths(o.depths)
	if err != nil {
		return err
	}
	allTargets, err := parseTargets(o.targets)
	if err != nil {
		return fmt.Errorf("targets: %w", err)
	}
	if len(allTargets) == 0 {
		return errors.New("no dial_ok targets in reach csv")
	}
	chunks, err := parseChunks(o.chunks)
	if err != nil {
		return fmt.Errorf("chunks: %w", err)
	}
	logger.Info("inputs loaded", "targets", len(allTargets), "chunks", len(chunks))

	stateStore, err := loadState(o.dataDir, logger)
	if err != nil {
		return err
	}
	defer stateStore.Close()

	signer, overlay, nonce, ethAddress, err := loadIdentity(o.dataDir, stateStore)
	if err != nil {
		return err
	}
	logger.Info("probe identity", "overlay", overlay, "eth_address", ethAddress)

	// refuse to run unsettled: a chequebook must already be deployed
	var storedChequebook common.Address
	if err := stateStore.Get(chequebookKey, &storedChequebook); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return errors.New("no chequebook in statestore — run --mode init first (funded wallet required)")
		}
		return err
	}

	keys := file.New(filepath.Join(o.dataDir, "keys"))
	libp2pKey, _, err := keys.Key("libp2p_v2", keyPassword, crypto.EDGSecp256_R1)
	if err != nil {
		return fmt.Errorf("libp2p key: %w", err)
	}

	svcCtx, svcCancel := context.WithCancel(context.Background())
	defer svcCancel()
	runCtx, runCancel := context.WithTimeout(svcCtx, time.Duration(o.maxMins)*time.Minute)
	defer runCancel()

	// chain backend: cheque issuance is off-chain but needs the price oracle
	// exchange rate and chequebook balance reads
	backend, _, chainID, monitor, txService, err := node.InitChain(
		svcCtx, logger, stateStore, gnosisChainID, signer, 5*time.Second, true,
		0, 500_000,
		node.BlockchainRPCConfig{
			Endpoint:    o.rpc,
			DialTimeout: 30 * time.Second,
			TLSTimeout:  10 * time.Second,
			IdleTimeout: 90 * time.Second,
			Keepalive:   30 * time.Second,
		},
		10,
	)
	if err != nil {
		return fmt.Errorf("init chain: %w", err)
	}
	defer backend.Close()
	defer monitor.Close()
	defer txService.Close()

	factory, err := node.InitChequebookFactory(logger, backend, chainID, txService, "")
	if err != nil {
		return fmt.Errorf("init chequebook factory: %w", err)
	}
	erc20Address, err := factory.ERC20Address(svcCtx)
	if err != nil {
		return fmt.Errorf("factory erc20 address: %w", err)
	}
	erc20Service := erc20.New(txService, erc20Address)

	// deposit "0": the chequebook exists, this only constructs and verifies
	chequebookService, err := node.InitChequebookService(
		svcCtx, logger, stateStore, signer, chainID, backend, ethAddress,
		txService, factory, "0", erc20Service,
	)
	if err != nil {
		return fmt.Errorf("init chequebook service: %w", err)
	}
	counting := &countingChequebook{Service: chequebookService, total: big.NewInt(0)}

	chequeStore := chequebook.NewChequeStore(
		stateStore, factory, chainID, ethAddress, txService, chequebook.RecoverCheque,
	)
	cashout := chequebook.NewCashoutService(stateStore, backend, txService, chequeStore)

	// p2p stack, wired exactly as pkg/node/node.go does for a light node
	ab := addressbook.New(stateStore)
	lightNodes := lightnode.NewContainer(overlay)
	tracer, tracerCloser, err := tracing.NewTracer(&tracing.Options{Enabled: false})
	if err != nil {
		return fmt.Errorf("tracer: %w", err)
	}
	defer tracerCloser.Close()

	p2ps, err := libp2p.New(svcCtx, signer, networkID, overlay, o.listen, ab, stateStore, lightNodes, logger, tracer, libp2p.Options{
		PrivateKey:        libp2pKey,
		FullNode:          false,
		Nonce:             nonce,
		AllowPrivateCIDRs: true,
	})
	if err != nil {
		return fmt.Errorf("p2p service: %w", err)
	}
	defer p2ps.Close()

	notifier := newTrackingNotifier(logger)
	p2ps.SetPickyNotifier(notifier)

	// advertise our chequebook in the handshake (node.go does this right
	// after constructing the service); SVCRATE_NO_CB_ADVERT=1 disables it
	// for bisecting remote-ejection causes
	if os.Getenv("SVCRATE_NO_CB_ADVERT") == "" {
		p2ps.SetChequebookAddress(chequebookService.Address())
	} else {
		logger.Info("chequebook advertisement DISABLED (bisect mode)")
	}

	// mount hive: remotes announce peers to a fresh light peer immediately
	// after handshake, and a client without the hive protocol gets its
	// stream reset and the connection torn down (measured — see STATUS);
	// a stock-shaped light client must accept hive gossip
	hiveSvc := hive.New(p2ps, ab, networkID, overlay, logger, hive.Options{})
	if err := p2ps.AddProtocol(hiveSvc.Protocol()); err != nil {
		return fmt.Errorf("mount hive: %w", err)
	}
	defer hiveSvc.Close()

	paymentThreshold, _ := new(big.Int).SetString(paymentThresholdStr, 10)
	lightPaymentThreshold := new(big.Int).Div(paymentThreshold, big.NewInt(lightFactor))
	minThreshold := big.NewInt(2 * lightRefreshRate) // light node
	pricingSvc := pricing.New(p2ps, logger, paymentThreshold, lightPaymentThreshold, minThreshold)
	if err := p2ps.AddProtocol(pricingSvc.Protocol()); err != nil {
		return fmt.Errorf("mount pricing: %w", err)
	}

	enforcedRefreshRate := big.NewInt(lightRefreshRate) // light node

	acc, err := accounting.NewAccounting(
		paymentThreshold,
		paymentTolerance,
		paymentEarly,
		logger,
		stateStore,
		pricingSvc,
		new(big.Int).Set(enforcedRefreshRate),
		lightFactor,
		p2ps,
	)
	if err != nil {
		return fmt.Errorf("accounting: %w", err)
	}
	defer acc.Close()

	pseudosettleService := pseudosettle.New(p2ps, logger, stateStore, acc, new(big.Int).Set(enforcedRefreshRate), big.NewInt(lightRefreshRate), p2ps)
	if err := p2ps.AddProtocol(pseudosettleService.Protocol()); err != nil {
		return fmt.Errorf("mount pseudosettle: %w", err)
	}
	acc.SetRefreshFunc(pseudosettleService.Pay)

	// swap + swapprotocol + price oracle, via bee's own InitSwap ("" selects
	// the mainnet oracle from pkg/config); mounts the swap protocol itself
	swapService, priceOracle, err := node.InitSwap(
		p2ps, logger, stateStore, networkID, ethAddress, counting,
		chequeStore, cashout, acc, "", chainID, txService,
	)
	if err != nil {
		return fmt.Errorf("init swap: %w", err)
	}
	defer priceOracle.Close()
	acc.SetPayFunc(swapService.Pay)

	pricingSvc.SetPaymentThresholdObserver(acc)

	if err := p2ps.Ready(); err != nil {
		return fmt.Errorf("p2p ready: %w", err)
	}

	priceSvc := pricer.NewFixedPricer(overlay, basePrice)

	picked := pickTargets(allTargets, o.peersN)
	logger.Info("targets picked", "count", len(picked), "of", len(allTargets))

	csvW, closeCSV, err := openResultCSV(o.out)
	if err != nil {
		return err
	}
	defer closeCSV()

	var results []runResult
	if o.concurrent {
		// aggregate mode: all picked peers measured simultaneously —
		// the whole point is whether per-connection settlement ceilings
		// stack across independent connections
		var (
			wg    sync.WaitGroup
			resMu sync.Mutex
		)
		aggStart := time.Now()
		for _, tgt := range picked {
			wg.Add(1)
			go func(tgt target) {
				defer wg.Done()
				peerRuns := measurePeer(runCtx, logger, p2ps, notifier, acc, priceSvc, counting, swapService, pseudosettleService, tgt, chunks, depths, o)
				resMu.Lock()
				results = append(results, peerRuns...)
				resMu.Unlock()
			}(tgt)
		}
		wg.Wait()
		aggWall := time.Since(aggStart).Seconds()
		var totBytes, totOK int64
		sumRates := 0.0
		for _, r := range results {
			if err := writeResultRow(csvW, r); err != nil {
				return fmt.Errorf("write result: %w", err)
			}
			totBytes += r.bytes
			totOK += r.ok
			sumRates += r.mbPerSec()
		}
		fmt.Printf("\nAGGREGATE concurrent=%d peers: %.3f MB/s bytes-over-wall (%.1f MB in %.1fs incl. graces), sum of per-peer steady rates %.3f MB/s, %d chunks ok\n",
			len(picked), float64(totBytes)/1e6/aggWall, float64(totBytes)/1e6, aggWall, sumRates, totOK)
		summarizeMeasure(os.Stdout, results, depths)
		return nil
	}

	for i, tgt := range picked {
		if runCtx.Err() != nil {
			logger.Warning("global timeout, stopping peer loop", "done", i, "picked", len(picked))
			break
		}
		if i > 0 {
			select {
			case <-time.After(time.Duration(o.pause) * time.Second):
			case <-runCtx.Done():
			}
		}

		peerRuns := measurePeer(runCtx, logger, p2ps, notifier, acc, priceSvc, counting, swapService, pseudosettleService, tgt, chunks, depths, o)
		for _, r := range peerRuns {
			results = append(results, r)
			if err := writeResultRow(csvW, r); err != nil {
				return fmt.Errorf("write result: %w", err)
			}
		}
	}

	summarizeMeasure(os.Stdout, results, depths)
	return nil
}

type settlementSnapshot struct {
	swapPlur    *big.Int
	pseudoAcct  *big.Int
	chequeCount int64
	chequePlur  *big.Int
}

func snapshotSettlement(peer swarm.Address, swapSvc interface {
	TotalSent(swarm.Address) (*big.Int, error)
}, pseudo interface {
	TotalSent(swarm.Address) (*big.Int, error)
}, counting *countingChequebook) settlementSnapshot {
	s := settlementSnapshot{swapPlur: big.NewInt(0), pseudoAcct: big.NewInt(0)}
	if v, err := swapSvc.TotalSent(peer); err == nil {
		s.swapPlur = v
	}
	if v, err := pseudo.TotalSent(peer); err == nil {
		s.pseudoAcct = v
	}
	s.chequeCount, s.chequePlur = counting.snapshot()
	return s
}

func measurePeer(
	ctx context.Context,
	logger log.Logger,
	p2ps *libp2p.Service,
	notifier *trackingNotifier,
	acc *accounting.Accounting,
	priceSvc *pricer.FixedPricer,
	counting *countingChequebook,
	swapSvc interface {
		TotalSent(swarm.Address) (*big.Int, error)
	},
	pseudo interface {
		TotalSent(swarm.Address) (*big.Int, error)
	},
	tgt target,
	allChunks []swarm.Address,
	depths []int,
	o measureOpts,
) []runResult {
	base := runResult{
		peer:        tgt.overlay,
		prefix:      tgt.prefix,
		acctDebited: big.NewInt(0),
		chequePlur:  big.NewInt(0),
		pseudoAcct:  big.NewInt(0),
	}

	// per-peer chunk set: chunks the target is expected to store
	var set []swarm.Address
	for _, c := range allChunks {
		if swarm.Proximity(c.Bytes(), tgt.overlay.Bytes()) >= o.minPO {
			set = append(set, c)
		}
	}
	if len(set) == 0 {
		r := base
		r.stopReason = "no_chunks_at_min_po"
		r.terminal = true
		return []runResult{r}
	}
	// deterministic per-peer shuffle
	h := int64(0)
	for _, b := range tgt.overlay.Bytes() {
		h = h*131 + int64(b)
	}
	rng := mrand.New(mrand.NewSource(o.seed ^ h))
	rng.Shuffle(len(set), func(i, j int) { set[i], set[j] = set[j], set[i] })

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	_, err := p2ps.Connect(dialCtx, tgt.underlays)
	cancel()
	if err != nil && !errors.Is(err, p2p.ErrAlreadyConnected) {
		r := base
		r.stopReason = "dial_failed: " + err.Error()
		r.terminal = true
		return []runResult{r}
	}
	defer func() { _ = p2ps.Disconnect(tgt.overlay, "svcrate run complete") }()

	// let the handshake side-effects land: pricing threshold announcement,
	// swap beneficiary handshake (swapprotocol ConnectOut)
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
	}
	logger.Info("post-grace state", "peer", tgt.overlay,
		"disconnects_seen", notifier.disconnects(tgt.overlay))

	var (
		results []runResult
		cursor  int // shared across depths: never re-request a chunk from this peer
	)
	for _, depth := range depths {
		if ctx.Err() != nil {
			break
		}
		r := runOne(ctx, p2ps, notifier, acc, priceSvc, counting, swapSvc, pseudo, tgt, set, &cursor, depth, len(depths), o)
		results = append(results, r)
		logger.Info("run complete",
			"peer", tgt.overlay, "depth", depth,
			"ok", r.ok, "err", r.errs, "refusals", r.refusals,
			"mb_per_s", fmt.Sprintf("%.3f", r.mbPerSec()),
			"stop", r.stopReason)
		if r.terminal {
			break
		}
	}
	return results
}

func runOne(
	ctx context.Context,
	p2ps *libp2p.Service,
	notifier *trackingNotifier,
	acc *accounting.Accounting,
	priceSvc *pricer.FixedPricer,
	counting *countingChequebook,
	swapSvc interface {
		TotalSent(swarm.Address) (*big.Int, error)
	},
	pseudo interface {
		TotalSent(swarm.Address) (*big.Int, error)
	},
	tgt target,
	set []swarm.Address,
	cursor *int,
	depth int,
	depthsTotal int,
	o measureOpts,
) runResult {
	res := runResult{
		peer:         tgt.overlay,
		prefix:       tgt.prefix,
		depth:        depth,
		acctDebited:  big.NewInt(0),
		chequePlur:   big.NewInt(0),
		pseudoAcct:   big.NewInt(0),
		chunkSetSize: len(set),
	}

	before := snapshotSettlement(tgt.overlay, swapSvc, pseudo, counting)
	disconnectsBefore := notifier.disconnects(tgt.overlay)

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(o.secs)*time.Second)
	defer cancel()

	var (
		mu             sync.Mutex // guards cursor, latencies, counters, stopReason
		latencies      []time.Duration
		bytesTotal     int64
		okCount        int64
		errCount       int64
		refusals       int64
		overdraftWaits int64
		acctSum        = big.NewInt(0)
		stopReason     string
		consecErr      int
	)
	setStop := func(reason string, terminal bool) {
		mu.Lock()
		if stopReason == "" {
			stopReason = reason
			res.terminal = res.terminal || terminal
		}
		mu.Unlock()
		cancel()
	}
	// budget: an even share of the peer's chunk set per depth run, so a
	// low-depth run can't starve the higher depths (the set is ~516 chunks
	// at min-po 9 — the hard per-peer budget our payload provides)
	startCursor := *cursor
	budget := len(set) / depthsTotal
	if budget < 1 {
		budget = 1
	}
	next := func() (swarm.Address, bool) {
		mu.Lock()
		defer mu.Unlock()
		if *cursor >= len(set) || *cursor-startCursor >= budget {
			return swarm.ZeroAddress, false
		}
		a := set[*cursor]
		*cursor++
		return a, true
	}

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < depth; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if runCtx.Err() != nil {
					return
				}
				mu.Lock()
				capped := bytesTotal >= o.maxBytes
				mu.Unlock()
				if capped {
					setStop("byte_cap", false)
					return
				}
				chunkAddr, ok := next()
				if !ok {
					setStop("chunks_exhausted", false)
					return
				}

				price := priceSvc.PeerPrice(tgt.overlay, chunkAddr)
				var credit accounting.Action
				for {
					var err error
					credit, err = acc.PrepareCredit(runCtx, tgt.overlay, price, true)
					if err == nil {
						break
					}
					if runCtx.Err() != nil {
						return // timer expiry, not a rejection
					}
					if errors.Is(err, accounting.ErrOverdraft) {
						// our own accounting refusing to overdraw while async
						// settlement restores headroom: backpressure, not a
						// refusal — wait and retry; the achieved rate IS the
						// settlement-limited service rate we're measuring
						mu.Lock()
						overdraftWaits++
						mu.Unlock()
						select {
						case <-time.After(100 * time.Millisecond):
						case <-runCtx.Done():
							return
						}
						continue
					}
					setStop("accounting_rejection: "+err.Error(), true)
					return
				}

				t0 := time.Now()
				n, refused, err := fetchChunk(runCtx, p2ps, tgt.overlay, chunkAddr, credit)
				lat := time.Since(t0)
				if err != nil {
					credit.Cleanup()
					mu.Lock()
					errCount++
					consecErr++
					streak := consecErr
					mu.Unlock()
					if notifier.disconnects(tgt.overlay) > disconnectsBefore {
						setStop("disconnect", true)
						return
					}
					if streak >= 20 {
						setStop("error_streak", false)
						return
					}
					continue
				}
				if refused {
					credit.Cleanup()
					mu.Lock()
					refusals++
					consecErr = 0
					mu.Unlock()
					continue
				}
				credit.Cleanup() // no-op after Apply inside fetchChunk
				mu.Lock()
				okCount++
				consecErr = 0
				bytesTotal += int64(n)
				latencies = append(latencies, lat)
				acctSum.Add(acctSum, new(big.Int).SetUint64(price))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)

	mu.Lock()
	defer mu.Unlock()
	if stopReason == "" {
		if ctx.Err() != nil {
			stopReason = "global_timeout"
			res.terminal = true
		} else {
			stopReason = "secs_elapsed"
		}
	}

	after := snapshotSettlement(tgt.overlay, swapSvc, pseudo, counting)

	res.wallSecs = wall.Seconds()
	res.ok = okCount
	res.errs = errCount
	res.refusals = refusals
	res.overdraftWaits = overdraftWaits
	res.bytes = bytesTotal
	res.acctDebited = acctSum
	res.chequeCount = after.chequeCount - before.chequeCount
	res.chequePlur = new(big.Int).Sub(after.chequePlur, before.chequePlur)
	res.pseudoAcct = new(big.Int).Sub(after.pseudoAcct, before.pseudoAcct)
	res.disconnects = notifier.disconnects(tgt.overlay) - disconnectsBefore
	res.chunkSetUsed = *cursor
	res.stopReason = stopReason
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	res.p50 = percentile(latencies, 50)
	res.p95 = percentile(latencies, 95)
	return res
}

// fetchChunk mirrors pkg/retrieval's client sequence: open stream, send
// pb.Request, read pb.Delivery, validate, then Apply the prepared credit.
// The latency recorded by callers spans this whole function.
func fetchChunk(ctx context.Context, p2ps *libp2p.Service, peer, addr swarm.Address, credit accounting.Action) (n int, refused bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	stream, err := p2ps.NewStream(reqCtx, peer, nil, retrievalProtocolName, retrievalProtocolVersion, retrievalStreamName)
	if err != nil {
		return 0, false, fmt.Errorf("new stream: %w", err)
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)
	if err = w.WriteMsgWithContext(reqCtx, &pb.Request{Addr: addr.Bytes()}); err != nil {
		return 0, false, fmt.Errorf("write request: %w", err)
	}

	var d pb.Delivery
	if err = r.ReadMsgWithContext(reqCtx, &d); err != nil {
		return 0, false, fmt.Errorf("read delivery: %w", err)
	}
	if d.Err != "" {
		return 0, true, nil
	}

	chunk := swarm.NewChunk(addr, d.Data)
	if !cac.Valid(chunk) && !soc.Valid(chunk) {
		err = swarm.ErrInvalidChunk
		return 0, false, err
	}

	if err = credit.Apply(); err != nil {
		return 0, false, fmt.Errorf("credit apply: %w", err)
	}
	return len(d.Data), false, nil
}

// --- output ---

var resultHeader = []string{
	"peer_overlay", "neighborhood_prefix_hex", "depth", "wall_secs",
	"chunks_ok", "chunks_err", "refusals", "bytes",
	"chunks_per_s", "mb_per_s", "lat_p50_ms", "lat_p95_ms",
	"acct_units_debited", "swap_cheques_count", "swap_cheques_plur",
	"pseudosettle_refreshed_acct_units", "overdraft_waits",
	"disconnects", "chunk_set_size", "chunk_set_used", "stop_reason",
}

func openResultCSV(path string) (*csv.Writer, func(), error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	w := csv.NewWriter(f)
	if st.Size() == 0 {
		if err := w.Write(resultHeader); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	return w, func() { w.Flush(); f.Close() }, nil
}

func writeResultRow(w *csv.Writer, r runResult) error {
	err := w.Write([]string{
		r.peer.String(),
		fmt.Sprintf("%03x", r.prefix),
		strconv.Itoa(r.depth),
		fmt.Sprintf("%.2f", r.wallSecs),
		strconv.FormatInt(r.ok, 10),
		strconv.FormatInt(r.errs, 10),
		strconv.FormatInt(r.refusals, 10),
		strconv.FormatInt(r.bytes, 10),
		fmt.Sprintf("%.3f", r.chunksPerSec()),
		fmt.Sprintf("%.4f", r.mbPerSec()),
		strconv.FormatInt(r.p50.Milliseconds(), 10),
		strconv.FormatInt(r.p95.Milliseconds(), 10),
		r.acctDebited.String(),
		strconv.FormatInt(r.chequeCount, 10),
		r.chequePlur.String(),
		r.pseudoAcct.String(),
		strconv.FormatInt(r.overdraftWaits, 10),
		strconv.Itoa(r.disconnects),
		strconv.Itoa(r.chunkSetSize),
		strconv.Itoa(r.chunkSetUsed),
		r.stopReason,
	})
	w.Flush()
	if err != nil {
		return err
	}
	return w.Error()
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func summarizeMeasure(w *os.File, results []runResult, depths []int) {
	fmt.Fprintf(w, "\n%-16s %5s %9s %10s %8s %8s %8s %10s %12s %s\n",
		"peer", "depth", "MB/s", "chunks/s", "p50ms", "cheques", "refusal",
		"swap_plur", "pseudo_acct", "stop")
	for _, r := range results {
		fmt.Fprintf(w, "%-16s %5d %9.4f %10.2f %8d %8d %8d %10s %12s %s\n",
			shortOverlay(r.peer), r.depth, r.mbPerSec(), r.chunksPerSec(),
			r.p50.Milliseconds(), r.chequeCount, r.refusals,
			r.chequePlur, r.pseudoAcct, r.stopReason)
	}
	fmt.Fprintln(w, "\nmedians across peers per depth:")
	for _, d := range depths {
		var rates []float64
		for _, r := range results {
			if r.depth == d && r.stopReason != "dial_failed" && r.ok > 0 {
				rates = append(rates, r.mbPerSec())
			}
		}
		if len(rates) == 0 {
			fmt.Fprintf(w, "  depth %3d: no successful runs\n", d)
			continue
		}
		sort.Float64s(rates)
		fmt.Fprintf(w, "  depth %3d: median %.4f MB/s over %d peers\n", d, rates[len(rates)/2], len(rates))
	}
}

func shortOverlay(a swarm.Address) string {
	s := a.String()
	if len(s) > 16 {
		return s[:16]
	}
	return s
}
