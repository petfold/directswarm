//! `directswarm crawl` — M3: build the topology cache with a bounded,
//! polite snowball crawl, then report neighborhood coverage against a
//! chunk inventory.

use clap::Args;
use ds_core::S