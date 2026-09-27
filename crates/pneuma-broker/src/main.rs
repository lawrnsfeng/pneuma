//! Demultiplexes one subject into one per tenant.
//!
//! Wiring only. `scripts/coverage.sh` excludes this file and refuses to let it
//! define anything, so every decision lives in `pneuma_broker::boot`.

use pneuma_broker::{boot, Config};
use pneuma_config::Env;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let token = CancellationToken::new();
    let stopping = token.clone();
    tokio::spawn(async move {
        match pneuma_serve::termination().await {
            Ok(signal) => eprintln!("pneuma-broker: stopping on {signal:?}"),
            Err(error) => eprintln!("pneuma-broker: cannot watch for signals: {error}"),
        }
        stopping.cancel();
    });
    let config = match Config::from_env(&Env::from_process()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("pneuma-broker: {error}");
            std::process::exit(2);
        }
    };
    let listening = |address| eprintln!("pneuma-broker: listening on {address}");
    if let Err(error) = boot::run(&config, token, listening).await {
        eprintln!("pneuma-broker: {error}");
        std::process::exit(1);
    }
}
