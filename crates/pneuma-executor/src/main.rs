//! Calls components for steps that arrive on NATS.
//!
//! Wiring only. `scripts/coverage.sh` excludes this file and refuses to let it
//! define anything, so every decision lives in `pneuma_executor::boot`.

use pneuma_config::Env;
use pneuma_executor::{boot, Config};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let token = CancellationToken::new();
    let stopping = token.clone();
    tokio::spawn(async move {
        match pneuma_serve::termination().await {
            Ok(signal) => eprintln!("pneuma-executor: stopping on {signal:?}"),
            Err(error) => eprintln!("pneuma-executor: cannot watch for signals: {error}"),
        }
        stopping.cancel();
    });
    let config = match Config::from_env(&Env::from_process()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("pneuma-executor: {error}");
            std::process::exit(2);
        }
    };
    let listening = |address| eprintln!("pneuma-executor: listening on {address}");
    if let Err(error) = boot::run(&config, token, listening).await {
        eprintln!("pneuma-executor: {error}");
        std::process::exit(1);
    }
}
