//! Archives finished runs, expires history, and terminates work that stopped.
//!
//! Wiring only. `scripts/coverage.sh` excludes this file and refuses to let it
//! define anything, so every decision lives in `pneuma_janitor::boot`.

use pneuma_config::Env;
use pneuma_janitor::boot::{self, Config, Mode};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let mode = match Mode::from_args(std::env::args().skip(1)) {
        Ok(mode) => mode,
        Err(reason) => {
            eprintln!("pneuma-janitor: {reason}");
            std::process::exit(2);
        }
    };
    let token = CancellationToken::new();
    let stopping = token.clone();
    tokio::spawn(async move {
        match pneuma_serve::termination().await {
            Ok(signal) => eprintln!("pneuma-janitor: stopping on {signal:?}"),
            Err(error) => eprintln!("pneuma-janitor: cannot watch for signals: {error}"),
        }
        stopping.cancel();
    });
    let config = match Config::from_env(&Env::from_process()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("pneuma-janitor: {error}");
            std::process::exit(2);
        }
    };
    let listening = |address| eprintln!("pneuma-janitor: listening on {address}");
    if let Err(error) = boot::run(&config, mode, token, listening).await {
        eprintln!("pneuma-janitor: {error}");
        std::process::exit(1);
    }
}
