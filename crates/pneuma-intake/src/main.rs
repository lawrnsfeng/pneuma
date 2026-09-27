//! Admits runs from AMQP into the pipeline.
//!
//! Wiring only. `scripts/coverage.sh` excludes this file and refuses to let it
//! define anything, so every decision lives in `pneuma_intake::boot`.

use pneuma_config::Env;
use pneuma_intake::{boot, Config};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let token = CancellationToken::new();
    let stopping = token.clone();
    tokio::spawn(async move {
        match pneuma_serve::termination().await {
            Ok(signal) => eprintln!("pneuma-intake: stopping on {signal:?}"),
            Err(error) => eprintln!("pneuma-intake: cannot watch for signals: {error}"),
        }
        stopping.cancel();
    });
    let config = match Config::from_env(&Env::from_process()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("pneuma-intake: {error}");
            std::process::exit(2);
        }
    };
    let listening = |address| eprintln!("pneuma-intake: listening on {address}");
    if let Err(error) = boot::run(&config, token, listening).await {
        eprintln!("pneuma-intake: {error}");
        std::process::exit(1);
    }
}
