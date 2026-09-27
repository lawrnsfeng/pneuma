//! Serves the pneuma runner as a Restate service.
//!
//! Wiring only. `scripts/coverage.sh` excludes this file, because `main` is
//! never executed by the test suite, and refuses to let it define anything —
//! so every decision it makes lives in `pneuma_restate::serve`, where it is
//! tested, and what remains here is reading the environment and reporting a
//! refusal.

use pneuma_restate::serve::{configuration, connect, serve};

#[tokio::main]
async fn main() {
    let prepared = match configuration(|key| std::env::var(key).ok()) {
        Ok(prepared) => prepared,
        Err(error) => {
            eprintln!("pneuma-restate: {error}");
            std::process::exit(1);
        }
    };
    match connect(prepared).await {
        Ok((runner, address)) => serve(runner, address).await,
        Err(error) => {
            eprintln!("pneuma-restate: {error}");
            std::process::exit(1);
        }
    }
}
