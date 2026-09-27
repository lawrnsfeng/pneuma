//! Schema fingerprinting and baselining, as a command.
//!
//! Wiring only. `scripts/coverage.sh` excludes this file, because `main` is
//! never executed by the test suite, and refuses to let it define anything —
//! so parsing lives in `pneuma_migrate::cli`, execution in
//! `pneuma_migrate::run`, and both are tested.

use pneuma_migrate::{cli, run};

#[tokio::main]
async fn main() {
    let invocation = match cli::parse(std::env::args_os()) {
        Ok(invocation) => invocation,
        // clap already renders `--help`, `--version` and usage errors; its
        // exit code distinguishes the two.
        Err(error) => error.exit(),
    };
    let outcome = run::run(invocation, |key| std::env::var(key).ok()).await;
    match &outcome {
        Ok(report) => println!("{}", cli::describe(report)),
        Err(error) => eprintln!("pneuma-migrate: {error}"),
    }
    std::process::exit(cli::exit_code(&outcome));
}
