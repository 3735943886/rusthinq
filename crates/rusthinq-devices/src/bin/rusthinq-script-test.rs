//! Runs the driver tests (`<scripts>/tests/*.test.rhai`) of a directory of `.rhai` device
//! scripts: `rusthinq-script-test [scripts_dir]` (default `./scripts`). Exits non-zero if
//! any test fails or none is found.

use rusthinq_devices::scripting::script_test::{print_reports, run_dir};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let dir = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("scripts"));
    match run_dir(&dir) {
        Ok(reports) => {
            let failed = print_reports(&reports);
            if failed > 0 || reports.is_empty() {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
