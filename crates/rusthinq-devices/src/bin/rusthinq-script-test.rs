//! Checks a directory of `.rhai` device drivers: `rusthinq-script-test [scripts_dir]`
//! (default: the current directory). Runs every driver test (`<dir>/tests/*.test.rhai`), then the
//! checks every driver must pass (a test file per driver, a descriptor well formed against
//! the IL). Exits non-zero if a test fails, none is found, or a check fails.

use rusthinq_devices::scripting::driver_check::check_dir;
use rusthinq_devices::scripting::script_test::{print_reports, run_dir};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let dir = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut ok = true;

    match run_dir(&dir) {
        Ok(reports) => {
            let failed = print_reports(&reports);
            if failed > 0 || reports.is_empty() {
                ok = false;
            }
        }
        Err(e) => {
            eprintln!("{e}");
            ok = false;
        }
    }

    match check_dir(&dir) {
        Ok(report) => {
            for problem in &report.problems {
                println!("  CHECK {problem}");
            }
            println!(
                "{} driver(s) checked, {} problem(s)",
                report.drivers,
                report.problems.len()
            );
            if !report.problems.is_empty() || report.drivers == 0 {
                ok = false;
            }
        }
        Err(e) => {
            eprintln!("{e}");
            ok = false;
        }
    }

    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
