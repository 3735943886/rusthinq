//! Runs the driver tests that live beside the drivers (`scripts/tests/*.test.rhai`, see
//! `rusthinq_devices::scripting::script_test`), and checks every driver has some.
#![cfg(feature = "scripting")]

use rusthinq_devices::scripting::script_test::{print_reports, run_dir};
use std::path::PathBuf;

fn scripts_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts")
}

/// The device drivers in `scripts/`: every `.rhai` that is not a shared module.
pub fn drivers() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(scripts_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|n| n.strip_suffix(".rhai").map(str::to_string))
        .filter(|n| !n.ends_with("_common"))
        .collect();
    names.sort();
    names
}

#[test]
fn every_driver_test_passes() {
    let reports = run_dir(&scripts_dir()).unwrap();
    let failed = print_reports(&reports);
    assert_eq!(
        failed, 0,
        "{failed} driver test(s) failed (see the output above)"
    );
}

#[test]
fn every_driver_has_tests() {
    for name in drivers() {
        let file = scripts_dir()
            .join("tests")
            .join(format!("{name}.test.rhai"));
        assert!(file.is_file(), "{name}.rhai has no {}", file.display());
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(
            text.contains("fn test_"),
            "{} has no test_ function",
            file.display()
        );
    }
}
