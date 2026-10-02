fn main() -> std::io::Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| ".".into());
    let reports = rusthinq_scripting::testing::run(&path)?;
    let failed = reports.iter().filter(|r| r.error.is_some()).count();
    for report in &reports {
        if let Some(error) = &report.error {
            println!("FAIL {} / {}: {}", report.model, report.test, error);
        }
    }
    println!("{} tests; {} failures", reports.len(), failed);
    if failed > 0 {
        Err(std::io::Error::other("driver tests failed"))
    } else {
        Ok(())
    }
}
