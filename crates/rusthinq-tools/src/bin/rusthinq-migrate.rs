fn main() -> std::io::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "usage: rusthinq-migrate CONFIG_0.1 DESTINATION_DIRECTORY",
        ));
    }
    let report = rusthinq_tools::migration::migrate(
        std::path::Path::new(&args[0]),
        std::path::Path::new(&args[1]),
    )?;
    println!("{report}");
    Ok(())
}
