#[tokio::main]
async fn main() -> std::io::Result<()> {
    rusthinq_tools::cli::run(std::env::args().collect()).await
}
