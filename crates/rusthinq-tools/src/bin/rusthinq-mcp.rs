#[tokio::main]
async fn main() -> std::io::Result<()> {
    rusthinq_tools::mcp::run().await
}
