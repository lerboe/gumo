use tokio::net::TcpListener;

/// Usage: `example [ADDRESS]`, where ADDRESS defaults to `127.0.0.1:3000`.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:3000".to_string());
    let listener = TcpListener::bind(&address).await?;
    println!("echo server listening on http://{}", listener.local_addr()?);
    example::serve(listener).await
}
