// This example uses a simplified conceptual API for wasi-http.
// A real implementation would be more verbose.

fn main() {
    println!("WASM Rust server starting...");
    loop {
        let req = wasi_http::get_request();
        let response = format!("Hello from Rust WASM! You requested: {}", req.uri);
        wasi_http::send_response(200, &response);
    }
} 