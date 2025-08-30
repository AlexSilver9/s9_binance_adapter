use std::collections::HashMap;
use s9_binance_websocket::binance_websocket::{BinanceWebSocket, BinanceWebSocketConfig, BinanceWebSocketConnection, S9WebSocketClientHandler};

const MAX_STREAMS: u16 = 1024;

fn main() {

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    //rustls::crypto::ring::default_provider().install_default()
    //    .expect("Failed to install Rustls provider");
    rustls::crypto::aws_lc_rs::default_provider().install_default()
        .expect("Failed to install AWS-LC provider");

    let mut client_handler = MessageHandler;
    let connection = BinanceWebSocketConnection {
        protocol: "wss".to_string(),
        host: "stream.binance.com".to_string(),
        port: 9443,
        path: "/ws".to_string(),
        headers: HashMap::new(),
    };

    let config = BinanceWebSocketConfig {
        connection,
    };

    let streams = vec![
        "btcusdt@trade".to_string(),
        "ethusdt@trade".to_string(),
        "solusdt@trade".to_string(),
        "adausdt@trade".to_string(),
    ];

    struct MessageHandler;

    impl S9WebSocketClientHandler for MessageHandler {
        fn on_text_message(&mut self, data: &[u8]) {
            println!("Received text message: {:?}", std::str::from_utf8(data).unwrap());
        }

        fn on_binary_message(&mut self, data: &[u8]) {
            println!("Received binary message: {:?}", data);
        }

        fn on_connection_closed(&mut self, reason: Option<String>) {
            println!("Connection closed: {:?}", reason);
        }

        fn on_error(&mut self, error: String) {
            println!("Error: {}", error);
        }
    }

    let result = BinanceWebSocket::connect(config);
    match result {
        Ok(mut ws) => {
            let result = ws.subscribe_to_streams(streams);
            match result {
                Ok(_) => {
                    let mut handler = MessageHandler;
                    ws.run(&mut handler);
                }
                Err(e) => {
                    println!("Error subscribing to streams: {}", e);
                }
            }
        }
        Err(e) => {
            println!("Error connecting to Binance WebSocket: {}", e);
        }
    }
}
