use std::collections::HashMap;
use std::str::Utf8Error;
use std::sync::{mpsc};
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use s9_binance_codec::websocket::Trade;
use s9_binance_websocket::binance_websocket::{BinanceWebSocket, BinanceWebSocketConfig, BinanceWebSocketConnection, ControlMessage, S9WebSocketClientHandler};
use s9_parquet::{Entry, ParquetWriter, TimestampInfo};

const MAX_STREAMS: u16 = 1024;

fn main() {

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (shutdown_tx, shutdown_rx) = mpsc::channel();

    let ctrlc_result = ctrlc::set_handler(move || {
        println!("Received one of SIGINT, SIGTERM and SIGHUP signal, initiating graceful shutdown...");
        let _ = shutdown_tx.send(ControlMessage::Close());
    });

    match ctrlc_result {
        Ok(_) => println!("Ctrl-C (SIGINT(2), SIGTERM(15) and SIGHUP(1)) handler set successfully."),
        Err(e) => println!("Error setting Ctrl-C (SIGINT(2), SIGTERM(15) and SIGHUP(1)) handler: {}", e),
    }

    //rustls::crypto::ring::default_provider().install_default().expect("Failed to install Rustls provider");
    //rustls::crypto::aws_lc_rs::default_provider().install_default().expect("Failed to install AWS-LC provider");

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

    let file_paths: Vec<String> = streams.iter().map(|stream| format!("{}.parquet", stream.replace("@", "."))).collect();

    let mut writers: HashMap<String, ParquetWriter> = HashMap::new();
    for (i, file_path) in file_paths.iter().enumerate() {
        let writer = ParquetWriter::new(file_path);
        match writer {
            Ok(writer) => {
                let symbol = streams[i].split('@').next().unwrap().to_uppercase();
                writers.insert(symbol, writer);
            }
            Err(e) => {
                println!("Error creating Parquet writer for {}: {}", file_path, e);
                return;
            }
        }
    }

    struct MessageHandler {
        writers: HashMap<String, ParquetWriter>,
    }

    impl S9WebSocketClientHandler for MessageHandler {
        fn on_text_message(&mut self, data: &[u8]) {
            let timestamp_info = get_timestamp_info().unwrap();

            let utf8: Result<&str, Utf8Error> = str::from_utf8(data);
            match utf8 {
                Ok(utf8) => {

                    let trade = Trade::from_json(utf8);
                    match trade {
                        Ok(trade) => {
                            let entry = Entry {
                                timestamp_info: timestamp_info.clone(),
                                data: data.to_vec(),
                            };

                            let writer = self.writers.get_mut(&trade.symbol.to_uppercase());
                            match writer {
                                Some(writer) => {
                                    let result = writer.write(&entry);
                                    let file_path = writer.file_path.to_str().unwrap();
                                    match result {
                                        Ok(_) => {
                                            println!("Wrote entry for {} to parquet file {}: {:?}", trade.symbol, file_path, entry.timestamp_info);
                                        }
                                        Err(e) => {
                                            println!("Error writing parquet entry to {}: {}", trade.symbol, e);
                                        }
                                    }
                                }
                                None => {
                                    println!("No parquet writer found for symbol: {}", trade.symbol);
                                }
                            }

                        }
                        Err(e) => {
                            println!("Error parsing trade: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error parsing text message to utf8 string: {}", e);
                }
            }

            let message = std::str::from_utf8(data).unwrap();
            println!("Received text message: {:?}", message);
        }

        fn on_binary_message(&mut self, data: &[u8]) {
            println!("Received binary message: {:?}", data);
        }

        fn on_connection_closed(&mut self, reason: Option<String>) {
            println!("Connection closed: {:?}", reason);
            close_parquet_writers(&mut self.writers);
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
                    let mut handler = MessageHandler{
                        writers,
                    };
                    ws.run(&mut handler, shutdown_rx);
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

fn close_parquet_writers(writers: &mut HashMap<String, ParquetWriter>) {
    println!("Closing Parquet writers...");
    for (symbol, writer) in writers.drain() {
        match writer.close() {
            Ok(_) => {
                println!("Closed Parquet writer for {}.", symbol);
            }
            Err(e) => {
                println!("Error closing Parquet writer for {}: {}", symbol, e);
            }
        }
    }
}

fn get_timestamp_info() -> Result<TimestampInfo, SystemTimeError> {
    let current_system_time = SystemTime::now();
    let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH)?;
    let timestamp_info = TimestampInfo {
        timestamp_millis: duration_since_epoch.as_millis() as i64,
        timestamp_sec: duration_since_epoch.as_secs() as i64,
        timestamp_sub_sec: duration_since_epoch.subsec_nanos() as i32,
    };
    Ok(timestamp_info)
}
