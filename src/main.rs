mod queue;

use s9_binance_codec::websocket::Trade;
use s9_binance_websocket::binance_websocket::{BinanceNonBlockingWebSocket, BinanceWebSocketConfig, BinanceWebSocketConnection, ControlMessage, S9WebSocketClientHandler, WebSocketEvent};
use s9_parquet::{Record, TimestampInfo};
use std::collections::HashMap;
use std::str::Utf8Error;
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{fs, thread};
use std::io::Read;
use ctrlc::Error;
use tiny_http::{Method, Response, Server};
use crate::queue::{ConcurrentQueuedParquetWriter};

const MAX_STREAMS: u16 = 1024;

fn main() {

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // TODO: Make configurable with clap
    let max_records_per_parquet_group = 128;
    let flush_timeout = Duration::from_secs(5);

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
        "bnbusdt@trade".to_string(),
        "xrpusdt@trade".to_string(),
        "bardusdt@trade".to_string(),
        "adaeur@trade".to_string(),
        "dogeusdt@trade".to_string(),
        "dogeusdt@trade".to_string(),
        "asterusdc@trade".to_string(),
        "xplusdc@trade".to_string(),
    ];

    if let Err(e) = setup_application_directories() {
        println!("Error setting up application directories: {}", e);
        return;
    }

    let data_dir = "data";
    let file_paths: Vec<String> = streams.iter().map(|stream| format!("{}/{}.parquet", data_dir, stream.replace("@", "."))).collect();

    let mut writers: HashMap<String, ConcurrentQueuedParquetWriter> = HashMap::new();
    for (i, file_path) in file_paths.iter().enumerate() {
        let writer = ConcurrentQueuedParquetWriter::new(file_path, max_records_per_parquet_group, flush_timeout);
        match writer {
            Ok(writer) => {
                let symbol = streams[i].split('@').next().unwrap().to_uppercase();
                writers.insert(symbol, writer);
            }
            Err(e) => {
                println!("Error creating ConcurrentQueuedParquetWriter for {}: {}", file_path, e);
                return;
            }
        }
    }

    let writers_for_error_handling = writers.iter()
        .map(|(symbol, writer)|
            (symbol.clone(), writer.error_receiver().clone()))
        .collect::<Vec<(String, Receiver<String>)>>();
    thread::spawn(move || {
        loop {
            for (symbol, error_receiver) in &writers_for_error_handling {
                if let Ok(error_message) = error_receiver.try_recv() {
                    println!("Error from writer for {}: {}", symbol, error_message);
                }
            }
            // TODO: This is non-blocking polling, maybe find a better way that uses event driven style or so
            thread::sleep(Duration::from_secs(1)); // TODO: Make configurable
        }
    });

    let result = BinanceNonBlockingWebSocket::connect(config);
    match result {
        Ok(mut ws) => {
            let control_tx = ws.s9_websocket_client.control_tx.clone();
            let event_rx = ws.s9_websocket_client.event_rx.clone();

            setup_ctrlc_handler(&control_tx);
            setup_http_shutdown_handler(&control_tx);

            ws.run_non_blocking();

            let mut running = true;
            while running {
                select! {
                    recv(event_rx) -> msg => match msg {
                        Ok(event) => match event {
                            WebSocketEvent::Activated => {
                                println!("Websocket activated, subscribing to streams...");
                                let result = ws.subscribe_to_streams_non_blocking(streams.clone());
                                match result {
                                    Ok(_) => {},
                                    Err(e) => {
                                        println!("Error subscribing to streams: {}", e);
                                        let result = control_tx.send(ControlMessage::Close());
                                        if let Err(e) = result {
                                            println!("Error sending close message to chanel, : {}", e);
                                            // TODO: Shutdown writers and application manually here
                                        }
                                    }
                                }
                            },
                            WebSocketEvent::TextMessage(mut data) => {
                                let timestamp_info = get_timestamp_info().unwrap();
                                let utf8: Result<&str, Utf8Error> = str::from_utf8(&data);
                                match utf8 {
                                    Ok(utf8) => {
                                        let trade = Trade::from_json(utf8);
                                        match trade {
                                            Ok(trade) => {
                                                let record = Record {
                                                    timestamp_info: timestamp_info.clone(),
                                                    data: data.to_vec(),
                                                };

                                                let queued_writer = writers.get_mut(&trade.symbol.to_uppercase());
                                                match queued_writer {
                                                    Some(queued_writer) => {
                                                        let enqueue_result = queued_writer.enqueue(record);
                                                        match enqueue_result {
                                                            Ok(_) => {
                                                                println!("Enqueued records for {} at timestamp {:?}",
                                                                         trade.symbol, timestamp_info);
                                                            }
                                                            Err(e) => {
                                                                println!("Error enqueuing records for {} at timestamp {:?}: {}",
                                                                         trade.symbol, timestamp_info, e);
                                                            }

                                                        }
                                                        /*if queued_writer.is_full() {
                                                            let result = queued_writer.flush();
                                                            let file_path = queued_writer.file_path().to_string_lossy().to_string();
                                                            match result {
                                                                Ok(flushed_records_size) => {
                                                                    println!("Wrote {} records for {} to parquet file {} at timestamp {:?}",
                                                                             flushed_records_size, trade.symbol, file_path, timestamp_info);
                                                                }
                                                                Err(e) => {
                                                                    println!("Error writing records for {} to parquet file {} at timestamp {:?}: {}",
                                                                             trade.symbol, file_path, timestamp_info, e);
                                                                }
                                                            }
                                                        }*/
                                                    }
                                                    None => {
                                                        println!("No writer found for symbol: {}", trade.symbol);
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

                                //let message = std::str::from_utf8(data).unwrap();
                                //println!("Received text message: {:?}", message);
                            },
                            WebSocketEvent::BinaryMessage(data) => {
                                println!("Received binary message: {:?}", data);
                            },
                            WebSocketEvent::ConnectionClosed(reason) => {
                                println!("Connection closed: {:?}", reason);
                                close_parquet_writers(&mut writers);
                            },
                            WebSocketEvent::Error(error) => {
                                println!("Error: {}", error);
                                // TODO: shutdown or recover?
                            },
                            WebSocketEvent::Quit => {
                                println!("Binance WebSocket quitted");
                                close_parquet_writers(&mut writers);
                                running = false;
                            },
                            _ => {}
                        },
                        Err(recv_error) => {
                            println!("Error receiving message from channel: {}", recv_error);
                            // TODO: shutdown or recover?
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Error connecting to Binance WebSocket: {}", e);
        }
    };

    /*struct MessageHandler {
        queued_writers: HashMap<String, ConcurrentQueuedParquetWriter>,
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
                            let record = Record {
                                timestamp_info: timestamp_info.clone(),
                                data: data.to_vec(),
                            };

                            let queued_writer = self.queued_writers.get_mut(&trade.symbol.to_uppercase());
                            match queued_writer {
                                Some(queued_writer) => {
                                    let enqueue_result = queued_writer.enqueue(record);
                                    match enqueue_result {
                                        Ok(_) => {
                                            println!("Enqueued records for {} at timestamp {:?}",
                                                     trade.symbol, timestamp_info);
                                        }
                                        Err(e) => {
                                            println!("Error enqueuing records for {} at timestamp {:?}: {}",
                                                     trade.symbol, timestamp_info, e);
                                        }
                                        
                                    }
                                    /*if queued_writer.is_full() {
                                        let result = queued_writer.flush();
                                        let file_path = queued_writer.file_path().to_string_lossy().to_string();
                                        match result {
                                            Ok(flushed_records_size) => {
                                                println!("Wrote {} records for {} to parquet file {} at timestamp {:?}",
                                                         flushed_records_size, trade.symbol, file_path, timestamp_info);
                                            }
                                            Err(e) => {
                                                println!("Error writing records for {} to parquet file {} at timestamp {:?}: {}",
                                                         trade.symbol, file_path, timestamp_info, e);
                                            }
                                        }
                                    }*/
                                }
                                None => {
                                    println!("No writer found for symbol: {}", trade.symbol);
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

            //let message = std::str::from_utf8(data).unwrap();
            //println!("Received text message: {:?}", message);
        }

        fn on_binary_message(&mut self, data: &[u8]) {
            println!("Received binary message: {:?}", data);
        }

        fn on_connection_closed(&mut self, reason: Option<String>) {
            println!("Connection closed: {:?}", reason);
            close_parquet_writers(&mut self.queued_writers);
        }

        fn on_error(&mut self, error: String) {
            println!("Error: {}", error);
        }

        fn on_quit(&mut self) {
            println!("Binance WebSocket quitted");
            close_parquet_writers(&mut self.queued_writers);
        }
    }

    let result = BinanceBlockingWebSocket::connect(config);
    match result {
        Ok(mut ws) => {
            let result = ws.subscribe_to_streams_blocking(streams);
            match result {
                Ok(_) => {
                    let mut handler = MessageHandler{
                        queued_writers: writers,
                    };
                    ws.run_blocking(&mut handler, control_rx);
                }
                Err(e) => {
                    println!("Error subscribing to streams: {}", e);
                }
            }
        }
        Err(e) => {
            println!("Error connecting to Binance WebSocket: {}", e);
        }
    }*/
}

fn setup_ctrlc_handler(control_tx: &Sender<ControlMessage>) {
    let ctrlc_tx = control_tx.clone();
    let ctrlc_result = ctrlc::set_handler(move || {
        println!("Received one of SIGINT, SIGTERM and SIGHUP signal, initiating graceful shutdown...");
        // TODO: Eval result
        ctrlc_tx.send(ControlMessage::Close());
        let ctrlc_tx = ctrlc_tx.clone();
        thread::spawn(move || {
            // TODO: Make duration configurable
            println!("Waiting for 5 seconds to force quit...");
            thread::sleep(Duration::from_secs(5));
            println!("Forcing quit...");
            // TODO: Eval result
            ctrlc_tx.send(ControlMessage::ForceQuit());
        });
    });

    match ctrlc_result {
        Ok(_) => println!("Ctrl-C (SIGINT(2), SIGTERM(15) and SIGHUP(1)) handler set successfully."),
        Err(e) => println!("Error setting Ctrl-C (SIGINT(2), SIGTERM(15) and SIGHUP(1)) handler: {}", e),
    }
}

fn setup_http_shutdown_handler(control_tx: &Sender<ControlMessage>) {
    let http_control_tx = control_tx.clone();
    thread::spawn(move || {
        let server_addr = "0.0.0.0:8090"; // TODO Make configurable
        let server = match Server::http(server_addr) {
            Ok(server) => server,
            Err(e) => {
                println!("Error starting HTTP server: {}", e);
                return;
            }
        };
        println!("Admin HTTP server listening on http://{}", server_addr);

        for request in server.incoming_requests() {
            match (request.method(), request.url()) {
                (Method::Post, "/shutdown") => {
                    println!("Received shutdown request via HTTP, initiating graceful shutdown...");
                    // TODO: Eval results
                    http_control_tx.send(ControlMessage::Close());
                    let response = Response::from_string("Shutdown initiated.");
                    request.respond(response);
                    let http_control_tx = http_control_tx.clone();
                    thread::spawn(move || {
                        // TODO: Make duration configurable
                        println!("Waiting for 5 seconds to force quit...");
                        thread::sleep(Duration::from_secs(5));
                        println!("Forcing quit...");
                        http_control_tx.send(ControlMessage::ForceQuit());
                    });
                },
                (Method::Get, "/health") => {
                    let response = Response::from_string("OK");
                    request.respond(response.with_status_code(200));
                }
                _ => {
                    let response = Response::from_string("Not Found");
                    request.respond(response.with_status_code(404));
                }
            }
        }
    });
}

fn close_parquet_writers(queued_writers: &mut HashMap<String, ConcurrentQueuedParquetWriter>) {
    println!("Closing {} writers ...", queued_writers.len());
    for (symbol, queued_writer) in queued_writers.drain() {
        match queued_writer.shutdown() {
            Ok(_) => {
                println!("Closed writer for {}.", symbol);
            }
            Err(e) => {
                println!("Error closing writer for {}: {:?}", symbol, e);
            }
        }
    }
}

// TODO: Move timestamping to s9_websocket right after recv() and populate it through the trait
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

fn setup_application_directories() -> Result<(), Box<dyn std::error::Error>> {
    let directories = vec![
        "data",
    ];
    for dir in directories {
        fs::create_dir_all(dir)?;
        println!("Created directory: {}", dir);
    }
    Ok(())
}
