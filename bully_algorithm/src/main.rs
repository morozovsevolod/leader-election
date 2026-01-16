use anyhow::{Result, anyhow};
use tracing::{info, warn};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver},
    time::sleep,
    spawn
};

use crate::config::load_config;

pub mod config;

enum Message {
    Election,
    Coordinator(String),
    Heartbeat
}

enum State {
    Follower(String),
    Leader
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    info!("straing");

    let addr = std::env::var("PROCESS")?;
    let node_id = id(&addr)?;

    info!("going inside load config");
    let (higher_nodes, lower_nodes): (Vec<(String, u32)>, Vec<(String, u32)>) = load_config()?
        .into_iter()
        .map(|p| id(&p).map(|k| (p, k)))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|&(_, k)| k != node_id)
        .partition(|(_, k)| *k > node_id);

    let (sender, mut receiver) = mpsc::channel::<Message>(32);

    spawn(async move {
        let listener = if let Ok(x) = TcpListener::bind(format!("{addr}:5555")).await {
            x
        } else {
            panic!("Failed to start listener");
        };

        loop {
            let mut buf = [0u8; 256];
            if let Ok((mut stream, addr)) = listener.accept().await {
                info!("Received from {addr}");
                let n = match stream.read(&mut buf[..]).await {
                    Ok(n) => n,
                    Err(_) => {
                        warn!("Failed to read from stream.");
                        continue;
                    }
                };

                info!("Received from {addr} buffer {}", String::from_utf8_lossy(&buf[..n]).trim());
                let message = match String::from_utf8_lossy(&buf[..n]).trim() {
                    "ELECTION" => {
                        let size = stream.write(b"OK").await;
                        info!("Recevied and responded to the ELECTION by {addr} sent {size:?} bytes");
                        Message::Election
                    },
                    "COORDINATOR" => {
                        let _ = stream.write(b"OK").await;
                        info!("Recevied and responded to the COORDINATOR by {addr}");
                        Message::Coordinator(addr.ip().to_string())
                    },
                    "HEARTBEAT" => {
                        let _ = stream.write(b"ALIVE").await;
                        Message::Heartbeat
                    },
                    _ => { continue; }
                };

                if let Err(_) = sender.send(message).await {
                    panic!("No reciever found!")
                }
            }
        }
    });

    sleep(Duration::from_secs(2)).await;

    'outer: loop {
        match election(&higher_nodes, &lower_nodes, &mut receiver).await {
            State::Follower(coordinator) => {
                heartbeat(coordinator, &mut receiver).await;
            },
            State::Leader => {
                loop {
                    match receiver.recv().await {
                        Some(Message::Election) | Some(Message::Coordinator(_)) => { continue 'outer; },
                        Some(_) => { },
                        None => panic!("No sender found.")
                    }
                }
            }
        }
    }
}

fn id(p: &str) -> Result<u32> {
    Ok(p.split(".").nth(3).ok_or_else(|| anyhow!("Wrong `p` format!"))?.parse::<u32>()?)
}

async fn election(higher_nodes: &Vec<(String, u32)>, lower_nodes: &Vec<(String, u32)>, receiver: &mut Receiver<Message>) -> State {
    let mut flag = false;
    info!("{} higher nodes detected", higher_nodes.len());
    if !higher_nodes.is_empty() {
        for (node, _) in higher_nodes {
            let mut buf = [0; 256];
            let mut stream = if let Ok(x) = TcpStream::connect(format!("{node}:5555")).await {
                x
            } else {
                continue;
            };
            info!("Connected to the {node}");

            match stream.write(b"ELECTION").await {
                Ok(size) if size != 0 => {
                    info!("Wrote ELECTION to the {node} {size} bytes");
                },
                Ok(_) | Err(_) => {
                    continue;
                }
            }

            match stream.read(&mut buf[..]).await {
                Ok(size) if size != 0 => {
                    info!("read passed: {} == OK: {}", String::from_utf8_lossy(&buf[..size]).trim(), String::from_utf8_lossy(&buf[..size]).trim() == "OK");
                    if String::from_utf8_lossy(&buf[..size]).trim() == "OK" {
                        info!("Should be true");
                        flag = true;
                        break;
                    }
                },
                Ok(e) => {
                    info!("Received {e} bytes");
                }
                Err(e) => {
                    warn!("Actually failed for {node} with error {e}");
                    continue;
                }
            }
        }
    }

    if flag {
        info!("flag true: listening for the coordinator!");
        let mut coordinator;
        loop {
            sleep(Duration::from_secs(1)).await;
            match receiver.recv().await {
                Some(Message::Coordinator(node)) => {
                    coordinator = node;
                    break;
                },
                Some(_) => {}
                None => {
                    panic!("No sender!")
                }
            }
        }

        return State::Follower(coordinator);
    } else {
        info!("I am leader!");
        let mut buf = [0; 256];
        for (node, _) in lower_nodes {
            let mut stream = if let Ok(x) = TcpStream::connect(format!("{node}:5555")).await {
                x
            } else {
                continue;
            };

            match stream.write(b"COORDINATOR").await {
                Ok(size) if size != 0 => {
                    if String::from_utf8_lossy(&buf[..size]).trim() == "OK" {}
                },
                Ok(_) | Err(_) => {
                    continue;
                }
            }

            match stream.read(&mut buf[..]).await {
                Ok(size) if size != 0 => {
                    if String::from_utf8_lossy(&buf[..size]).trim() == "OK" { }
                },
                Ok(_) | Err(_) => {
                    continue;
                }
            }
        }

        return State::Leader;
    }
}

async fn heartbeat(coordinator: String, receiver: &mut Receiver<Message>) -> () {
    let addr = format!("{coordinator}:5555");
    let mut buf = [0; 256];
    info!("{addr} is leader!");
    loop {
        sleep(Duration::from_secs(5)).await;
        match receiver.try_recv() {
            Ok(Message::Election) | Ok(Message::Coordinator(_)) => { return; },
            Ok(_) => { },
            Err(mpsc::error::TryRecvError::Empty) => {},
            Err(_) => panic!("No sender found.")
        }

        if let Ok(mut stream) = TcpStream::connect(&addr).await {
            let _ = stream.write(b"HEARTBEAT").await;
            let _ = stream.read(&mut buf[..]).await;
        } else {
            return;
        }
    }
}
