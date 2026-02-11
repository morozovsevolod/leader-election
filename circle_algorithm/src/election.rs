use std::{sync::{Arc}, time::Duration};
use tokio::{sync::{RwLock, mpsc::Receiver}, time::Instant};
use tonic::transport::Endpoint;
use anyhow::{Result, anyhow};
use tracing::{info, warn};

use crate::{State, generated::circle_node_service::{CircleNodeResponse, CoordinatorRequest, ElectionRequest, circle_node_client::CircleNodeClient}, ring::Ring};

pub async fn send_election_further(ring: Arc<Ring>, election_nodes: Vec<i32>) -> Result<()> {
    for (node, k) in ring.fancy_iter() {
        info!("Sending to {}", k);
        let endpoint = Endpoint::from_shared(format!("http://{node}:50051"))?
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(3));
        let mut client = match CircleNodeClient::connect(endpoint).await {
            Ok(x) => x,
            Err(_) => {
                info!("Client connect failed {node:?}");
                continue;
            }
        };

        let request = ElectionRequest { nodes: election_nodes.clone() };
        if let Ok(inner_response) = client.election(request).await {
            let CircleNodeResponse { success } = inner_response.into_inner();
            if success {
                return Ok(());
            } else {
                info!("Node {node} responded success: false to the election");
            }
        }
    }

    Err(anyhow::anyhow!("Request didnt result in success true"))
}

pub async fn send_coordinator_further(nodes: Arc<Ring>, election_nodes: Vec<i32>) -> Result<()> {
    for (node, k) in nodes.fancy_iter() {
        info!("Sending to {k}");
        let endpoint = Endpoint::from_shared(format!("http://{node}:50051"))?
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(3));
        let mut client = match CircleNodeClient::connect(endpoint).await {
            Ok(x) => x,
            Err(_) => {
                info!("Client connect failed {node:?}");
                continue;
            }
        };

        let request = CoordinatorRequest { nodes: election_nodes.clone() };
        if let Ok(inner_response) = client.coordinator(request).await {
            let CircleNodeResponse { success } = inner_response.into_inner();
            if success {
                return Ok(());
            }
        }
    }

    Err(anyhow!("Request didnt result in success true"))
}

pub enum Message {
    Election,
    Coordinator(i32),
    Leader
}

pub async fn election_worker(_: i32, ring: Arc<Ring>, state: Arc<RwLock<State>>, mut receiver: Receiver<Message>) -> Result<()> {
    let mut timeout = Instant::now();
    loop {
        match receiver.recv().await {
            Some(Message::Election) => {
                if timeout.elapsed() > Duration::from_secs(5) {
                    info!("Chaning state to Candidate");
                    {
                        let mut rw = state.write().await;
                        *rw = State::Candidate;
                    }

                    timeout = Instant::now();
                }
            },
            Some(Message::Coordinator(id)) => {
                if let Some(leader_node) = ring.get_by_id(id) {
                    info!("Selected coordinator {id}");
                    {
                        let mut rw = state.write().await;
                        *rw = State::Follower(leader_node);
                    }
                    timeout = Instant::now();
                } else {
                    warn!("Failed to find node with id {id}");
                }
            },
            Some(Message::Leader) => {
                info!("I am leader");
                {
                    let mut rw = state.write().await;
                    *rw = State::Leader;
                }
                timeout = Instant::now();
            },
            None => {
                panic!("No producer found!");
            }
        }
    }
}
