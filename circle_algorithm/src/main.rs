use std::{sync::Arc, time::Duration};

use tokio::{spawn, sync::{RwLock, mpsc}, time::sleep};
use tonic::transport::Server;
use tracing::{error, info};
use anyhow::Result;

use crate::{config::load_config, election::{election_worker, send_election_further}, generated::circle_node_service::circle_node_server::CircleNodeServer, heartbeat::send_heartbeat, ring::{Ring, id}, service::CircleNodeService};

pub mod generated;
pub mod ring;
pub mod config;
pub mod service;
pub mod election;
pub mod heartbeat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Candidate,
    Follower(String),
    Leader
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    info!("Starting");

    let addr = std::env::var("PROCESS")?;
    info!("Starting server {addr}");
    let node_id: i32 = id(&addr)?;
    let ring = Ring::new(addr, load_config()?)?;
    let state = Arc::new(RwLock::new(State::Candidate));
    let (sender, receiver) = mpsc::channel(32);

    let sender = Arc::new(sender);

    let sender_clone = sender.clone();
    let ring_clone = ring.clone();
    spawn(async move {
        let addr = "0.0.0.0:50051".parse().unwrap();
        let _ = Server::builder()
            .add_service(CircleNodeServer::new(CircleNodeService::new(node_id, sender_clone, ring_clone)))
            .serve(addr)
            .await;

        error!("Shouldnt be here");
    });

    let ring_clone = ring.clone();
    let state_clone = state.clone();
    spawn(async move {
        let _ = election_worker(node_id, ring_clone, state_clone, receiver).await;

        error!("Shouldnt be here");
    });

    sleep(Duration::from_secs(5)).await;
    let _ = send_election_further(ring.clone(), vec![node_id]).await;

    loop {
        let cur_state = state.read().await.clone();
        info!("State: {:?}", cur_state);
        if let State::Follower(node) = cur_state {
            let sender_clone = sender.clone();
            let state_clone = state.clone();
            if let Err(e) = send_heartbeat(node, sender_clone, state_clone).await {
                info!("Stopped heartbeat with error {e}");
            } else {
                info!("Stopped sending heartbeat");
            }

            let ring_clone = ring.clone();
            spawn(async move {
                let _ = send_election_further(ring_clone, vec![node_id]).await;
            });
        } else {
            sleep(Duration::from_secs(5)).await;
        }
    }
}
