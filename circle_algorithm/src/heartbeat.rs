use std::{sync::Arc, time::Duration};

use tokio::{sync::{RwLock, mpsc::Sender}, time::sleep};
use tonic::transport::Endpoint;
use tracing::{info, warn};

use crate::{State, election::Message, generated::circle_node_service::{CircleNodeResponse, circle_node_client::CircleNodeClient}};

pub async fn send_heartbeat(node: String, sender: Arc<Sender<Message>>, state: Arc<RwLock<State>>) -> anyhow::Result<()> {
    let endpoint = Endpoint::from_shared(format!("http://{node}:50051"))?
        .timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(3));

    info!("Sending heartbeat to {node}");

    loop {
        let mut client = match CircleNodeClient::connect(endpoint.clone()).await {
            Ok(x) => x,
            Err(_) => {
                let _ = sender.send(Message::Election).await;
                warn!("Client connect failed {node:?}");
                return Ok(());
            }
        };
        info!("Sending heartbeat to {node}");
        {
            let cur_state = state.read().await.clone();
            if cur_state == State::Leader || cur_state == State::Candidate {
                break;
            }
        }

        if let Ok(inner_response) = client.heartbeat(()).await {
            let CircleNodeResponse { success } = inner_response.into_inner();
            if !success {
                warn!("Leader node responded not in the expected way");
            }
        } else {
            let _ = sender.send(Message::Election).await;
            warn!("Client heartbeat send failed {node:?}");
            break;
        }

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
