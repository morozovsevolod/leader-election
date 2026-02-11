use std::sync::Arc;

use tokio::{spawn, sync::mpsc::Sender};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    election::{Message, send_coordinator_further, send_election_further}, generated::circle_node_service::{
        CircleNodeResponse, CoordinatorRequest, ElectionRequest, circle_node_server::CircleNode as CircleNodeServer
    }, ring::Ring
};

pub struct CircleNodeService {
    node_id: i32,
    sender: Arc<Sender<Message>>,
    ring: Arc<Ring>
}

impl CircleNodeService {
    pub fn new(node_id: i32, sender: Arc<Sender<Message>>, ring:Arc<Ring>) -> Self {
        CircleNodeService {
            node_id,
            sender,
            ring
        }
    }
}

#[tonic::async_trait]
impl CircleNodeServer for CircleNodeService {
    async fn election(
        &self,
        request: Request<ElectionRequest>,
    ) -> Result<Response<CircleNodeResponse>, Status> {
        let addr = request.remote_addr();
        let ElectionRequest { mut nodes } = request.into_inner();

        let _ = self.sender.send(Message::Election).await;
        info!("Received election {nodes:?} from {addr:?}");
        if let Some(first) = nodes.get(0) {
            let ring_clone = self.ring.clone();
            if *first == self.node_id {
                spawn(async move {
                    let _ = send_coordinator_further(ring_clone, nodes).await;
                });
            } else {
                nodes.push(self.node_id);
                spawn(async move {
                    let _ = send_election_further(ring_clone, nodes).await;
                });
            }
        } else {
            warn!("Received an empty election message");
            return Ok(CircleNodeResponse { success: false }.into());
        }

        Ok(CircleNodeResponse { success: true }.into())
    }

    async fn coordinator(
        &self,
        request: Request<CoordinatorRequest>,
    ) -> Result<Response<CircleNodeResponse>, Status> {
        let addr = request.remote_addr();
        let CoordinatorRequest { nodes } = request.into_inner();

        info!("Received coordinator {nodes:?} from {addr:?}");

        if let Some(first) = nodes.get(0) {
            if *first == self.node_id {
                match nodes.iter().max() {
                    Some(x) if *x == self.node_id => {
                        let _ = self.sender.send(Message::Leader).await;
                    },
                    Some(leader_id) => {
                        let _ = self.sender.send(Message::Coordinator(*leader_id)).await;
                    },
                    None => {
                        warn!("Failed to select leader for Coordinator {nodes:?}");
                        return Ok(CircleNodeResponse { success: false }.into());
                    }
                }
            } else {
                match nodes.iter().max() {
                    Some(x) if *x == self.node_id => {
                        let _ = self.sender.send(Message::Leader).await;
                    },
                    Some(leader_id) => {
                        let _ = self.sender.send(Message::Coordinator(*leader_id)).await;
                    },
                    None => {
                        warn!("Failed to select leader for Coordinator {nodes:?}");
                        return Ok(CircleNodeResponse { success: false }.into());
                    }
                }

                let ring_clone = self.ring.clone();
                spawn(async move {
                    let _ = send_coordinator_further(ring_clone, nodes).await;
                });
            }
        } else {
            warn!("Received an empty coordinator message");
            return Ok(CircleNodeResponse { success: false }.into());
        }

        Ok(CircleNodeResponse { success: true }.into())
    }

    async fn heartbeat(
        &self,
        request: Request<()>,
    ) -> Result<Response<CircleNodeResponse>, Status> {
        let addr = request.remote_addr();

        info!("Received heartbeat from {addr:?}");

        Ok(CircleNodeResponse { success: true }.into())
    }
}
