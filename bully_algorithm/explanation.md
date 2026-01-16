We consider N processes: {P_0, .., P_N-1} and let id(P)=k. When any process notices that coordinator is no longer responding to requests, it initiates an election. A process, P_k, holds an election as follows:

1. P_k sends an **ELECTION** message to all processes with higher identifiers P_k+1, P_k+2, ..., P_N-1
2. If no one responds, P_k wins the election and becomes coordinator.
3. If no one of the higher-ups answer, it takes over and P_k`s job is done.

At any moment, a process can get an ELECTION message from one of its lower-numbered colleagues. When such message arrives, the reciever sends an OK message back to the sender to indicate that it is alive and will take over. The reciever then holds and election, unless it is already holding one. Eventually,  all processes give up but one, and that one is the new coordinator. It announces its viectory by sending all processes **COORDINATOR** message.

If a process that was previously down comes back up, it holds an election. If it happens to be the highest-numbered process currently running, it will win the election and take over the coordinator`s job. Thus the biggest guy in the town always wins, hence the name "bully algorithm".

(c) Distributed systems. Fourth edition. Maarten van Steen andv Andrew S. Tanenbaum.

#### *In this implementation:*
- Following are assumed to be synonyms: process = node, coordinator=leader.
- id(P)=k assumes P to be an ip address of the node, it takes last number from the ip address. Thus, the limitation is up to 255 processes.
- Heartbeat is for detection is leader down. Only every 5s, just to keep load small.
