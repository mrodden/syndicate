// Copyright (C) 2021 Mathew Odden <mathewrodden@gmail.com>. All rights reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use std::cmp::{max, min};
use std::convert::TryInto;
use std::io::{Cursor, Read};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub type NodeId = u64;

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    AppendEntries(AppendEntriesRequest),
    Vote(VoteRequest),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VoteResponse {
    pub term: u64,
    pub granted: bool,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

fn call_rpc(req: Request, addr: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    req.serialize(&mut Serializer::new(&mut buf)).unwrap();

    let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
    sock.connect(addr).unwrap();
    let sent = sock.send(&buf).unwrap();
    if sent < buf.len() {
        println!("not all data sent!");
    }

    let mut rbuf = [0; 4096];
    let br = sock.recv(&mut rbuf)?;
    let data = &mut rbuf[..br];
    Ok(data.to_vec())
}

pub fn append_entries(req: &AppendEntriesRequest, addr: &str) -> Result<AppendEntriesResponse> {
    let data = call_rpc(Request::AppendEntries(req.clone()), addr)?;
    let resp: AppendEntriesResponse = rmp_serde::from_read_ref(&data).unwrap();
    Ok(resp)
}

pub fn request_vote(req: &VoteRequest, addr: &str) -> Result<VoteResponse> {
    let data = call_rpc(Request::Vote(req.clone()), addr)?;
    let resp: VoteResponse = rmp_serde::from_read_ref(&data).unwrap();
    Ok(resp)
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct LogEntry {
    term: u64,
    key: String,
    val: String,
}

impl LogEntry {
    pub fn new(term: u64, key: &str, val: &str) -> Self {
        LogEntry {
            term,
            key: key.into(),
            val: val.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    state: Arc<Mutex<NodeState>>,
}

#[derive(Clone, Debug)]
struct NodeState {
    node_id: NodeId,

    // persistent
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,

    // volatile
    commit_index: u64,
    last_applied: u64,

    peers: Vec<Peer>,
    mode: Mode,

    last_request: Instant,
}

#[derive(Clone, Debug)]
pub struct Peer {
    addr: String,
    next_index: u64,
    match_index: u64,
}

impl Peer {
    pub fn new(addr: &str) -> Self {
        Peer {
            addr: addr.to_string(),
            next_index: 1,
            match_index: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Follower,
    Candidate,
    Leader,
}

trait RPCServer {
    fn serve_forever(&self, bind_addr: SocketAddr);
}

impl RPCServer for Node {
    fn serve_forever(&self, bind_addr: SocketAddr) {
        let socket = UdpSocket::bind(bind_addr).expect("failed to bind socket");
        info!("Listening on udp://{}", bind_addr);
        let mut buf = [0; 4096];

        loop {
            let (bytes_read, src_addr) = socket.recv_from(&mut buf).unwrap();
            let mesg = &mut buf[..bytes_read];

            let req: Request = rmp_serde::from_read_ref(&mesg).unwrap();

            match req {
                Request::AppendEntries(r) => {
                    let resp = self.handle_append_entries(r);
                    let buf = rmp_serde::to_vec(&resp).unwrap();
                    socket.send_to(&buf, src_addr);
                }
                Request::Vote(r) => {
                    let resp = self.handle_request_vote(r);
                    let buf = rmp_serde::to_vec(&resp).unwrap();
                    socket.send_to(&buf, src_addr);
                }
                _ => {
                    socket.send_to(b"invalid request", src_addr);
                }
            }
        }
    }
}

impl Node {
    pub fn new(node_id: u64, peers: Vec<Peer>) -> Self {
        Node {
            state: Arc::new(Mutex::new(NodeState {
                node_id,
                current_term: 0,
                voted_for: None,
                log: vec![],
                commit_index: 0,
                last_applied: 0,
                peers: peers.into(),
                mode: Mode::Follower,
                last_request: Instant::now(),
            })),
        }
    }

    pub fn run(&self) {
        let l_clone = self.clone();
        let leader_thread = thread::spawn(move || {
            l_clone.leader_main();
        });

        let sync_clone = self.clone();
        let state_sync_thread = thread::spawn(move || {
            sync_clone.state_sync_main();
        });

        let addr = SocketAddr::from((
            [127, 0, 0, 1],
            3000 + self.state.lock().unwrap().node_id as u16,
        ));
        self.serve_forever(addr);
    }

    #[tracing::instrument]
    fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut inner = self.state.lock().unwrap();
        inner.last_request = Instant::now();
        debug!("Curr State: {:#?}", inner);

        if req.term > inner.current_term {
            inner.current_term = req.term;
            inner.mode = Mode::Follower;
        }

        let prev_log_idx: usize = req.prev_log_index.try_into().unwrap();

        if req.term < inner.current_term {
            return AppendEntriesResponse {
                term: inner.current_term,
                success: false,
            };
        }

        if prev_log_idx > 0 {
            // check previous log index and term
            if let Some(entry) = inner.log.get(prev_log_idx - 1) {
                if entry.term != req.prev_log_term {
                    return AppendEntriesResponse {
                        term: inner.current_term,
                        success: false,
                    };
                }
            } else {
                // log index was None
                return AppendEntriesResponse {
                    term: inner.current_term,
                    success: false,
                };
            }
        }

        let mut last_index: u64 = 0;

        // start processing log entries from request
        for (i, entry) in req.entries.iter().enumerate() {
            let mut curr_idx = prev_log_idx + i + 1;
            last_index = curr_idx.try_into().unwrap();

            if let Some(our_entry) = inner.log.get(curr_idx - 1) {
                // already have an entry at this index, check conflicting term
                if our_entry.term != entry.term {
                    // need to delete this and all that follow
                    inner.log.truncate(curr_idx - 1);
                } else {
                    // entry is correct, continue
                    continue;
                }
            }

            // no entry in log, or we just truncated; append
            inner.log.push(entry.clone());

            debug!("New State: {:#?}", inner);
        }

        if req.leader_commit > inner.commit_index {
            inner.commit_index = min(req.leader_commit, last_index);
        }

        info!("Final State: {:?}", inner);
        AppendEntriesResponse {
            term: inner.current_term,
            success: true,
        }
    }

    #[tracing::instrument]
    fn handle_request_vote(&self, req: VoteRequest) -> VoteResponse {
        let mut inner = self.state.lock().unwrap();

        if req.term < inner.current_term {
            info!("Vote requested for old term");
            return VoteResponse {
                term: inner.current_term,
                granted: false,
            };
        }

        if let Some(id) = inner.voted_for {
            if id != req.candidate_id {
                // already voted for another this term
                return VoteResponse {
                    term: inner.current_term,
                    granted: false,
                };
            }
        }

        let last_log_term = get_last_log_term(&inner.log);

        // compare logs
        if req.last_log_term > last_log_term {
            // their term is higher, therefore they are more up-to-date
            inner.last_request = Instant::now();
            return VoteResponse {
                term: 1,
                granted: true,
            };
        } else if req.last_log_term < last_log_term {
            // our last log term is higher, deny vote
            return VoteResponse {
                term: inner.current_term,
                granted: false,
            };
        }

        let last_log_index = get_last_log_index(&inner.log);

        // equal terms, compare lengths of logs
        if req.last_log_index < last_log_index {
            // not as as up to date as ours
            return VoteResponse {
                term: inner.current_term,
                granted: false,
            };
        } else {
            // their log is at least as up to date as ours
            inner.last_request = Instant::now();
            return VoteResponse {
                term: 1,
                granted: true,
            };
        }
    }

    fn leader_main(&self) {
        let heartbeat_time = Duration::from_millis(75);
        let mut election_timeout = self.gen_new_election_time();
        info!("Leader thread started");
        loop {
            debug!("Leader loop start");
            let mode = self.state.lock().unwrap().mode;
            match mode {
                Mode::Follower => {
                    let last_req = self.state.lock().unwrap().last_request;
                    info!("Checking election_timeout");
                    if last_req.elapsed() > election_timeout {
                        info!("Election timeout hit");
                        let mut inner = self.state.lock().unwrap();
                        inner.mode = Mode::Candidate;
                    } else {
                        info!("Sleeping");
                        thread::sleep(election_timeout - last_req.elapsed());
                    }
                }
                Mode::Candidate => {
                    info!("Starting new election");
                    let mut inner = self.state.lock().unwrap();
                    inner.current_term += 1;
                    inner.voted_for = Some(inner.node_id);
                    election_timeout = self.gen_new_election_time();

                    // voted for myself so start at 1
                    let mut vote_count = 1;

                    let req = VoteRequest {
                        term: inner.current_term,
                        candidate_id: inner.node_id,
                        last_log_index: get_last_log_index(&inner.log),
                        last_log_term: get_last_log_term(&inner.log),
                    };

                    let peers = inner.peers.clone();

                    // request the votes!
                    for peer in peers.iter() {
                        let resp = match request_vote(&req, &peer.addr) {
                            Ok(resp) => resp,
                            Err(e) => {
                                error!("error while requesting vote: {}", e);
                                continue;
                            }
                        };
                        if resp.granted {
                            vote_count += 1;
                        }
                    }

                    if vote_count >= calc_quorum_size(inner.peers.len().try_into().unwrap()) {
                        info!("Election ended: Node is now Leader");
                        inner.mode = Mode::Leader;
                        inner.voted_for = None;
                        let last_log_index = get_last_log_index(&inner.log);
                        for p in inner.peers.iter_mut() {
                            p.next_index = last_log_index + 1;
                            p.match_index = 0;
                        }
                    } else {
                        info!("Election ended: Not enough votes");
                    }
                }
                Mode::Leader => {
                    info!("Leader round started");
                    let mut inner = self.state.lock().unwrap();

                    let start = Instant::now();
                    let last_log_index = get_last_log_index(&inner.log);

                    // ping peers
                    for peer in inner.peers.iter() {
                        let prev_log_index = if peer.next_index == 0 {
                            0
                        } else {
                            peer.next_index - 1
                        };
                        let prev_log_term = if prev_log_index <= 0 {
                            0
                        } else {
                            inner.log.get((prev_log_index - 1) as usize).unwrap().term
                        };

                        let req = AppendEntriesRequest {
                            term: inner.current_term,
                            leader_id: inner.node_id,
                            prev_log_index: prev_log_index,
                            prev_log_term: prev_log_term,
                            entries: vec![],
                            leader_commit: inner.commit_index,
                        };

                        append_entries(&req, &peer.addr);
                    }

                    let mut req = AppendEntriesRequest {
                        term: inner.current_term,
                        leader_id: inner.node_id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit: inner.commit_index,
                    };

                    let log = inner.log.clone();

                    // catch up peers
                    for peer in inner.peers.iter_mut() {
                        if last_log_index > peer.next_index {
                            continue;
                        }
                        // else, peer needs catchup or we should sync

                        loop {
                            let prev_log_index = if peer.next_index == 0 {
                                0
                            } else {
                                peer.next_index - 1
                            };
                            let prev_log_term = if prev_log_index <= 0 {
                                0
                            } else {
                                log.get((prev_log_index - 1) as usize).unwrap().term
                            };

                            req.prev_log_index = prev_log_index;
                            req.prev_log_term = prev_log_term;
                            req.entries = log[prev_log_index as usize..].to_vec();

                            let resp = match append_entries(&req, &peer.addr) {
                                Ok(resp) => resp,
                                Err(e) => {
                                    error!("error while updating a peer: {}", e);
                                    break;
                                }
                            };
                            if resp.success {
                                info!("Peer update successful");
                                peer.next_index = last_log_index;
                                peer.match_index = last_log_index;
                                break;
                            } else {
                                info!("Peer update not successful");
                                peer.next_index -= 1;
                            }
                        }
                    }

                    drop(inner);

                    let stime = heartbeat_time - start.elapsed();
                    info!("Leader thread sleeping for {}ms", stime.as_millis());
                    thread::sleep(stime);
                }
            }
        }
    }

    fn gen_new_election_time(&self) -> Duration {
        let election_timeout_min = 150;
        let play: u64 = rand::thread_rng().gen_range(0..150);

        Duration::from_millis(election_timeout_min + play)
    }

    fn state_sync_main(&self) {
        info!("Sync thread started.");
        loop {
            thread::sleep(std::time::Duration::from_millis(100));
            let mut inner = self.state.lock().unwrap();
            if inner.commit_index > inner.last_applied {
                // apply
                inner.last_applied += 1;
            }
        }
    }
}

fn calc_quorum_size(num_peers: u64) -> u64 {
    max((num_peers as f64 / 2.0).ceil() as u64, 1)
}

fn get_last_log_term(log: &[LogEntry]) -> u64 {
    if let Some(x) = log.last() {
        x.term
    } else {
        0
    }
}

fn get_last_log_index(log: &[LogEntry]) -> u64 {
    log.len().try_into().unwrap()
}
