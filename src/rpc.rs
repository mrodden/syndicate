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

use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::{NodeId, Result};

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    AppendEntries(AppendEntriesRequest),
    Vote(VoteRequest),
    GetLog(GetLogRequest),
    Get(GetRequest),
    Set(SetRequest),
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SetRequest {
    pub key: String,
    pub val: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SetResponse {
    pub old_val: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRequest {
    pub key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetResponse {
    pub val: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetLogRequest {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetLogResponse {
    pub log: Vec<LogEntry>,
}

pub(crate) fn call_rpc(req: Request, addr: &str) -> Result<Vec<u8>> {
    let buf = rmp_serde::to_vec(&req).unwrap();

    let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(50)));
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
    pub term: u64,
    pub key: String,
    pub val: String,
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
