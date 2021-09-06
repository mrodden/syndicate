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

use tracing_subscriber;

use crate::rpc::*;
//use crate::rpc::{append_entries, call_rpc};
//use crate::rpc::{
//    AppendEntriesRequest, AppendEntriesResponse, GetLogRequest, GetLogResponse, LogEntry, Request,
//};
use crate::Result;

pub struct RaftClient {
    servers: Vec<String>,
    master: String,
}

impl RaftClient {
    pub fn new(servers: Vec<String>) -> Self {
        let master = servers[0].clone();
        RaftClient { servers, master }
    }

    pub fn update_master_addr(&mut self) -> Result<()> {
        let addr = self.get("leader_addr").unwrap();
        if let Some(addr) = addr {
            self.master = addr.clone();
            println!("Leader: {}", self.master);
        }
        Ok(())
    }

    pub fn set(&self, key: &str, val: &str) -> Result<Option<String>> {
        let addr = &self.master;
        println!("Connecting to {}", addr);

        let req = SetRequest {
            key: key.into(),
            val: val.into(),
        };
        let data = call_rpc(Request::Set(req), addr)?;
        let resp: SetResponse = rmp_serde::from_read_ref(&data)?;
        Ok(resp.old_val)
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let addr = &self.master;
        println!("Connecting to {}", addr);

        let req = GetRequest { key: key.into() };
        let data = call_rpc(Request::Get(req), addr)?;
        let resp: GetResponse = rmp_serde::from_read_ref(&data)?;
        Ok(resp.val)
    }

    pub fn get_log(&self, addr: &str) -> Result<Vec<LogEntry>> {
        let req = GetLogRequest {};
        let data = call_rpc(Request::GetLog(req), addr)?;
        let resp: GetLogResponse = rmp_serde::from_read_ref(&data).unwrap();
        Ok(resp.log)
    }

    pub fn append_entries(&self, addr: &str) -> AppendEntriesResponse {
        let req = AppendEntriesRequest {
            term: 4,
            leader_id: 20,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: [
                LogEntry::new(1, "stuff", "things"),
                LogEntry::new(1, "1other", "yep"),
                LogEntry::new(1, "1things", "yep"),
                LogEntry::new(2, "1things", "yep"),
                LogEntry::new(3, "overwritten", "because 'm the boss"),
                LogEntry::new(4, "overwritten", "because 'm the boss"),
            ]
            .into(),
            leader_commit: 4,
        };

        append_entries(&req, addr).unwrap()
    }
}
