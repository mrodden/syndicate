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

use std::{env, thread, time};
use syndicate::client::RaftClient;

fn main() {
    tracing_subscriber::fmt::init();

    let servers: Vec<String> = ["127.0.0.1:3001", "127.0.0.1:3002", "127.0.0.1:3003"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rc = RaftClient::new(servers.clone());

    let mut zero = String::from("");
    let mut command: String = String::from("");
    let mut key: String = String::from("");
    let mut val: String = String::from("");

    for arg in env::args() {
        if zero.is_empty() {
            zero = arg;
            continue;
        }
        if command.is_empty() {
            command = arg;
            continue;
        }
        if key.is_empty() {
            key = arg;
            continue;
        }
        if val.is_empty() {
            val = arg;
            continue;
        }
    }

    match command.as_str() {
        "getlogs" => {
            for s in servers.iter() {
                println!("{:?}", rc.get_log(&s).unwrap());
            }
        }
        "get" => {
            rc.update_master_addr();
            if let Some(ret) = rc.get(&key).unwrap() {
                println!("{:#?}", ret);
            } else {
                println!("None");
            }
        }
        "set" => {
            rc.update_master_addr();
            if let Some(ret) = rc.set(&key, &val).unwrap() {
                println!("{:#?}", ret);
            } else {
                println!("None");
            }
        }
        e => {
            println!("unrecognized command: {}", e);
            println!("usage: client COMMAND [key] [val]");
        }
    }
}
