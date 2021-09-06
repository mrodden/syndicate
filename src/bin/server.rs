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

use std::env;

use tracing_subscriber;

use syndicate::{Node, Peer};

fn main() {
    tracing_subscriber::fmt::init();

    let mut zero = String::from("");
    let mut idst = String::from("");
    let mut peers = vec![];
    for arg in env::args() {
        if zero.is_empty() {
            zero = arg;
            continue;
        }
        if idst.is_empty() {
            idst = arg;
            continue;
        }
        peers.push(arg);
    }

    let mut ps = vec![];

    for p in peers {
        ps.push(Peer::new(&p));
    }

    let id: u64 = idst.parse().expect("Unable to parse node_id from arg 1");

    let n1 = Node::new(id, ps);
    n1.run();
}
