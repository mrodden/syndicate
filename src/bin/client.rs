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

use syndicate::{AppendEntriesRequest, LogEntry, Request};

fn main() {
    tracing_subscriber::fmt::init();

    let req = AppendEntriesRequest {
        term: 3,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [
            LogEntry::new(0, "stuff", "things"),
            LogEntry::new(0, "1other", "yep"),
            LogEntry::new(0, "1things", "yep"),
            LogEntry::new(2, "1things", "yep"),
            LogEntry::new(3, "overwritten", "because 'm the boss"),
        ]
        .into(),
        leader_commit: 4,
    };
    println!("{:?}", syndicate::append_entries(&req, "127.0.0.1:3001"));
}
