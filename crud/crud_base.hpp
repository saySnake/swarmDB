// Copyright (C) 2018 Bluzelle
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#pragma once

#include <include/bluzelle.hpp>
#include <node/session_base.hpp>
#include <proto/bluzelle.pb.h>


namespace bzn
{
    const std::string MSG_INVALID_RAFT_STATE = "INVALID_RAFT_STATE";
    const std::string MSG_INVALID_CRUD_COMMAND = "INVALID_CRUD";
    const std::string MSG_ELECTION_IN_PROGRESS = "ELECTION_IN_PROGRESS";
    const std::string MSG_RECORD_EXISTS = "RECORD_EXISTS";
    const std::string MSG_RECORD_NOT_FOUND = "RECORD_NOT_FOUND";
    const std::string MSG_DATABASE_NOT_FOUND = "DATABASE_NOT_FOUND";
    const std::string MSG_INVALID_ARGUMENTS = "INVALID_ARGUMENTS";
    const std::string MSG_VALUE_SIZE_TOO_LARGE = "VALUE_SIZE_TOO_LARGE";
    const std::string MSG_KEY_SIZE_TOO_LARGE = "KEY_SIZE_TOO_LARGE";

    // this interface is tied too closely with raft usage and will eventually be removed...
    namespace deprecated
    {
        class crud_base
        {
        public:
            virtual ~crud_base() = default;

            virtual void handle_create(const bzn::json_message& msg, const database_msg& request, database_response& response) = 0;

            virtual void handle_read(const bzn::json_message& msg, const database_msg& request, database_response& response) = 0;

            virtual void handle_update(const bzn::json_message& msg, const database_msg& request, database_response& response) = 0;

            virtual void handle_delete(const bzn::json_message& msg, const database_msg& request, database_response& response) = 0;

            virtual void start() = 0;
        };

    } // namespace deprecated

    class crud_base
    {
    public:
        virtual ~crud_base() = default;

        virtual void handle_request(const bzn::caller_id_t& caller_id, const database_msg& request, const std::shared_ptr<bzn::session_base>& session) = 0;

        virtual void start() = 0;
    };

} // namespace bzn
