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
#include <proto/bluzelle.pb.h>
#include <bootstrap/bootstrap_peers_base.hpp>
#include <cstdint>
#include <string>
#include <node/session_base.hpp>
#include <crypto/crypto_base.hpp>

namespace bzn
{
    // View, sequence
    using operation_key_t = std::tuple<uint64_t, uint64_t, bzn::hash_t>;

    // View, sequence
    using log_key_t = std::tuple<uint64_t, uint64_t>;

    enum class pbft_operation_state
    {
        prepare, commit, committed
    };

    class pbft_operation
    {
    public:

        pbft_operation(uint64_t view, uint64_t sequence, bzn::hash_t request_hash, std::shared_ptr<const std::vector<peer_address_t>> peers);

        void set_session(std::weak_ptr<bzn::session_base>);

        operation_key_t get_operation_key();
        pbft_operation_state get_state();

        void record_request(std::string request);

        void record_preprepare(const pbft_msg& preprepare, const wrapped_bzn_msg& encoded_preprepare);
        bool has_preprepare();

        void record_prepare(const pbft_msg& prepare, const wrapped_bzn_msg& encoded_prepare);
        bool is_prepared();

        void record_commit(const pbft_msg& commit, const wrapped_bzn_msg& encoded_commit);
        bool is_committed();

        void begin_commit_phase();
        void end_commit_phase();

        std::weak_ptr<bzn::session_base> session();

        bool has_request();
        std::string get_request();

        const uint64_t view;
        const uint64_t sequence;
        const std::string request_hash;

        std::string debug_string();

        size_t faulty_nodes_bound() const;

    private:
        void maybe_record_request(const pbft_msg& msg);

        const std::shared_ptr<const std::vector<peer_address_t>> peers;

        pbft_operation_state state = pbft_operation_state::prepare;

        bool preprepare_seen = false;
        std::set<bzn::uuid_t> prepares_seen;
        std::set<bzn::uuid_t> commits_seen;

        std::weak_ptr<bzn::session_base> listener_session;

        std::string request;

    };
}
