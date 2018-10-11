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
#include <pbft/pbft_base.hpp>
#include <cstdint>
#include <string>
#include <vector>

namespace bzn
{
    using hash_t = std::string;
    using index_t = uint64_t;

    class pbft_configuration
    {
    public:

        pbft_configuration();

        pbft_configuration fork() const;

        bool from_json(const bzn::json_message& json);
        bzn::json_message to_json();

        index_t get_index() const;
        hash_t get_hash() const;
        std::shared_ptr<const std::vector<bzn::peer_address_t>> get_peers() const;

        bool add_peer(const bzn::peer_address_t& peer);
        bool remove_peer(const bzn::peer_address_t& peer);

    private:
        void sort();
        void cache_sorted_peers();

        static index_t next_index;
        index_t index;
        std::vector<std::pair<uint64_t, bzn::peer_address_t>> peers;
        std::shared_ptr<std::vector<bzn::peer_address_t>> sorted_peers;
    };
}

