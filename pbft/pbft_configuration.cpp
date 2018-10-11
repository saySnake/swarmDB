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

#include "pbft_configuration.hpp"

using namespace bzn;

pbft_configuration::pbft_configuration()
{
    this->sorted_peers = std::make_shared<std::vector<bzn::peer_address_t>>();
}

pbft_configuration
pbft_configuration::fork() const
{
    return pbft_configuration();
}

bool
pbft_configuration::from_json(const bzn::json_message& json)
{
    (void) json;
    return false;
}

bzn::json_message
pbft_configuration::to_json()
{
    return "";
}

index_t
pbft_configuration::get_index() const
{
    return 0;
}

hash_t
pbft_configuration::get_hash() const
{
    return "";
}

std::shared_ptr<const std::vector<bzn::peer_address_t>>
pbft_configuration::get_peers() const
{
    return this->sorted_peers;
}

bool
pbft_configuration::add_peer(const bzn::peer_address_t& peer)
{
    (void) peer;
    return false;
}

bool
pbft_configuration::remove_peer(const bzn::peer_address_t& peer)
{
    (void) peer;
    return false;
}

void
pbft_configuration::sort()
{

}

void
pbft_configuration::cache_sorted_peers()
{

}
