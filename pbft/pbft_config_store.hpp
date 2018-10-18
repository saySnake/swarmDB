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

#include <pbft/pbft_configuration.hpp>
#include <map>

namespace bzn
{
    using hash_t = std::string;

    class pbft_config_store
    {
    public:
        pbft_config_store();

        bool add(pbft_configuration::shared_const_ptr config);
        bool remove_prior_to(pbft_configuration::index_t index);
        pbft_configuration::shared_const_ptr get(const hash_t& hash) const;

        bool set_current(const hash_t& hash);
        bool set_current(pbft_configuration::index_t index);
        pbft_configuration::shared_const_ptr current() const;

        bool enable(const hash_t& hash, bool val = true);
        bool is_enabled(const hash_t& hash) const;
        bool enable(pbft_configuration::index_t index, bool val = true);
        bool is_enabled(pbft_configuration::index_t index) const;

    private:
        using config_map = std::map<pbft_configuration::index_t, std::pair<pbft_configuration::shared_const_ptr, bool>>;
        config_map::const_iterator find_by_hash(hash_t hash) const;

        // a map from the config index to a pair of <config, is_config_enabled>
        config_map configs;
        pbft_configuration::index_t current_index;
    };
}

