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

#include <pbft/pbft_config_store.hpp>

using namespace bzn;

pbft_config_store::pbft_config_store()
    : current_index(0)
{
}

bool
pbft_config_store::add(pbft_configuration::shared_const_ptr config)
{
    return (this->configs.insert(std::make_pair(config->get_index(), std::make_pair(config, false)))).second;
}

bool
pbft_config_store::set_current(const hash_t& hash)
{
    auto config = this->get(hash);
    if (config)
    {
        this->current_index = config->get_index();
        return true;
    }

    return false;
}

bool
pbft_config_store::set_current(pbft_configuration::index_t index)
{
    if (this->configs.find(index) != this->configs.end())
    {
        this->current_index = index;
        return true;
    }

    return false;
}

bool
pbft_config_store::remove_prior_to(pbft_configuration::index_t index)
{
    auto it = this->configs.find(index);
    if (it == this->configs.end())
        return false;

    this->configs.erase(this->configs.begin(), it);
    return true;
}

pbft_config_store::config_map::const_iterator
pbft_config_store::find_by_hash(hash_t hash) const
{
    auto config = std::find_if(this->configs.begin(), this->configs.end(),
        [hash](auto c)
        {
            return c.second.first->get_hash() == hash;
        });

    return config;
}

pbft_configuration::shared_const_ptr
pbft_config_store::get(const hash_t& hash) const
{
    auto config = this->find_by_hash(hash);
    return config != this->configs.end() ? config->second.first : nullptr;
}

bool
pbft_config_store::enable(const hash_t& hash, bool val)
{
    // can't find_by_hash here because we need a non-const
    auto config = std::find_if(this->configs.begin(), this->configs.end(),
        [hash](auto c)
        {
            return c.second.first->get_hash() == hash;
        });

    if (config != this->configs.end())
    {
        config->second.second = val;
        return true;
    }

    return false;
}

bool
pbft_config_store::enable(pbft_configuration::index_t index, bool val)
{
    auto it = this->configs.find(index);
    if (it != this->configs.end())
    {
        it->second.second = val;
        return true;
    }

    return false;
}

bool
pbft_config_store::is_enabled(const hash_t& hash) const
{
    auto config = this->find_by_hash(hash);
    return config != this->configs.end() ? config->second.second : false;
}

bool
pbft_config_store::is_enabled(pbft_configuration::index_t index) const
{
    auto it = this->configs.find(index);
    if (it != this->configs.end())
    {
        return it->second.second;
    }

    return false;
}

pbft_configuration::shared_const_ptr
pbft_config_store::current() const
{
    auto it = this->configs.find(this->current_index);
    return it != this->configs.end() ? it->second.first : nullptr;
}
