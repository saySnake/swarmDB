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
#include <storage/storage_base.hpp>
#include <unordered_map>
#include <shared_mutex>


namespace bzn
{
    class mem_storage : public bzn::storage_base
    {
    public:

        storage_base::result create(const bzn::uuid_t& uuid, const std::string& key, const std::string& value) override;

        std::optional<bzn::value_t> read(const bzn::uuid_t& uuid, const std::string& key) override;

        storage_base::result update(const bzn::uuid_t& uuid, const std::string& key, const std::string& value) override;

        storage_base::result remove(const bzn::uuid_t& uuid, const std::string& key) override;

        std::vector<std::string> get_keys(const bzn::uuid_t& uuid) override;

        bool has(const bzn::uuid_t& uuid, const  std::string& key) override;

        std::pair<std::size_t, std::size_t> get_size(const bzn::uuid_t& uuid) override;

        storage_base::result remove(const bzn::uuid_t& uuid) override;

    private:
        std::unordered_map<bzn::uuid_t, std::unordered_map<bzn::key_t, bzn::value_t>> kv_store;

        std::shared_mutex lock; // for multi-reader and single writer access
    };

} // bzn
