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

#include <crud/crud.hpp>

using namespace bzn;

namespace
{
    const std::string PERMISSION_UUID{"PERMS"};
}


crud::crud(std::shared_ptr<bzn::storage_base> storage, std::shared_ptr<bzn::subscription_manager_base> subscription_manager)
           : storage(std::move(storage))
           , subscription_manager(std::move(subscription_manager))
           , message_handlers{
                 {database_msg::kCreate, std::bind(&crud::handle_create, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kRead,   std::bind(&crud::handle_read,   this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kUpdate, std::bind(&crud::handle_update, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kDelete, std::bind(&crud::handle_delete, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kHas,    std::bind(&crud::handle_has,    this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kKeys,   std::bind(&crud::handle_keys,   this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kSize,   std::bind(&crud::handle_size,   this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kSubscribe,   std::bind(&crud::handle_subscribe,   this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kUnsubscribe, std::bind(&crud::handle_unsubscribe, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kCreateDb,    std::bind(&crud::handle_create_db,   this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kDeleteDb,    std::bind(&crud::handle_delete_db,   this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)},
                 {database_msg::kHasDb,       std::bind(&crud::handle_has_db,      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)}}
{
}


void
crud::start()
{
    std::call_once(this->start_once,
        [this]()
        {
            this->subscription_manager->start();
        });
}


void
crud::handle_request(const bzn::caller_id_t& caller_id, const database_msg& request, const std::shared_ptr<bzn::session_base>& session)
{
    if (auto it = this->message_handlers.find(request.msg_case()); it != this->message_handlers.end())
    {
        LOG(debug) << "processing message: " << uint32_t(request.msg_case());

        it->second(caller_id, request, session);

        return;
    }

    LOG(error) << "unknown request: " << uint32_t(request.msg_case());
}


void
crud::send_response(const database_msg& request, const bzn::storage_base::result result,
    database_response&& response, std::shared_ptr<bzn::session_base>& session)
{
    *response.mutable_header() = request.header();

    if (result != storage_base::result::ok)
    {
        if (result == storage_base::result::value_too_large)
        {
            response.mutable_error()->set_message(bzn::MSG_VALUE_SIZE_TOO_LARGE);
        }
        else if (result == storage_base::result::key_too_large)
        {
            response.mutable_error()->set_message(bzn::MSG_KEY_SIZE_TOO_LARGE);
        }
        else if (result == storage_base::result::exists)
        {
            response.mutable_error()->set_message(bzn::MSG_RECORD_EXISTS);
        }
        else if (result == storage_base::result::not_found)
        {
            response.mutable_error()->set_message(bzn::MSG_RECORD_NOT_FOUND);
        }
        else if (result == storage_base::result::db_not_found)
        {
            response.mutable_error()->set_message(bzn::MSG_DATABASE_NOT_FOUND);
        }
        else
        {
            LOG(error) << "unknown error code: " << uint32_t(result);
        }
    }

    session->send_message(std::make_shared<std::string>(response.SerializeAsString()), false);
}


void
crud::handle_create(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    storage_base::result result{storage_base::result::db_not_found};

    if (this->storage->has(PERMISSION_UUID, request.header().db_uuid()))
    {
        result = this->storage->create(request.header().db_uuid(), request.create().key(), request.create().value());
    }

    if (session)
    {
        this->send_response(request, result, database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. CREATE response not sent.";
}


void
crud::handle_read(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        auto result = this->storage->read(request.header().db_uuid(), request.read().key());

        database_response response;

        if (result)
        {
            response.mutable_read()->set_key(request.read().key());
            response.mutable_read()->set_value(*result);
        }

        this->send_response(request, (result) ? bzn::storage_base::result::ok : bzn::storage_base::result::not_found,
            std::move(response), session);

        return;
    }

    LOG(warning) << "session no longer available. READ not executed.";
}


void
crud::handle_update(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    auto result = this->storage->update(request.header().db_uuid(), request.update().key(), request.update().value());

    if (session)
    {
        this->send_response(request, result, database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. UPDATE response not sent.";
}


void
crud::handle_delete(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    auto result = this->storage->remove(request.header().db_uuid(), request.delete_().key());

    if (session)
    {
        this->send_response(request, result, database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. DELETE response not sent.";
}


void
crud::handle_has(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        const bool has = this->storage->has(request.header().db_uuid(), request.has().key());

        this->send_response(request, (has) ? storage_base::result::ok : storage_base::result::not_found,
            database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. HAS not executed.";
}


void
crud::handle_keys(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        const auto keys = this->storage->get_keys(request.header().db_uuid());

        database_response response;
        response.mutable_keys();

        for (const auto& key : keys)
        {
            response.mutable_keys()->add_keys(key);
        }

        this->send_response(request, storage_base::result::ok, std::move(response), session);

        return;
    }

    LOG(warning) << "session no longer available. KEYS not executed.";
}


void
crud::handle_size(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        const auto [keys, size] = this->storage->get_size(request.header().db_uuid());

        database_response response;

        response.mutable_size()->set_keys(keys);
        response.mutable_size()->set_bytes(size);

        this->send_response(request, storage_base::result::ok, std::move(response), session);

        return;
    }

    LOG(warning) << "session no longer available. SIZE not executed.";
}


void
crud::handle_subscribe(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        database_response response;

        this->subscription_manager->subscribe(request.header().db_uuid(), request.subscribe().key(),
            request.header().transaction_id(), response, session);

        this->send_response(request, storage_base::result::ok, std::move(response), session);

        return;
    }

    LOG(warning) << "session no longer available. SUBSCRIBE not executed.";
}


void
crud::handle_unsubscribe(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        database_response response;

        this->subscription_manager->unsubscribe(request.header().db_uuid(), request.unsubscribe().key(),
            request.unsubscribe().transaction_id(), response, session);

        this->send_response(request, storage_base::result::ok, std::move(response), session);

        return;
    }

    // subscription manager will cleanup stale sessions...
    LOG(warning) << "session no longer available. UNSUBSCRIBE not executed.";
}


void
crud::handle_create_db(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    storage_base::result result;

    if (this->storage->has(PERMISSION_UUID, request.header().db_uuid()))
    {
        result = storage_base::result::exists;
    }
    else
    {
        result = this->storage->create(PERMISSION_UUID, request.header().db_uuid(), "<todo>");
    }

    if (session)
    {
        this->send_response(request, result, database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. CREATE DB response not sent.";
}


void
crud::handle_delete_db(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    storage_base::result result;

    if (!this->storage->has(PERMISSION_UUID, request.header().db_uuid()))
    {
        result = storage_base::result::not_found;
    }
    else
    {
        result = this->storage->remove(PERMISSION_UUID, request.header().db_uuid());

        this->storage->remove(request.header().db_uuid());
    }

    if (session)
    {
        this->send_response(request, result, database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. DELETE DB response not sent.";
}


void
crud::handle_has_db(const bzn::caller_id_t& /*caller_id*/, const database_msg& request, std::shared_ptr<bzn::session_base> session)
{
    if (session)
    {
        const bool has_db = this->storage->has(PERMISSION_UUID, request.header().db_uuid());

        this->send_response(request, (has_db) ? storage_base::result::ok : storage_base::result::not_found,
            database_response(), session);

        return;
    }

    LOG(warning) << "session no longer available. HAS DB not executed.";
}
