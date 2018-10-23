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

#include "pbft_operation.hpp"
#include <boost/format.hpp>
#include <string>

using namespace bzn;

pbft_operation::pbft_operation(uint64_t view, uint64_t sequence, hash_t request_hash, std::shared_ptr<const std::vector<peer_address_t>> peers)
        : view(view)
        , sequence(sequence)
        , request_hash(request_hash)
        , peers(std::move(peers))
{
}

void
pbft_operation::record_preprepare(const pbft_msg& preprepare, const wrapped_bzn_msg& /*encoded_preprepare*/)
{
    this->maybe_record_request(preprepare);
    this->preprepare_seen = true;
}

bool
pbft_operation::has_preprepare()
{
    return this->preprepare_seen;
}

void
pbft_operation::record_prepare(const pbft_msg& prepare, const wrapped_bzn_msg& encoded_prepare)
{
    this->maybe_record_request(prepare);
    // TODO: Save message
    this->prepares_seen.insert(encoded_prepare.sender());
}

size_t
pbft_operation::faulty_nodes_bound() const
{
    return (this->peers->size() - 1) / 3;
}

bool
pbft_operation::is_prepared()
{
    return this->has_preprepare() && this->has_request() && this->prepares_seen.size() > 2 * this->faulty_nodes_bound();
}

void
pbft_operation::record_commit(const pbft_msg& commit, const wrapped_bzn_msg& encoded_commit)
{
    this->maybe_record_request(commit);
    this->commits_seen.insert(encoded_commit.sender());
}

bool
pbft_operation::is_committed()
{
    return this->is_prepared() && this->commits_seen.size() > 2 * this->faulty_nodes_bound();
}

void
pbft_operation::begin_commit_phase()
{
    if (!this->is_prepared() || this->state != pbft_operation_state::prepare)
    {
        throw std::runtime_error("Illegaly tried to move to commit phase");
    }

    this->state = pbft_operation_state::commit;
}

void
pbft_operation::end_commit_phase()
{
    if (!this->is_committed() || this->state != pbft_operation_state::commit)
    {
        throw std::runtime_error("Illegally tried to end the commit phase");
    }

    this->state = pbft_operation_state::committed;
}

operation_key_t
pbft_operation::get_operation_key()
{
    return std::tuple<uint64_t, uint64_t, hash_t>(this->view, this->sequence, this->request_hash);
}

pbft_operation_state
pbft_operation::get_state()
{
    return this->state;
}

std::string
pbft_operation::debug_string()
{
    return boost::str(boost::format("(v%1%, s%2%) - %3%[%4%]") % this->view % this->sequence % this->request % this->request_hash);
}

void
pbft_operation::set_session(std::weak_ptr<bzn::session_base> session)
{
    this->listener_session = std::move(session);
}

std::weak_ptr<bzn::session_base>
pbft_operation::session()
{
    return this->listener_session;
}

bool
pbft_operation::has_request()
{
    return !this->request.empty();
}

std::string
pbft_operation::get_request()
{
    if (!this->has_request())
    {
        throw new std::runtime_error("pbft_operation does not have the request");
    }

    return this->request;
}

void
pbft_operation::maybe_record_request(const pbft_msg& msg)
{
    // We are assuming that the caller has checked to make sure that the request and its hash match
    if (this->request.empty() && !msg.request().empty())
    {
        this->request = msg.request();
    }
}

void
pbft_operation::record_request(std::string request)
{
    this->request = request;
}
