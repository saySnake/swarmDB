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

#include <pbft/pbft.hpp>
#include <utils/make_endpoint.hpp>
#include <google/protobuf/text_format.h>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/format.hpp>
#include <cstdint>
#include <memory>
#include <algorithm>
#include <numeric>
#include <iterator>
#include <crud/crud_base.hpp>
#include <random>
#include <chrono>

using namespace bzn;

pbft::pbft(
    std::shared_ptr<bzn::node_base> node
    , std::shared_ptr<bzn::asio::io_context_base> io_context
    , const bzn::peers_list_t& peers
    , bzn::uuid_t uuid
    , std::shared_ptr<pbft_service_base> service
    , std::shared_ptr<pbft_failure_detector_base> failure_detector
    , std::shared_ptr<bzn::crypto_base> crypto)
    : node(std::move(node))
    , uuid(std::move(uuid))
    , service(std::move(service))
    , failure_detector(std::move(failure_detector))
    , io_context(io_context)
    , audit_heartbeat_timer(this->io_context->make_unique_steady_timer()), crypto(std::move(crypto))
{
    if (peers.empty())
    {
        throw std::runtime_error("No peers found!");
    }

    this->initialize_configuration(peers);

    // TODO: stable checkpoint should be read from disk first: KEP-494
    this->low_water_mark = this->stable_checkpoint.first;
    this->high_water_mark = this->stable_checkpoint.first + std::lround(CHECKPOINT_INTERVAL*HIGH_WATER_INTERVAL_IN_CHECKPOINTS);
}

void
pbft::start()
{
    std::call_once(this->start_once,
            [this]()
            {
                this->node->register_for_message(bzn_envelope::PayloadCase::kPbft,
                        std::bind(&pbft::handle_bzn_message, shared_from_this(), std::placeholders::_1, std::placeholders::_2));

                this->node->register_for_message(bzn_envelope::PayloadCase::kPbftMembership,
                    std::bind(&pbft::handle_membership_message, shared_from_this(), std::placeholders::_1, std::placeholders::_2));

                this->node->register_for_message("database",
                        std::bind(&pbft::handle_database_message, shared_from_this(), std::placeholders::_1, std::placeholders::_2));

                this->audit_heartbeat_timer->expires_from_now(HEARTBEAT_INTERVAL);
                this->audit_heartbeat_timer->async_wait(
                        std::bind(&pbft::handle_audit_heartbeat_timeout, shared_from_this(), std::placeholders::_1));

                this->service->register_execute_handler(
                        [weak_this = this->weak_from_this(), fd = this->failure_detector]
                                (std::shared_ptr<pbft_operation> op)
                                        {
                                            if (!op)
                                            {
                                                // TODO: Get real pbft_operation pointers from pbft_service
                                                LOG(error) << "Ignoring null operation pointer recieved from pbft_service";
                                            }

                                            fd->request_executed(op->request_hash);

                                            if (op->sequence % CHECKPOINT_INTERVAL == 0)
                                            {
                                                auto strong_this = weak_this.lock();
                                                if(strong_this)
                                                {
                                                    strong_this->checkpoint_reached_locally(op->sequence);
                                                }
                                                else
                                                {
                                                    throw std::runtime_error("pbft_service callback failed because pbft does not exist");
                                                }
                                            }
                                        }
                );

                this->failure_detector->register_failure_handler(
                        [weak_this = this->weak_from_this()]()
                        {
                            auto strong_this = weak_this.lock();
                            if (strong_this)
                            {
                                strong_this->handle_failure();
                            }
                        }
                        );
            });

}

void
pbft::handle_audit_heartbeat_timeout(const boost::system::error_code& ec)
{
    if (ec)
    {
        LOG(error) << "pbft audit heartbeat canceled? " << ec.message();
        return;
    }

    if (this->is_primary() && this->audit_enabled)
    {
        audit_message msg;
        msg.mutable_primary_status()->set_view(this->view);
        msg.mutable_primary_status()->set_primary(this->uuid);

        this->broadcast(this->wrap_message(msg));

    }

    this->audit_heartbeat_timer->expires_from_now(HEARTBEAT_INTERVAL);
    this->audit_heartbeat_timer->async_wait(std::bind(&pbft::handle_audit_heartbeat_timeout, shared_from_this(), std::placeholders::_1));
}

void
pbft::handle_bzn_message(const bzn_envelope& msg, std::shared_ptr<bzn::session_base> /*session*/)
{
    if (msg.payload_case() != bzn_envelope::kPbft )
    {
        LOG(error) << "Got misdirected message " << msg.DebugString().substr(0, MAX_MESSAGE_SIZE);
    }

    pbft_msg inner_msg;
    if (!inner_msg.ParseFromString(msg.pbft()))
    {
        LOG(error) << "Failed to parse payload of wrapped message " << msg.DebugString().substr(0, MAX_MESSAGE_SIZE);
        return;
    }

    this->handle_message(inner_msg, msg);
}

void
pbft::handle_membership_message(const bzn_envelope& msg, std::shared_ptr<bzn::session_base> session)
{
    pbft_membership_msg inner_msg;
    if (!inner_msg.ParseFromString(msg.pbft_membership()))
    {
        LOG(error) << "Failed to parse payload of wrapped message " << msg.DebugString().substr(0, MAX_MESSAGE_SIZE);
        return;
    }

    switch (inner_msg.type())
    {
        case PBFT_MMSG_JOIN:
        case PBFT_MMSG_LEAVE:
            this->handle_join_or_leave(inner_msg);
            break;
        case PBFT_MMSG_GET_STATE:
            this->handle_get_state(inner_msg, std::move(session));
            break;
        case PBFT_MMSG_SET_STATE:
            this->handle_set_state(inner_msg);
            break;
        default:
            LOG(error) << "Invalid membership message received "
                << inner_msg.DebugString().substr(0, MAX_MESSAGE_SIZE);
    }
}

void
pbft::handle_message(const pbft_msg& msg, const bzn_envelope& original_msg)
{

    LOG(debug) << "Received message: " << msg.ShortDebugString().substr(0, MAX_MESSAGE_SIZE);

    if (!this->preliminary_filter_msg(msg))
    {
        return;
    }

    std::lock_guard<std::mutex> lock(this->pbft_lock);

    switch (msg.type())
    {
        case PBFT_MSG_PREPREPARE :
            this->handle_preprepare(msg, original_msg);
            break;
        case PBFT_MSG_PREPARE :
            this->handle_prepare(msg, original_msg);
            break;
        case PBFT_MSG_COMMIT :
            this->handle_commit(msg, original_msg);
            break;
        case PBFT_MSG_CHECKPOINT :
            this->handle_checkpoint(msg, original_msg);
            break;
        case PBFT_MSG_VIEWCHANGE :
            this->handle_viewchange(msg, original_msg);
            break;
        case PBFT_MSG_NEWVIEW :
            this->handle_newview(msg, original_msg);
            break;

        default :
            throw std::runtime_error("Unsupported message type");
    }
}

bool
pbft::preliminary_filter_msg(const pbft_msg& msg)
{
    if (!this->is_view_valid()  && ( msg.type() != PBFT_MSG_CHECKPOINT || msg.type() != PBFT_MSG_VIEWCHANGE || msg.type() != PBFT_MSG_NEWVIEW) )
    {
        LOG(debug) << "Dropping message because local view is invalid";
        return false;
    }

    auto t = msg.type();
    if (t == PBFT_MSG_PREPREPARE || t == PBFT_MSG_PREPARE || t == PBFT_MSG_COMMIT)
    {
        if (msg.view() != this->view)
        {
            LOG(debug) << "Dropping message because it has the wrong view number";
            return false;
        }

        if (msg.sequence() <= this->low_water_mark)
        {
            LOG(debug) << "Dropping message becasue it has an unreasonable sequence number " << msg.sequence();
            return false;
        }

        if (msg.sequence() > this->high_water_mark)
        {
            LOG(debug) << "Dropping message becasue it has an unreasonable sequence number " << msg.sequence();
            return false;
        }
    }

    return true;
}

std::shared_ptr<pbft_operation>
pbft::setup_request_operation(const bzn::encoded_message& request, const request_hash_t& hash,
    const std::shared_ptr<session_base>& session)
{
    const uint64_t request_seq = this->next_issued_sequence_number++;
    auto op = this->find_operation(this->view, request_seq, hash);
    op->record_request(request);

    if (session)
    {
        op->set_session(session);
    }

    return op;
}

void
pbft::handle_request(const pbft_request& msg, const bzn::json_message& original_msg, const std::shared_ptr<session_base>& session)
{
    if (!this->is_primary())
    {
        LOG(info) << "Forwarding request to primary: " << original_msg.toStyledString();
        this->node->send_message(bzn::make_endpoint(this->get_primary()), std::make_shared<bzn::json_message>(original_msg));
        return;
    }

    if (msg.timestamp() < (this->now() - MAX_REQUEST_AGE_MS) || msg.timestamp() > (this->now() + MAX_REQUEST_AGE_MS))
    {
        // TODO: send error message to client
        LOG(info) << "Rejecting request because it is outside allowable timestamp range: "
            << original_msg.toStyledString();
        return;
    }

    auto smsg = original_msg.toStyledString();
    auto hash = this->crypto->hash(smsg);

    // keep track of what requests we've seen based on timestamp and only send preprepares once
    if (this->already_seen_request(msg, hash))
    {
        // TODO: send error message to client
        LOG(info) << "Rejecting duplicate request: " << original_msg.toStyledString();
        return;
    }
    this->saw_request(msg, hash);

    auto op = setup_request_operation(smsg, hash, session);
    this->do_preprepare(op);
}

void
pbft::maybe_record_request(const pbft_msg& msg, const std::shared_ptr<pbft_operation>& op)
{
    if (!msg.request().empty() && !op->has_request())
    {
        if (this->crypto->hash(msg.request()) != msg.request_hash())
        {
            LOG(info) << "Not recording request because its hash does not match";
            return;
        }

        op->record_request(msg.request());
    }
}

void
pbft::handle_preprepare(const pbft_msg& msg, const bzn_envelope& original_msg)
{
    // If we've already accepted a preprepare for this view+sequence, and it's not this one, then we should reject this one
    // Note that if we get the same preprepare more than once, we can still accept it
    const log_key_t log_key(msg.view(), msg.sequence());

    if (auto lookup = this->accepted_preprepares.find(log_key);
        lookup != this->accepted_preprepares.end()
        && std::get<2>(lookup->second) != msg.request_hash())
    {

        LOG(debug) << "Rejecting preprepare because I've already accepted a conflicting one \n";
        return;
    }
    else
    {
        auto op = this->find_operation(msg);
        op->record_preprepare(original_msg);
        this->maybe_record_request(msg, op);

        // This assignment will be redundant if we've seen this preprepare before, but that's fine
        accepted_preprepares[log_key] = op->get_operation_key();

        if (op->has_request() && op->get_request().type() == PBFT_REQ_NEW_CONFIG)
        {
            this->handle_config_message(msg, op);
        }

        this->do_preprepared(op);
        this->maybe_advance_operation_state(op);
    }
}

void
pbft::handle_prepare(const pbft_msg& msg, const bzn_envelope& original_msg)
{
    // Prepare messages are never rejected, assuming the sanity checks passed
    auto op = this->find_operation(msg);

    op->record_prepare(original_msg);
    this->maybe_record_request(msg, op);
    this->maybe_advance_operation_state(op);
}

void
pbft::handle_commit(const pbft_msg& msg, const bzn_envelope& original_msg)
{
    // Commit messages are never rejected, assuming  the sanity checks passed
    auto op = this->find_operation(msg);

    op->record_commit(original_msg);
    this->maybe_record_request(msg, op);
    this->maybe_advance_operation_state(op);
}

void
pbft::handle_join_or_leave(const pbft_membership_msg& msg)
{
    if (!this->is_primary())
    {
        LOG(error) << "Ignoring client request because I am not the primary";
        // TODO - KEP-327
        return;
    }

    if (msg.has_peer_info())
    {
        // build a peer_address_t from the message
        auto const &peer_info = msg.peer_info();
        bzn::peer_address_t peer(peer_info.host(), static_cast<uint16_t>(peer_info.port()),
            static_cast<uint16_t>(peer_info.http_port()), peer_info.name(), peer_info.uuid());

        auto config = std::make_shared<pbft_configuration>(*(this->configurations.current()));
        if (msg.type() == PBFT_MMSG_JOIN)
        {
            // see if we can add this peer
            if (!config->add_peer(peer))
            {
                // TODO - respond with negative result?
                LOG(debug) << "Can't add new peer due to conflict";
                return;
            }
        }
        else if (msg.type() == PBFT_MMSG_LEAVE)
        {
            if (!config->remove_peer(peer))
            {
                // TODO - respond with negative result?
                LOG(debug) << "Couldn't remove requested peer";
                return;
            }
        }

        this->configurations.add(config);
        this->broadcast_new_configuration(config);
    }
    else
    {
        LOG(debug) << "Malformed join/leave message";
    }
}

void
pbft::handle_get_state(const pbft_membership_msg& msg, std::shared_ptr<bzn::session_base> session) const
{
    // get stable checkpoint for request
    checkpoint_t req_cp(msg.sequence(), msg.state_hash());

    if (req_cp == this->latest_stable_checkpoint())
    {
        pbft_membership_msg reply;
        reply.set_type(PBFT_MMSG_SET_STATE);
        reply.set_sequence(req_cp.first);
        reply.set_state_hash(req_cp.second);
        reply.set_state_data(this->get_checkpoint_state(req_cp));

        auto msg_ptr = std::make_shared<bzn::encoded_message>(this->wrap_message(reply));
        session->send_datagram(msg_ptr);
    }
    else
    {
        LOG(debug) << boost::format("Request for checkpoint that I don't have: seq: %1%, hash: %2%")
            % msg.sequence(), msg.state_hash();
    }
}

void
pbft::handle_set_state(const pbft_membership_msg& msg)
{
    checkpoint_t cp(msg.sequence(), msg.state_hash());

    // do we need this checkpoint state?
    // make sure we don't have this checkpoint locally, but do know of a stablized one
    // based on the messages sent by peers.
    if (this->unstable_checkpoint_proofs[cp].size() >= this->quorum_size() &&
        this->local_unstable_checkpoints.count(cp) == 0)
    {
        LOG(info) << boost::format("Adopting checkpoint %1% at seq %2%")
            % cp.second % cp.first;

        this->set_checkpoint_state(cp, msg.state_data());
        this->stabilize_checkpoint(cp);
    }
    else
    {
        LOG(debug) << boost::format("Sent state for checkpoint that I don't need: seq: %1%, hash: %2%")
            % msg.sequence() % msg.state_hash();
    }
}

void
pbft::broadcast(const bzn::encoded_message& msg)
{
    auto msg_ptr = std::make_shared<bzn::encoded_message>(msg);

    for (const auto& peer : this->current_peers())
    {
        this->node->send_message_str(make_endpoint(peer), msg_ptr);
    }
}

void
pbft::maybe_advance_operation_state(const std::shared_ptr<pbft_operation>& op)
{
    if (op->get_state() == pbft_operation_state::prepare && op->is_prepared())
    {
        this->do_prepared(op);
    }

    if (op->get_state() == pbft_operation_state::commit && op->is_committed())
    {
        this->do_committed(op);
    }
}

pbft_msg
pbft::common_message_setup(const std::shared_ptr<pbft_operation>& op, pbft_msg_type type)
{
    pbft_msg msg;
    msg.set_view(op->view);
    msg.set_sequence(op->sequence);
    msg.set_request_hash(op->request_hash);
    msg.set_type(type);

    return msg;
}

void
pbft::do_preprepare(const std::shared_ptr<pbft_operation>& op)
{
    LOG(debug) << "Doing preprepare for operation " << op->debug_string();

    pbft_msg msg = this->common_message_setup(op, PBFT_MSG_PREPREPARE);
    msg.set_request(op->get_encoded_request());

    this->broadcast(this->wrap_message(msg, "preprepare"));
}

void
pbft::do_preprepared(const std::shared_ptr<pbft_operation>& op)
{
    LOG(debug) << "Entering prepare phase for operation " << op->debug_string();

    pbft_msg msg = this->common_message_setup(op, PBFT_MSG_PREPARE);

    this->broadcast(this->wrap_message(msg, "prepare"));
}

void
pbft::do_prepared(const std::shared_ptr<pbft_operation>& op)
{
    // accept new configuration if applicable
    if (op->has_request() && op->get_request().type() == PBFT_REQ_NEW_CONFIG && op->get_request().has_config())
    {
        pbft_configuration config;
        if (config.from_string(op->get_request().config().configuration()))
        {
            this->configurations.enable(config.get_hash());
        }
    }

    LOG(debug) << "Entering commit phase for operation " << op->debug_string();
    op->begin_commit_phase();

    pbft_msg msg = this->common_message_setup(op, PBFT_MSG_COMMIT);

    this->broadcast(this->wrap_message(msg, "commit"));
}

void
pbft::do_committed(const std::shared_ptr<pbft_operation>& op)
{
    // commit new configuration if applicable
    if (op->has_request() && op->get_request().type() == PBFT_REQ_NEW_CONFIG && op->get_request().has_config())
    {
        pbft_configuration config;
        if (config.from_string(op->get_request().config().configuration()))
        {
            // get rid of all other previous configs, except for currently active one
            this->configurations.remove_prior_to(config.get_hash());
        }
    }

    LOG(debug) << "Operation " << op->debug_string() << " is committed-local";
    op->end_commit_phase();

    if (this->audit_enabled)
    {
        audit_message msg;
        msg.mutable_pbft_commit()->set_operation(op->request_hash);
        msg.mutable_pbft_commit()->set_sequence_number(op->sequence);
        msg.mutable_pbft_commit()->set_sender_uuid(this->uuid);

        this->broadcast(this->wrap_message(msg));
    }

    // TODO: this needs to be refactored to be service-agnostic
    if (op->get_request().type() == PBFT_REQ_DATABASE)
    {
        this->io_context->post(std::bind(&pbft_service_base::apply_operation, this->service, this->find_operation(op)));
    }
    else
    {
        // the service needs sequentially sequenced operations. post a null request to fill in this hole
        auto msg = new database_msg;
        msg->set_allocated_nullmsg(new database_nullmsg);
        pbft_request request;
        request.set_allocated_operation(msg);
        auto smsg = request.SerializeAsString();
        auto new_op = std::make_shared<pbft_operation>(op->view, op->sequence
            , this->crypto->hash(smsg), nullptr);
        new_op->record_request(smsg);
        this->io_context->post(std::bind(&pbft_service_base::apply_operation, this->service, new_op));
            request.SerializeAsString();
    }
}

size_t
pbft::outstanding_operations_count() const
{
    return operations.size();
}

bool
pbft::is_primary() const
{
    return this->get_primary().uuid == this->uuid;
}

const peer_address_t&
pbft::get_primary() const
{
    return this->current_peers()[this->view % this->current_peers().size()];
}

// Find this node's record of an operation (creating a new record for it if this is the first time we've heard of it)
std::shared_ptr<pbft_operation>
pbft::find_operation(const pbft_msg& msg)
{
    return this->find_operation(msg.view(), msg.sequence(), msg.request_hash());
}

std::shared_ptr<pbft_operation>
pbft::find_operation(const std::shared_ptr<pbft_operation>& op)
{
    return this->find_operation(op->view, op->sequence, op->request_hash);
}

std::shared_ptr<pbft_operation>
pbft::find_operation(uint64_t view, uint64_t sequence, const bzn::hash_t& req_hash)
{
    auto key = bzn::operation_key_t(view, sequence, req_hash);

    auto lookup = operations.find(key);
    if (lookup == operations.end())
    {
        LOG(debug) << "Creating operation for seq " << sequence << " view " << view << " req " << req_hash;

        std::shared_ptr<pbft_operation> op = std::make_shared<pbft_operation>(view, sequence, req_hash,
                this->current_peers_ptr());
        auto result = operations.emplace(std::piecewise_construct, std::forward_as_tuple(std::move(key)), std::forward_as_tuple(op));

        assert(result.second);
        return result.first->second;
    }

    return lookup->second;
}

bzn::encoded_message
pbft::wrap_message(const pbft_msg& msg, const std::string& /*debug_info*/)
{
    bzn_envelope result;
    result.set_pbft(msg.SerializeAsString());
    result.set_sender(this->uuid);

    return result.SerializeAsString();
}

bzn::encoded_message
pbft::wrap_message(const pbft_membership_msg& msg, const std::string& /*debug_info*/) const
{
    bzn_envelope result;
    result.set_pbft_membership(msg.SerializeAsString());
    result.set_sender(this->uuid);

    return result.SerializeAsString();
}

bzn::encoded_message
pbft::wrap_message(const pbft_membership_msg& msg, const std::string& /*debug_info*/)
{
    wrapped_bzn_msg result;
    result.set_payload(msg.SerializeAsString());
    result.set_type(bzn_msg_type::BZN_MSG_PBFT_MEMBERSHIP);
    result.set_sender(this->uuid);

    return result.SerializeAsString();
}

bzn::encoded_message
pbft::wrap_message(const audit_message& msg, const std::string& debug_info)
{
    bzn::json_message json;

    json["bzn-api"] = "audit";
    json["audit-data"] = boost::beast::detail::base64_encode(msg.SerializeAsString());
    if (debug_info.length() > 0)
    {
        json["debug-info"] = debug_info;
    }

    return json.toStyledString();
}

const bzn::uuid_t&
pbft::get_uuid() const
{
    return this->uuid;
}

void
pbft::set_audit_enabled(bool setting)
{
    this->audit_enabled = setting;
}

void
pbft::notify_audit_failure_detected()
{
    if (this->audit_enabled)
    {
        audit_message msg;
        msg.mutable_failure_detected()->set_sender_uuid(this->uuid);
        this->broadcast(this->wrap_message(msg));
    }
}

void
pbft::handle_failure()
{
    LOG(fatal) << "Failure detected; view changes not yet implemented\n";
    this->notify_audit_failure_detected();
    //TODO: KEP-332
    this->view_is_valid = false;

    // at this point the timer has expired (i expires in view v)
    // doesn't matter: we must be a backup !this->is_primary()
    // Create  view-change message.
    pbft_msg view_change;

    // <VIEW-CHANGE v+1, n, C, P, i>_sigma_i
    view_change.set_type(PBFT_MSG_VIEWCHANGE);

    // v + 1 = this->view + 1
    view_change.set_view(this->view + 1);

    // n = sequence # of last valid checkpoint
    //   = this->stable_checkpoint.first
    view_change.set_sequence(this->latest_stable_checkpoint().first);

    // C = a set of local 2*f + 1 valid checkpoint messages
    //   = ?? I can get: **** this->stable_checkpoint_proof is a set of 2*f+1 map of uuid's to strings <- is this correct?
    for (const auto& msg : this->stable_checkpoint_proof)
    {
        view_change.add_checkpoint_messages(msg.second);
    }

    // P = a set (of client requests) containing a set P_m  for each request m that prepared at i with a sequence # higher
    //     than n
    // P_m = the pre prepare and the 2 f + 1 prepares
    //            get the set of operations, frome each operation get the messages..

    // std::set<std::shared_ptr<bzn::pbft_operation>>
    const auto operations = this->prepared_operations_since_last_checkpoint();

    for(const auto operation : operations)
    {
        prepared_proof* preprep_msg = view_change.add_prepared_proofs();
        preprep_msg->set_pre_prepare(operation->get_preprepare());
        for (const auto &prepared : operation->get_prepares())
        {
            preprep_msg->add_prepare(prepared);
        }
    }

    // std::set<std::shared_ptr<bzn::pbft_operation>> prepared_operations_since_last_checkpoint()
    this->broadcast(this->wrap_message(view_change));
}

void
pbft::checkpoint_reached_locally(uint64_t sequence)
{

    std::lock_guard<std::mutex> lock(this->pbft_lock);

    LOG(info) << "Reached checkpoint " << sequence;

    auto cp = this->local_unstable_checkpoints.emplace(sequence, this->service->service_state_hash(sequence)).first;

    pbft_msg cp_msg;
    cp_msg.set_type(PBFT_MSG_CHECKPOINT);
    cp_msg.set_view(this->view);
    cp_msg.set_sequence(sequence);
    cp_msg.set_state_hash(cp->second);

    this->broadcast(this->wrap_message(cp_msg));

    this->maybe_stabilize_checkpoint(*cp);
}

void
pbft::handle_checkpoint(const pbft_msg& msg, const bzn_envelope& original_msg)
{
    if (msg.sequence() <= stable_checkpoint.first)
    {
        LOG(debug) << boost::format("Ignoring checkpoint message for seq %1% because I already have a stable checkpoint at seq %2%")
                   % msg.sequence()
                   % stable_checkpoint.first;
        return;
    }

    LOG(info) << boost::format("Received checkpoint message for seq %1% from %2%")
              % msg.sequence()
              % original_msg.sender();

    checkpoint_t cp(msg.sequence(), msg.state_hash());

    this->unstable_checkpoint_proofs[cp][original_msg.sender()] = original_msg.SerializeAsString();
    if (msg.sequence() > this->first_sequence_to_execute)
    {
        this->maybe_stabilize_checkpoint(cp);
    }
    else
    {
        this->maybe_adopt_checkpoint(cp);
    }
}

bzn::checkpoint_t
pbft::latest_stable_checkpoint() const
{
    return this->stable_checkpoint;
}

bzn::checkpoint_t
pbft::latest_checkpoint() const
{
    return this->local_unstable_checkpoints.empty() ? this->stable_checkpoint : *(this->local_unstable_checkpoints.rbegin());
}

size_t
pbft::unstable_checkpoints_count() const
{
    return this->local_unstable_checkpoints.size();
}

void
pbft::maybe_stabilize_checkpoint(const checkpoint_t& cp)
{
    if (this->unstable_checkpoint_proofs[cp].size() < this->quorum_size())
    {
        return;
    }

    if (this->local_unstable_checkpoints.count(cp) != 0)
    {
        this->stabilize_checkpoint(cp);
    }
    else
    {
        // we don't have this checkpoint, so we need to catch up
        this->request_checkpoint_state(cp);
    }
}

void
pbft::stabilize_checkpoint(const checkpoint_t& cp)
{
    this->stable_checkpoint = cp;
    this->stable_checkpoint_proof = this->unstable_checkpoint_proofs[cp];

    LOG(info) << boost::format("Checkpoint %1% at seq %2% is now stable; clearing old data")
        % cp.second % cp.first;

    this->clear_local_checkpoints_until(cp);
    this->clear_checkpoint_messages_until(cp);
    this->clear_operations_until(cp);

    this->low_water_mark = std::max(this->low_water_mark, cp.first);
    this->high_water_mark = std::max(this->high_water_mark,
        cp.first + std::lround(HIGH_WATER_INTERVAL_IN_CHECKPOINTS * CHECKPOINT_INTERVAL));

    this->service->consolidate_log(cp.first);

    // remove seen requests older than our time threshold
    this->recent_requests.erase(this->recent_requests.begin(),
        this->recent_requests.upper_bound(this->now() - MAX_REQUEST_AGE_MS));
}

void
pbft::request_checkpoint_state(const checkpoint_t& cp)
{
    pbft_membership_msg msg;
    msg.set_type(PBFT_MMSG_GET_STATE);
    msg.set_sequence(cp.first);
    msg.set_state_hash(cp.second);

    auto selected = this->select_peer_for_checkpoint(cp);
    LOG(info) << boost::format("Requesting checkpoint state for hash %1% at seq %2% from %3%")
        % cp.second % cp.first % selected.uuid;

    auto msg_ptr = std::make_shared<bzn::encoded_message>(this->wrap_message(msg));
    this->node->send_message_str(make_endpoint(selected), msg_ptr);
}

const peer_address_t&
pbft::select_peer_for_checkpoint(const checkpoint_t& cp)
{
    // choose one of the peers who vouch for this checkpoint at random
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dist(0, this->unstable_checkpoint_proofs[cp].size() - 1);

    auto it = this->unstable_checkpoint_proofs[cp].begin();
    uint32_t selected = dist(gen);
    for (size_t i = 0; i < selected; i++)
    {
        it++;
    }

    return this->get_peer_by_uuid(it->first);
}

std::string
pbft::get_checkpoint_state(const checkpoint_t& cp) const
{
    // call service to retrieve state at this checkpoint
    return this->service->get_service_state(cp.first);
}

void
pbft::set_checkpoint_state(const checkpoint_t& cp, const std::string& data)
{
    // set the service state at the given checkpoint sequence
    // the service is expected to load the state and discard any pending operations
    // prior to the sequence number, then execute any subsequent operations sequentially
    this->service->set_service_state(cp.first, data);
}

void
pbft::maybe_adopt_checkpoint(const checkpoint_t& cp)
{
    if (this->unstable_checkpoint_proofs[cp].size() < this->quorum_size())
    {
        return;
    }

    pbft_membership_msg msg;
    msg.set_type(PBFT_MMSG_GET_STATE);
    msg.set_sequence(cp.first);
    msg.set_state_hash(cp.second);

    auto msg_ptr = std::make_shared<bzn::encoded_message>(this->wrap_message(msg));
    this->node->send_message_str(make_endpoint(this->get_primary()), msg_ptr);



//    this->stable_checkpoint = cp;
//    this->stable_checkpoint_proof = this->unstable_checkpoint_proofs[cp];
//
//    LOG(info) << boost::format("Checkpoint %1% at seq %2% is now stable; clearing old data")
//                 % cp.second
//                 % cp.first;
//
//    this->clear_local_checkpoints_until(cp);
//    this->clear_checkpoint_messages_until(cp);
//    this->clear_operations_until(cp);
//
//    this->low_water_mark = std::max(this->low_water_mark, cp.first);
//    this->high_water_mark = std::max(this->high_water_mark, cp.first + std::lround(HIGH_WATER_INTERVAL_IN_CHECKPOINTS*CHECKPOINT_INTERVAL));
}

void
pbft::clear_local_checkpoints_until(const checkpoint_t& cp)
{
    const auto local_start = this->local_unstable_checkpoints.begin();
    // Iterator to the first unstable checkpoint that's newer than this one. This logic assumes that CHECKPOINT_INTERVAL
    // is >= 2, otherwise we would have do do something awkward here
    const auto local_end = this->local_unstable_checkpoints.upper_bound(checkpoint_t(cp.first+1, ""));
    const size_t local_removed = std::distance(local_start, local_end);
    this->local_unstable_checkpoints.erase(local_start, local_end);
    LOG(debug) << boost::format("Cleared %1% unstable local checkpoints") % local_removed;
}

void
pbft::clear_checkpoint_messages_until(const checkpoint_t& cp)
{
    const auto start = this->unstable_checkpoint_proofs.begin();
    const auto end = this->unstable_checkpoint_proofs.upper_bound(checkpoint_t(cp.first+1, ""));
    const size_t to_remove = std::distance(start, end);
    this->unstable_checkpoint_proofs.erase(start, end);
    LOG(debug) << boost::format("Cleared %1% unstable checkpoint proof sets") % to_remove;
}

void
pbft::clear_operations_until(const checkpoint_t& cp)
{
    size_t ops_removed = 0;
    auto it = this->operations.begin();
    while (it != this->operations.end())
    {
        if(it->second->sequence <= cp.first)
        {
            it = this->operations.erase(it);
            ops_removed++;
        }
        else
        {
            it++;
        }
    }

    LOG(debug) << boost::format("Cleared %1% old operation records") % ops_removed;
}

size_t
pbft::quorum_size() const
{
    return 1 + (2*this->max_faulty_nodes());
}

size_t
pbft::max_faulty_nodes() const
{
    return this->current_peers().size()/3;
}

void
pbft::handle_database_message(const bzn::json_message& json, std::shared_ptr<bzn::session_base> session)
{
    bzn_msg msg;
    database_response response;

    LOG(debug) << "got database message: " << json.toStyledString();

    if (!json.isMember("msg"))
    {
        LOG(error) << "Invalid message: " << json.toStyledString().substr(0,MAX_MESSAGE_SIZE) << "...";
        response.mutable_error()->set_message(bzn::MSG_INVALID_CRUD_COMMAND);
        session->send_message(std::make_shared<bzn::encoded_message>(response.SerializeAsString()), true);
        return;
    }

    if (!msg.ParseFromString(boost::beast::detail::base64_decode(json["msg"].asString())))
    {
        LOG(error) << "Failed to decode message: " << json.toStyledString().substr(0,MAX_MESSAGE_SIZE) << "...";
        response.mutable_error()->set_message(bzn::MSG_INVALID_CRUD_COMMAND);
        session->send_message(std::make_shared<bzn::encoded_message>(response.SerializeAsString()), true);
        return;
    }

    *response.mutable_header() = msg.db().header();

    pbft_request req;
    *req.mutable_operation() = msg.db();
    req.set_timestamp(this->now()); //TODO: the timestamp needs to come from the client

    this->handle_request(req, json, session);

    LOG(debug) << "Sending request ack: " << response.ShortDebugString();
    session->send_message(std::make_shared<bzn::encoded_message>(response.SerializeAsString()), false);
}

uint64_t
pbft::get_low_water_mark()
{
    return this->low_water_mark;
}

uint64_t
pbft::get_high_water_mark()
{
    return this->high_water_mark;
}

bool
pbft::is_view_valid() const
{
    return this->view_is_valid;
}

bool
pbft::is_valid_viewchange_message(const pbft_msg& msg) const
{
    return (msg.type() == PBFT_MSG_VIEWCHANGE) && (msg.view() == this->view + 1);
}

bool
pbft::is_valid_newview_message(const pbft_msg& msg) const
{
    // the view change messages are valid
    auto V = msg.viewchange_messages();
    LOG(debug) << " Are the viewchange messages valid: [" << V.size() << "]";
    for(const auto& v : V)
    {
        pbft_msg viewchange;
        viewchange.ParseFromString(v);
        if ( (viewchange.type() != PBFT_MSG_VIEWCHANGE) || (viewchange.view()!= this->get_view()+1) )
        {
            return false;
        }
    }
    //      - The set O is correct
    //          - None of the messages it knows about are lost ???
//    auto O = msg.preprepare_messages();
//    LOG(debug) << " Are the prepare messages, O, valid: [" << V.size() << "]";
//    // ?? O.size() == this->prepared_operations_since_last_checkpoint().size()
//
//    for(const auto& o : O)
//    {
//        LOG (debug) << o;
//
//    }

    return (msg.type() == PBFT_MSG_NEWVIEW) && (msg.view() == this->view + 1);
}

void
pbft::handle_viewchange(const pbft_msg& msg, const wrapped_bzn_msg& original_msg)
{
    LOG(debug) << "\t*** handle_viewchange: " << msg.SerializeAsString()  << " -- " << original_msg.SerializeAsString();
    if (this->is_primary())
    {
        LOG(debug) << "\t*** primary";
        // KEP-633 - When the primary of view v+1 receives 2f valid view change messages
        // for view v+1,

        // TODO: Dry this by moving it one block up.
        // what is a valid vew change messsage? For now check that it is for view + 1;
        if (this->is_valid_viewchange_message(msg))
        {
            this->valid_view_change_messages.emplace(msg.SerializeAsString());
        }

        LOG(debug) << "\n&***this->max_faulty_nodes(): " << this->max_faulty_nodes();

        if (this->valid_view_change_messages.size() == 2 * this->max_faulty_nodes())
        {
            pbft_msg new_view;
            //       It sends a message <NEWVIEW, v+1, V, O> where
            new_view.set_type(PBFT_MSG_NEWVIEW);
            //          v+1 is the new view index
            new_view.set_view(this->view + 1);
            //          V is the set of 2f+1 view change messages
            ///new_view.set_??? V?

            //          O is a set of prepare messages computed by the new primary as follows:
            //              - Each message in some P from a view-change after the latest stable
            //              checkpoint is given a new preprepare in the new view
            //              - If there are gaps, they are filled with null requests
            //              - These messages are added to the log as normal
            //new_view.set_??? O?

            this->broadcast(this->wrap_message(new_view));

            //        - It then moves to v+1
            LOG(debug) << "\t*** %%%moving to view +1";
            this->view += 1;
            this->view_is_valid = true;
            this->valid_view_change_messages.clear();
        }
    }
    else
    {
        LOG(debug) << "\t*** *NOT* primary";
        // When a replica receives f+1 view change messages, it sends one as well
        // even if its timer has not yet expired - KEP-632

        // TODO: Dry this by moving it one block up.
        // Does this need to be a valid view? What is a valid vew change messsage? For now check that it is for view + 1;
        if (this->is_valid_viewchange_message(msg))
        {
            this->valid_view_change_messages.emplace(msg.SerializeAsString());
        }

        LOG(debug) << "\t*** this->valid_view_change_messages.size():" << this->valid_view_change_messages.size()  << "\t target:" <<   this->max_faulty_nodes() + 1;

        if (this->valid_view_change_messages.size() == (this->max_faulty_nodes() + 1) )
        {
            // TODO: DRY Refactor the following view_change, it is duplicated in handle_failure
            pbft_msg view_change;
            // <VIEW-CHANGE v+1, n, C, P, i>_sigma_i
            view_change.set_type(PBFT_MSG_VIEWCHANGE);

            // v + 1 = this->view + 1
            view_change.set_view(this->view + 1);

            // n = sequence # of last valid checkpoint
            //   = this->stable_checkpoint.first

            auto x = this->latest_stable_checkpoint().first;
            LOG(debug) << "\t***this->latest_stable_checkpoint().first:[" << x << "]";

            view_change.set_sequence(this->latest_stable_checkpoint().first);

            // C = a set of local 2*f + 1 valid checkpoint messages
            //   = ?? I can get: **** this->stable_checkpoint_proof is a set of 2*f+1 map of uuid's to strings <- is this correct?
            for (const auto& msg : this->stable_checkpoint_proof)
            {
                view_change.add_checkpoint_messages(msg.second);
            }

            // P = a set (of client requests) containing a set P_m  for each request m that prepared at i with a sequence # higher
            //     than n
            // P_m = the pre prepare and the 2 f + 1 prepares
            //            get the set of operations, frome each operation get the messages..

            // std::set<std::shared_ptr<bzn::pbft_operation>> prepared_operations_since_last_checkpoint()
            this->broadcast(this->wrap_message(view_change));
        }
    }
}

void
pbft::handle_newview(const pbft_msg& msg, const wrapped_bzn_msg& original_msg)
{
    LOG(debug) << "\t*** handle_newview" << msg.SerializeAsString()  << " -- " << original_msg.SerializeAsString();

    if (!this->is_primary())
    {
        LOG(debug) << "\t***I am a backup";
        // KEP-634 - A backup accepts a new-view message for view v+1 if
        //      - the view change messages are valid
        if(this->is_valid_newview_message(msg))
        {
            LOG(debug) << "\t***new view message is valid.";
            //      - It then moves to view v+1,
            this->view = msg.view();
            // processing the preprepares in O as normal. ???

            // DO I need to send the set O to be processed?
        }
    }
}

std::string
pbft::get_name()
{
    return "pbft";
}


bzn::json_message
pbft::get_status()
{
    bzn::json_message status;

    std::lock_guard<std::mutex> lock(this->pbft_lock);

    status["outstanding_operations_count"] = uint64_t(this->outstanding_operations_count());
    status["is_primary"] = this->is_primary();

    auto primary = this->get_primary();
    status["primary"]["host"] = primary.host;
    status["primary"]["host_port"] = primary.port;
    status["primary"]["http_port"] = primary.http_port;
    status["primary"]["name"] = primary.name;
    status["primary"]["uuid"] = primary.uuid;

    status["latest_stable_checkpoint"]["sequence_number"] = this->latest_stable_checkpoint().first;
    status["latest_stable_checkpoint"]["hash"] = this->latest_stable_checkpoint().second;
    status["latest_checkpoint"]["sequence_number"] = this->latest_checkpoint().first;
    status["latest_checkpoint"]["hash"] = this->latest_checkpoint().second;

    status["unstable_checkpoints_count"] = uint64_t(this->unstable_checkpoints_count());
    status["next_issued_sequence_number"] = this->next_issued_sequence_number;
    status["view"] = this->view;

    status["peer_index"] = bzn::json_message();
    for(const auto& p : this->current_peers())
    {
        bzn::json_message peer;
        peer["host"] = p.host;
        peer["port"] = p.port;
        peer["http_port"] = p.http_port;
        peer["name"] = p.name;
        peer["uuid"] = p.uuid;
        status["peer_index"].append(peer);
    }

    return status;
}

bool
pbft::initialize_configuration(const bzn::peers_list_t& peers)
{
    auto config = std::make_shared<pbft_configuration>();
    bool config_good = true;
    for (auto& p : peers)
    {
        config_good &= config->add_peer(p);
    }

    if (!config_good)
    {
        LOG(warning) << "One or more peers could not be added to configuration";
    }

    this->configurations.add(config);
    this->configurations.enable(config->get_hash());
    this->configurations.set_current(config->get_hash());

    return config_good;
}

std::shared_ptr<const std::vector<bzn::peer_address_t>>
pbft::current_peers_ptr() const
{
    auto config = this->configurations.current();
    if (config)
    {
        return config->get_peers();
    }

    throw std::runtime_error("No current configuration!");
}

const std::vector<bzn::peer_address_t>&
pbft::current_peers() const
{
    return *(this->current_peers_ptr());
}

const peer_address_t&
pbft::get_peer_by_uuid(const std::string& uuid) const
{
    for (auto const& peer : this->current_peers())
    {
        if (peer.uuid == uuid)
        {
            return peer;
        }
    }

    // something went wrong. this uuid should exist
    throw std::runtime_error("peer missing from peers list");
}

void
pbft::broadcast_new_configuration(pbft_configuration::shared_const_ptr config)
{
    pbft_request req;
    req.set_type(PBFT_REQ_NEW_CONFIG);
    auto cfg_msg = new pbft_config_msg;
    cfg_msg->set_configuration(config->to_string());
    req.set_allocated_config(cfg_msg);

    auto smsg = req.SerializeAsString();
    auto op  = this->setup_request_operation(smsg, this->crypto->hash(smsg));
    this->do_preprepare(op);
}

bool
pbft::is_configuration_acceptable_in_new_view(hash_t config_hash)
{
    return this->configurations.is_enabled(config_hash);
}

void
pbft::handle_config_message(const pbft_msg& msg, const std::shared_ptr<pbft_operation>& op)
{
    auto const& request = op->get_request();
    assert(request.type() == PBFT_REQ_NEW_CONFIG);
    auto config = std::make_shared<pbft_configuration>();
    if (msg.type() == PBFT_MSG_PREPREPARE && config->from_string(request.config().configuration()))
    {
        if (this->proposed_config_is_acceptable(config))
        {
            // store this configuration
            this->configurations.add(config);
        }
    }
}

bool
pbft::move_to_new_configuration(hash_t config_hash)
{
    if (this->configurations.is_enabled(config_hash))
    {
        this->configurations.set_current(config_hash);
        this->configurations.remove_prior_to(config_hash);
        return true;
    }

    return false;
}

bool
pbft::proposed_config_is_acceptable(std::shared_ptr<pbft_configuration> /* config */)
{
    return true;
}

std::set<std::shared_ptr<bzn::pbft_operation>>
pbft::prepared_operations_since_last_checkpoint()
{
    std::set<std::shared_ptr<bzn::pbft_operation>> retval;
    // TODO functional filter...
    for (const auto& p : this->operations)
    {
        if (p.second->is_prepared() && !p.second->is_committed())
        {
            if (p.second->sequence > this->latest_stable_checkpoint().first)
            {
                retval.emplace(p.second);
            }
        }
    }
    return retval;
}

timestamp_t
pbft::now() const
{
    return static_cast<timestamp_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

void
pbft::saw_request(const pbft_request& req, const request_hash_t& hash)
{
    this->recent_requests.insert(std::make_pair(req.timestamp(),
        std::make_pair(req.client(), hash)));
}

bool
pbft::already_seen_request(const pbft_request& req, const request_hash_t& hash) const
{
    auto range = this->recent_requests.equal_range(req.timestamp());
    for (auto r = range.first; r != range.second; r++)
    {
        if (r->second.first == req.client() && r->second.second == hash)
        {
            return true;
        }
    }

    return false;
}