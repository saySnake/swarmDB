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

#include <pbft/test/pbft_test_common.hpp>
#include <pbft/test/pbft_proto_test.hpp>
#include <utils/make_endpoint.hpp>

using namespace ::testing;

namespace bzn
{
    namespace test
    {
        pbft_membership_msg
        extract_pbft_membership_msg(std::string msg)
        {
            wrapped_bzn_msg outer;
            outer.ParseFromString(msg);
            pbft_membership_msg result;
            result.ParseFromString(outer.payload());
            return result;
        }

        bool
        is_get_state(std::shared_ptr<std::string> wrapped_msg)
        {
            pbft_membership_msg msg = extract_pbft_membership_msg(*wrapped_msg);

            return msg.type() == PBFT_MMSG_GET_STATE && msg.sequence() > 0 && extract_sender(*wrapped_msg) != ""
                && msg.state_hash() != "";
        }
    }

    using namespace test;

    class pbft_catchup_test : public pbft_proto_test
    {
    public:

        void
        run_non_executing_transaction()
        {
            // create request
            pbft_request request;
            request.set_type(PBFT_REQ_DATABASE);
            auto dmsg = new database_msg;
            auto create = new database_create;
            create->set_key(std::string("key_" + std::to_string(++this->index)));
            create->set_value(std::string("value_" + std::to_string(this->index)));
            dmsg->set_allocated_create(create);
            request.set_allocated_operation(dmsg);

            // send pre-prepare to SUT
            send_preprepare(this->index, request);

            // send prepares to SUT
            send_prepares(this->index, request);

            // send commits to SUT. It should NOT post the transaction
            EXPECT_CALL(*(this->mock_io_context), post(_)).Times(Exactly(0));
            for (const auto peer : TEST_PEER_LIST)
            {
                pbft_msg commit;

                commit.set_view(this->view);
                commit.set_sequence(this->index);
                commit.set_type(PBFT_MSG_COMMIT);
                commit.set_allocated_request(new pbft_request(request));
                auto wmsg = wrap_pbft_msg(commit);
                wmsg.set_sender(peer.uuid);
                pbft->handle_message(commit, wmsg);
            }
        }
    };

    TEST_F(pbft_catchup_test, new_node_initially_doesnt_execute_requests)
    {
        this->uuid = SECOND_NODE_UUID;
        this->build_pbft();
        this->run_non_executing_transaction();
    }

    TEST_F(pbft_catchup_test, new_node_requests_state_after_checkpoint)
    {
        this->uuid = SECOND_NODE_UUID;
        this->build_pbft();
        this->set_first_sequence_to_execute(std::numeric_limits<uint64_t>::max());

        // node shouldn't be sending any checkpoint messages right now
        EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_checkpoint, Eq(true))))
            .Times((Exactly(0)));

        auto nodes = TEST_PEER_LIST.begin();
        size_t req_nodes = 2 * this->faulty_nodes_bound();
        for (size_t i = 0; i < req_nodes; i++)
        {
            bzn::peer_address_t node(*nodes++);
            send_checkpoint(node, 100);
        }

        // one more checkpoint message and the node should request state from primary
        auto primary = this->pbft->get_primary();
        EXPECT_CALL(*mock_node, send_message_str(make_endpoint(primary), ResultOf(is_get_state, Eq(true))))
            .Times((Exactly(1)));

        bzn::peer_address_t node(*nodes++);
        send_checkpoint(node, 100);

    }

    TEST_F(pbft_catchup_test, primary_provides_state)
    {
        this->build_pbft();

        for (size_t i = 0; i < 99; i++)
        {
            run_transaction_through_primary();
        }
        prepare_for_checkpoint(100);
        run_transaction_through_primary();
        stabilize_checkpoint(100);



    }
}