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

using namespace ::testing;

namespace bzn
{
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


}