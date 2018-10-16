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
#include <utils/make_endpoint.hpp>

using namespace ::testing;

namespace bzn::test
{
    const bzn::peer_address_t new_peer{"127.0.0.1", 8090, 83, "name_new", "uuid_new"};

    MATCHER_P(message_is_correct_type, type, "")
    {
        wrapped_bzn_msg message;
        if (message.ParseFromString(*arg))
        {
            if (message.type() == bzn_msg_type::BZN_MSG_PBFT)
            {
                pbft_msg inner_msg;
                if (inner_msg.ParseFromString(message.payload()))
                {
                    if (inner_msg.type() == type)
                    {
                        return true;
                    }
                }

            }
        }
        return false;
    }

    TEST_F(pbft_test, join_request_generates_new_config)
    {
        this->build_pbft();

        auto info = new pbft_peer_info;
        info->set_host(new_peer.host);
        info->set_name(new_peer.name);
        info->set_port(new_peer.port);
        info->set_http_port(new_peer.http_port);
        info->set_uuid(new_peer.uuid);

        pbft_msg join_msg;
        join_msg.mutable_request()->set_client("bob");
        join_msg.mutable_request()->set_timestamp(1);
        join_msg.set_type(PBFT_MSG_JOIN);
        join_msg.set_allocated_peer_info(info);

        // each peer should be sent a new_config message when the join is received
        for (auto const& p : TEST_PEER_LIST)
        {
            EXPECT_CALL(*(this->mock_node),
                send_message_str(bzn::make_endpoint(p), message_is_correct_type(PBFT_MSG_NEW_CONFIG)))
                .Times(Exactly(1));
        }

        this->pbft->handle_message(join_msg);
    }

}
