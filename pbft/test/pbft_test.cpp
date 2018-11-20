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
#include <set>
#include <mocks/mock_session_base.hpp>
#include <utils/make_endpoint.hpp>
#include <gtest/gtest.h>

namespace bzn
{
    namespace test
    {
        TEST_F(pbft_test, test_requests_create_operations)
        {
            this->build_pbft();
            ASSERT_TRUE(pbft->is_primary());
            ASSERT_EQ(0u, pbft->outstanding_operations_count());
            pbft->handle_database_message(this->request_json, this->mock_session);
            ASSERT_EQ(1u, pbft->outstanding_operations_count());
        }

        TEST_F(pbft_test, test_requests_fire_preprepare)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_preprepare, Eq(true))))
                .Times(Exactly(TEST_PEER_LIST.size()));

            pbft->handle_database_message(this->request_json, this->mock_session);
        }

        TEST_F(pbft_test, test_forwarded_to_primary_when_not_primary)
        {
            boost::asio::ip::tcp::endpoint msg_sent_to;
            bzn::json_message msg_sent;

            EXPECT_CALL(*mock_node, send_message(_, _)).Times(1).WillRepeatedly(Invoke(
                [&](auto ep, auto msg) {
                    EXPECT_EQ(ep, make_endpoint(this->pbft->get_primary()));
                    EXPECT_EQ((*msg)["bzn-api"], "database");
                }));

            this->uuid = SECOND_NODE_UUID;
            this->build_pbft();
            EXPECT_FALSE(pbft->is_primary());

            pbft->handle_database_message(this->request_json, this->mock_session);

        }

        std::set<uint64_t> seen_sequences;

        void
        save_sequences(const boost::asio::ip::tcp::endpoint & /*ep*/, std::shared_ptr<std::string> wrapped_msg)
        {
            pbft_msg msg = extract_pbft_msg(*wrapped_msg);
            seen_sequences.insert(msg.sequence());
        }


        TEST_F(pbft_test, test_different_requests_get_different_sequences)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, _)).WillRepeatedly(Invoke(save_sequences));

            database_msg req, req2;
            req.mutable_header()->set_transaction_id(5);
            req2.mutable_header()->set_transaction_id(1055);

            seen_sequences = std::set<uint64_t>();
            pbft->handle_database_message(wrap_request(req), this->mock_session);
            pbft->handle_database_message(wrap_request(req2), this->mock_session);
            ASSERT_EQ(seen_sequences.size(), 2u);
        }

        TEST_F(pbft_test, test_preprepare_triggers_prepare)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_prepare, Eq(true))))
                .Times(Exactly(TEST_PEER_LIST.size()));

            this->pbft->handle_message(this->preprepare_msg, default_original_msg);
        }

        TEST_F(pbft_test, test_prepare_contains_uuid)
        {
            this->build_pbft();
            std::shared_ptr<std::string> wrapped_msg;
            EXPECT_CALL(*mock_node, send_message_str(_, _)).WillRepeatedly(SaveArg<1>(&wrapped_msg));

            this->pbft->handle_message(this->preprepare_msg, default_original_msg);

            pbft_msg msg_sent = extract_pbft_msg(*wrapped_msg);

            ASSERT_EQ(extract_sender(*wrapped_msg), this->pbft->get_uuid());
            ASSERT_EQ(extract_sender(*wrapped_msg), TEST_NODE_UUID);
        }

        TEST_F(pbft_test, test_wrong_view_preprepare_rejected)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, _)).Times(Exactly(0));

            pbft_msg preprepare2(this->preprepare_msg);
            preprepare2.set_view(6);

            this->pbft->handle_message(preprepare2, default_original_msg);
        }

        TEST_F(pbft_test, test_no_duplicate_prepares_same_sequence_number)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, _)).Times(Exactly(TEST_PEER_LIST.size()));

            pbft_msg preprepare2(this->preprepare_msg);
            preprepare2.set_request_hash("some other hash");



            this->pbft->handle_message(
            this->preprepare_msg, default_original_msg);
            this->pbft->handle_message(preprepare2, default_original_msg);
        }

        TEST_F(pbft_test, test_commit_messages_sent)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_prepare, Eq(true))))
                .Times(Exactly(TEST_PEER_LIST.size()));
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_commit, Eq(true))))
                .Times(Exactly(TEST_PEER_LIST.size()));

            this->pbft->handle_message(this->preprepare_msg, default_original_msg);
            for (const auto &peer : TEST_PEER_LIST)
            {
                pbft_msg prepare = pbft_msg(this->preprepare_msg);
                prepare.set_type(PBFT_MSG_PREPARE);
                this->pbft->handle_message(prepare, from(peer.uuid));
            }
        }

        TEST_F(pbft_test, test_commits_applied)
        {
            EXPECT_CALL(*(this->mock_io_context), post(_)).Times(Exactly(1));
            this->build_pbft();

            pbft_msg preprepare = pbft_msg(this->preprepare_msg);
            preprepare.set_sequence(1);
            this->pbft->handle_message(preprepare, default_original_msg);

            for (const auto &peer : TEST_PEER_LIST)
            {
                pbft_msg prepare = pbft_msg(preprepare);
                pbft_msg commit = pbft_msg(preprepare);
                prepare.set_type(PBFT_MSG_PREPARE);
                commit.set_type(PBFT_MSG_COMMIT);
                this->pbft->handle_message(prepare, from(peer.uuid));
                this->pbft->handle_message(commit, from(peer.uuid));
            }
        }

        TEST_F(pbft_test, dummy_pbft_service_does_not_crash)
        {
            mock_service->query(request_msg, 0);
            mock_service->consolidate_log(2);
        }

        TEST_F(pbft_test, client_request_results_in_message_ack)
        {
            auto mock_session = std::make_shared<NiceMock<bzn::Mocksession_base>>();
            std::string last_err;

            EXPECT_CALL(*mock_session, send_message(A<std::shared_ptr<std::string>>(), _)).WillRepeatedly(Invoke(
                [&](auto msg, auto) {
                    database_response resp;
                    resp.ParseFromString(*msg);

                    last_err = resp.error().message();
                }
            ));

            this->build_pbft();
            bzn::json_message msg;
            msg["bzn-api"] = "database";

            this->database_handler(msg, mock_session);
            EXPECT_NE(last_err, "");

            msg["msg"] = "not a valid crud message";
            this->database_handler(msg, mock_session);
            EXPECT_NE(last_err, "");

            bzn_msg payload;
            msg["msg"] = boost::beast::detail::base64_encode(payload.SerializeAsString());
            this->database_handler(msg, mock_session);
            EXPECT_EQ(last_err, "");
        }

        TEST_F(pbft_test, client_request_executed_results_in_message_response)
        {
            auto mock_session = std::make_shared<bzn::Mocksession_base>();
            EXPECT_CALL(*mock_session, send_datagram(_)).Times(Exactly(1));

            pbft_request msg;
            auto peers = std::make_shared<const std::vector<bzn::peer_address_t>>();
            auto op = std::make_shared<pbft_operation>(1, 1, "somehash", peers);
            op->set_session(mock_session);

            dummy_pbft_service service(this->mock_io_context);
            service.register_execute_handler([](auto) {});

            service.apply_operation(op);
        }

        TEST_F(pbft_test, messages_after_high_water_mark_dropped)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_prepare, Eq(true))))
                .Times(Exactly(0));

            this->preprepare_msg.set_sequence(pbft->get_high_water_mark() + 1);
            pbft->handle_message(preprepare_msg, default_original_msg);
        }

        TEST_F(pbft_test, messages_before_low_water_mark_dropped)
        {
            this->build_pbft();
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_prepare, Eq(true))))
                .Times(Exactly(0));

            this->preprepare_msg.set_sequence(this->pbft->get_low_water_mark());
            pbft->handle_message(preprepare_msg, default_original_msg);
        }

        TEST_F(pbft_test, request_redirect_to_primary_notifies_failure_detector)
        {
            EXPECT_CALL(*mock_failure_detector, request_seen(_)).Times(Exactly(0));

            this->uuid = SECOND_NODE_UUID;
            this->build_pbft();

            EXPECT_FALSE(pbft->is_primary());
            pbft->handle_database_message(this->request_json, this->mock_session);
        }


        ////////////////////////////////////////////////////////////////
        // TODO: move to new module

        TEST_F(pbft_test, pbft_handle_failure_causes_invalid_view_state)
        {
            // I expect that a replica forced to handle a failure will invalidate
            // its' view, and cause the replica to send a VIEWCHANGE messsage
            EXPECT_CALL(*mock_node, send_message_str(_, _))
                .WillRepeatedly(Invoke([&](const auto & /*endpoint*/, const auto p) {
                    wrapped_bzn_msg wmsg;
                    wmsg.ParseFromString(*p);
                    pbft_msg view_change;
                    view_change.ParseFromString(wmsg.payload());
                    EXPECT_EQ(PBFT_MSG_VIEWCHANGE, view_change.type());
                    EXPECT_TRUE(2 == view_change.view());
                    EXPECT_TRUE(this->pbft->latest_stable_checkpoint().first == view_change.sequence());
                }));

            this->uuid = SECOND_NODE_UUID;
            this->build_pbft();

            // force the failure.
            this->pbft->handle_failure();

            // Now the replicas' view should be invalid
            EXPECT_FALSE(this->pbft->is_view_valid());
        }


        TEST_F(pbft_test, pbft_with_invalid_view_drops_messages)
        {
            EXPECT_CALL(*mock_node, send_message_str(_, _))
                .Times(Exactly(4)); // there are 4 nodes in the test swarm

            // We do not expect the pre-prepares due to the handled message at
            // the end of the test.
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_preprepare, Eq(true))))
                .Times(Exactly(0));

            this->build_pbft();

            // invalidate the view - this will send 4 send_message_str VIEWCHANGE messages
            this->pbft->handle_failure();

            // nothing will happen with this request, that is there will be no new messages
            pbft->handle_message(this->preprepare_msg, default_original_msg);
        }


        TEST_F(pbft_test, pbft_replica_sends_viewchange_message)
        {
            // When a replica receives f+1 view change messages, it sends one as
            // well even if its timer has not yet expired - KEP-632
            const uint64_t NON_FAULTY_REPLICAS = TEST_PEER_LIST.size() / 3;

            this->uuid = SECOND_NODE_UUID;
            this->build_pbft();

            const size_t NEW_VIEW = this->pbft->get_view() + 1;
            EXPECT_FALSE(pbft->is_primary());
            size_t count{0};
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_viewchange, Eq(true))))
                .WillRepeatedly(Invoke([&](auto &, auto &) { ASSERT_EQ((NON_FAULTY_REPLICAS + 1), count); }));
            pbft_msg pbft_msg;
            pbft_msg.set_type(PBFT_MSG_VIEWCHANGE);
            pbft_msg.set_view(NEW_VIEW);

            // let's pretend that the sytem under test is receiving view change messages
            // from the other replicas
            for (const auto &peer : TEST_PEER_LIST)
            {
                count++;
                pbft_msg.set_sender(peer.uuid);
                this->pbft->handle_message(pbft_msg, this->default_original_msg);
                LOG(debug) << "\t***this->pbft->get_view(): " << this->pbft->get_view();
                if (count == (NON_FAULTY_REPLICAS + 1))
                    break;
            }

            EXPECT_EQ(NEW_VIEW, this->pbft->get_view());
            EXPECT_TRUE(this->pbft->is_view_valid());
        }


        TEST_F(pbft_test, pbft_primary_sends_newview_message)
        {
            const uint64_t NON_FAULTY_REPLICAS = TEST_PEER_LIST.size() / 3;
            this->build_pbft();
            EXPECT_TRUE(pbft->is_primary());
            size_t count{0};

            // We are expecting the primary to send the newview message after 2f of
            // the replicas have sent thier view change messages.
            EXPECT_CALL(*mock_node, send_message_str(_, ResultOf(is_newview, Eq(true))))
                .WillRepeatedly(Invoke([&](auto &, auto &) { ASSERT_TRUE((2 * NON_FAULTY_REPLICAS) == count); }));

            pbft_msg view_change_msg;
            view_change_msg.set_type(PBFT_MSG_VIEWCHANGE);
            view_change_msg.set_view(this->pbft->get_view() + 1);

            for (const auto &peer : TEST_PEER_LIST)
            {
                count++;
                view_change_msg.set_sender(peer.uuid);
                this->pbft->handle_message(view_change_msg, this->default_original_msg);
            }
        }


        TEST_F(pbft_test, backup_accepts_newview)
        {
            this->uuid = SECOND_NODE_UUID;
            this->build_pbft();
            EXPECT_TRUE(!pbft->is_primary());
            const size_t NEW_VIEW = this->pbft->get_view() + 1;

            EXPECT_EQ(this->pbft->get_view(), static_cast<size_t>(1));

            pbft_msg newview; // <NEWVIEW v+1, V -> Viewchange messages, O ->Prepare messages>
            newview.set_type(PBFT_MSG_NEWVIEW);
            newview.set_sender(TEST_NODE_UUID);
            newview.set_view(NEW_VIEW);

            // ??? newview.set_viewchange_messages( < 2f+1 V > )

            // ??? newview.set_preprepare_messages(< O >)


            this->pbft->handle_message(newview, this->default_original_msg);

            // it then moves to view v + 1
            EXPECT_EQ(this->pbft->get_view(), NEW_VIEW);

            // processing the preprepares in O as normal
            // ????

        }


        void
        create_and_send_request(std::shared_ptr<pbft> pbft)
        {
            auto request = new pbft_request();
            request->set_type(PBFT_REQ_DATABASE);
            pbft_msg msg;
            msg.set_type(PBFT_MSG_PREPREPARE);

            pbft->handle_request(msg, wrap_pbft_msg(msg));


            msg.set_allocated_request(request);
            pbft->handle_message(msg, wrap_pbft_msg(msg));
        }

        void
        send_prepares(std::shared_ptr<pbft> pbft)
        {
            auto request = new pbft_request();
            pbft_msg msg;
            msg.set_allocated_request(request);
            pbft->handle_message(msg, wrap_pbft_msg(msg));
        }
    }

    using namespace test;
    TEST_F(pbft_test, full_test)
    {
        this->build_pbft();

        auto operation = std::shared_ptr<pbft_operation>();
        EXPECT_CALL(*this->mock_node, send_message_str(_, ResultOf(test::is_preprepare, Eq(true)))).Times(
                Exactly(TEST_PEER_LIST.size()))
            .WillRepeatedly(Invoke([&](auto, auto enc_bzn_msg) {
                wrapped_bzn_msg wmsg;
                wmsg.ParseFromString(*enc_bzn_msg);


                pbft_msg msg;
                if (msg.ParseFromString(wmsg.payload()))
                {
                    if (operation == nullptr)
                        operation = this->pbft->find_operation(this->pbft->get_view(), msg.sequence(), msg.request());
                }
            }));


        EXPECT_CALL(*this->mock_node, send_message_str(_, ResultOf(test::is_commit, Eq(true)))).Times(
                        Exactly(TEST_PEER_LIST.size()))
                .WillRepeatedly(Invoke([&](auto, auto /*enc_bzn_msg*/) {
                    /*pbft_msg msg;
                    if (msg.ParseFromString(*enc_bzn_msg))
                    {
                        if (op == nullptr)
                            op = this->pbft->find_operation(this->pbft->get_view(), msg.sequence(), msg.request());
                    }*/
                }));






        // sending the initial request from a client
        auto request = new pbft_request();
        request->set_type(PBFT_REQ_DATABASE);
        bzn::json_message empty_json_msg;
        pbft->handle_request(*request, empty_json_msg);
        ASSERT_NE(operation, nullptr);





        // now we fake sending prepare messages from the backups
        for (const auto peer : TEST_PEER_LIST)
        {
            pbft_msg prepare;

            prepare.set_view(operation->view);
            prepare.set_sequence(operation->sequence);
            prepare.set_type(PBFT_MSG_PREPARE);
            prepare.set_allocated_request(new pbft_request(operation->request));


            auto wmsg = wrap_pbft_msg(prepare);

            // TEST_PEER_LIST

            wmsg.set_sender(peer.uuid);

            pbft->handle_message(prepare, wmsg);



        }









        // we have our SUT

        // 1 - create a request
        // 2 - SUT does this: pbft->handle_message
        // 3 - pbft sends a bunch of pre-prepares to "backups" - EXPECT that they are sent
        // 4 - fake prepares from "backups" to SUT
        // 5 - Expect the SUT to broadcast commits to all backups
        // 6a -
        // 6b -

        // ...
        // n - goto 1 101 times
        // n+1






    }

}

