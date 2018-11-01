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

using namespace ::testing;

namespace bzn
{
    using namespace test;

    class pbft_proto_test : public pbft_test
    {
    public:
        // send a fake request to SUT
        std::shared_ptr<pbft_operation> send_request();

        // send a preprepare message to SUT
        void send_preprepare(uint64_t sequence, const pbft_request& request);

        // send fake prepares from all nodes to SUT
        void send_prepares(uint64_t sequence, const pbft_request& request);

        // send fake commits from all nodes to SUT
        void send_commits(uint64_t sequence, const pbft_request& request);

        void prepare_for_checkpoint(size_t seq);

        void force_checkpoint(size_t seq);

        void run_transaction_through_primary(bool commit = true);

        void run_transaction_through_backup(bool commit = true);

    protected:
        size_t index = 0;
        uint64_t view = 1;
    };
}

