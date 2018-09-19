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

#include <chaos/chaos.hpp>
#include <mocks/mock_boost_asio_beast.hpp>
#include <gtest/gtest.h>
#include <options/options.hpp>

using namespace ::testing;

class chaos_test : public Test
{
public:
    bzn::options options;
    std::shared_ptr<bzn::asio::Mockio_context_base> mock_io_context = std::make_shared<NiceMock<bzn::asio::Mockio_context_base>>();

    std::unique_ptr<bzn::asio::Mocksteady_timer_base> node_crash_timer =
            std::make_unique<NiceMock<bzn::asio::Mocksteady_timer_base>>();
    bzn::asio::wait_handler node_crash_handler;

    std::shared_ptr<bzn::chaos> chaos;

    // This pattern copied from audit test
    chaos_test()
    {
        EXPECT_CALL(*(this->mock_io_context), make_unique_steady_timer())
                .WillOnce(Invoke(
                        [&](){return std::move(this->node_crash_timer);}
                ));

        EXPECT_CALL(*(this->node_crash_timer), async_wait(_))
                .Times(Exactly(1)) // This enforces the assumption: if this actually the leader progress timer,
                        // then there are some tests that will never wait on it
                .WillRepeatedly(Invoke(
                        [&](auto handler){this->node_crash_handler = handler;}
                ));
        
        this->options.get_mutable_simple_options().set(bzn::option_names::CHAOS_ENABLED, "true");
    }

    void build_chaos()
    {
        this->chaos = std::make_shared<bzn::chaos>(this->mock_io_context, this->options);
        this->chaos->start();
    }

};

TEST_F(chaos_test, test_timer_constructed_correctly)
{
    // This just tests the expecations built into the superclass
    this->build_chaos();
}
