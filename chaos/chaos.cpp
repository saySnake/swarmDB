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

#include <random>
#include <boost/format.hpp>
#include <chaos/chaos.hpp>
#include <options/simple_options.hpp>

using namespace bzn;

chaos::chaos(std::shared_ptr<bzn::asio::io_context_base> io_context, const bzn::options_base& options)
        : io_context(io_context)
        , options(options)
        , crash_timer(io_context->make_unique_steady_timer())
{
    // We don't need cryptographically secure randomness here, but it does need to be of reasonable quality and differ across processes
    std::random_device rd;
    this->random.seed(rd());
}

void
chaos::start()
{
    if (this->options.get_simple_options().get<bool>(bzn::option_names::CHAOS_ENABLED))
    {
        std::call_once(
                this->start_once, [this]()
                {
                    this->start_crash_timer();
                }
        );
    }
}


void
chaos::start_crash_timer()
{
    std::weibull_distribution<double> distribution(
            this->options.get_simple_options().get<double>(bzn::option_names::CHAOS_NODE_FAILURE_SHAPE),
            this->options.get_simple_options().get<double>(bzn::option_names::CHAOS_NODE_FAILURE_SCALE));

    double hours_until_crash = distribution(this->random);
    LOG(info) << boost::format("Chaos module will trigger this node crashing in %1$.2f hours") % hours_until_crash;

    auto time_until_crash = std::chrono::duration<double, std::chrono::hours::period>(hours_until_crash);
    LOG(info) << time_until_crash.count();
    LOG(info) << std::chrono::duration_cast<std::chrono::milliseconds>(time_until_crash).count();

    this->crash_timer->expires_from_now(std::chrono::duration_cast<std::chrono::milliseconds>(time_until_crash));

    // Doing this with this timer means that crashes will only occur at times where boost schedules
    // a new callback to take place, rather than truly at random.
    this->crash_timer->async_wait(std::bind(&chaos::handle_crash_timer, shared_from_this(), std::placeholders::_1));
}

void
chaos::handle_crash_timer(const boost::system::error_code& /*ec*/)
{
    if (!this->options.get_simple_options().get<bool>(bzn::option_names::CHAOS_ENABLED))
    {
        return;
    }

    LOG(fatal) << "Chaos module triggering node crash";

    std::abort();
    // Intentionally crashing abruptly
}
