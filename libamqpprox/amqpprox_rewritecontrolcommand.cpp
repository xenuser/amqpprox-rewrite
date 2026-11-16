// SPDX-License-Identifier: Apache-2.0
#include <amqpprox_rewritecontrolcommand.h>

#include <amqpprox_logging.h>
#include <amqpprox_rewrite.h>
#include <amqpprox_server.h>

#include <boost/algorithm/string.hpp>

#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace Bloomberg {
namespace amqpprox {

RewriteControlCommand::RewriteControlCommand(std::string userVhostMapFile)
: d_userVhostMapFile(std::move(userVhostMapFile))
{
}

std::string RewriteControlCommand::commandVerb() const
{
    return "REWRITE";
}

std::string RewriteControlCommand::helpText() const
{
    return "RELOAD (re-read the --rewriteVhostUserMap file) | PRINT";
}

void RewriteControlCommand::handleCommand(const std::string & /* command */,
                                          const std::string   &restOfCommand,
                                          const OutputFunctor &outputFunctor,
                                          Server              *serverHandle,
                                          Control * /* controlHandle */)
{
    ControlCommandOutput<OutputFunctor> output(outputFunctor);

    std::istringstream iss(restOfCommand);
    std::string        subcommand;
    iss >> subcommand;
    boost::to_upper(subcommand);

    if (subcommand == "RELOAD") {
        // Parse fully before swapping: on any error the current map stays.
        try {
            if (d_userVhostMapFile.empty()) {
                throw std::invalid_argument(
                    "Per-user vhost mapping is not enabled; start the proxy "
                    "with --rewriteVhostUserMap");
            }
            std::ifstream input(d_userVhostMapFile);
            if (!input) {
                throw std::invalid_argument("Cannot read '" +
                                            d_userVhostMapFile + "'");
            }
            auto userVhosts = std::make_shared<const UserVhostMap>(
                parseUserVhostMap(input));
            const auto count = userVhosts->size();
            serverHandle->setUserVhosts(std::move(userVhosts));

            LOG_INFO << "Reloaded " << count << " user vhost mappings from '"
                     << d_userVhostMapFile << "'";
            output << "OK: reloaded " << count << " user vhost mappings from '"
                   << d_userVhostMapFile << "'\n";
        }
        catch (const std::invalid_argument &error) {
            LOG_WARN << "User vhost map reload failed, keeping the current "
                        "map: "
                     << error.what();
            output << "ERROR: " << error.what()
                   << "; the current map is kept\n";
        }
    }
    else if (subcommand == "PRINT") {
        const auto config = serverHandle->rewriteConfig();
        output << "Source vhost: '" << config.sourceVhost << "'\n"
               << "Default destination: '" << config.targetVhost << "'\n"
               << "Quorum queues: " << (config.quorumQueues ? "yes" : "no")
               << ", pass-through unconvertible: "
               << (config.passThroughUnconvertible ? "yes" : "no")
               << ", promote non-durable to durable: "
               << (config.promoteTransientQueues ? "yes" : "no") << "\n";
        if (config.userVhosts) {
            output << "User vhost map '" << d_userVhostMapFile << "', "
                   << config.userVhosts->size() << " mappings:\n";
            for (const auto &[user, vhost] : *config.userVhosts) {
                output << "  " << user << " -> " << vhost << "\n";
            }
        }
        else {
            output << "No user vhost map\n";
        }
    }
    else {
        output << "Unrecognized subcommand. Use RELOAD or PRINT.\n";
    }
}

}
}
