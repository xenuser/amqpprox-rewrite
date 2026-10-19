// SPDX-License-Identifier: Apache-2.0
#ifndef BLOOMBERG_AMQPPROX_REWRITE
#define BLOOMBERG_AMQPPROX_REWRITE

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace Bloomberg {
namespace amqpprox {

class Frame;

// Immutable startup configuration. One tenant mapping per proxy process.
struct RewriteConfig {
    std::string sourceVhost = "/";
    std::string targetVhost;
    bool quorumQueues = false;

    void validate() const;
    bool matches(const std::string &vhost) const;
};

// Returns an empty vector when no change is needed. Otherwise returns a full
// AMQP frame. All unrelated bytes are preserved, including nested arguments.
// Invalid/unsupported declarations throw; the session closes and logs why.
std::vector<uint8_t> rewriteQueueDeclare(const Frame &frame,
                                         std::size_t maxFrameSize);

}
}
#endif
