// SPDX-License-Identifier: Apache-2.0
#ifndef BLOOMBERG_AMQPPROX_REWRITE
#define BLOOMBERG_AMQPPROX_REWRITE

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace Bloomberg {
namespace amqpprox {

class Frame;

// Immutable startup configuration. One tenant mapping per proxy process.
struct RewriteConfig {
    std::string sourceVhost = "/";
    std::string targetVhost;
    bool        quorumQueues = false;
    bool        passThroughUnconvertible = false;

    void validate() const;
    bool matches(const std::string &vhost) const;
};

/**
 * \brief Raised when a Queue.Declare on a rewriting connection cannot be
 * forwarded. Carries the AMQP reply code used to close the client connection.
 */
class QueueRewriteError : public std::runtime_error {
    uint16_t d_replyCode;

  public:
    QueueRewriteError(uint16_t replyCode, const std::string &text);

    uint16_t replyCode() const { return d_replyCode; }

    /**
     * \return the class and method ids of Queue.Declare, reported as the
     * failing method in the synthesized Connection.Close
     */
    static constexpr uint16_t classId() { return 50; }
    static constexpr uint16_t methodId() { return 10; }
};

/**
 * \brief Rewrite an ingress Queue.Declare frame into a quorum declaration.
 *
 * Returns an empty vector when the frame is forwarded unchanged: it is not an
 * active Queue.Declare, it explicitly requests a non-classic type, or it
 * cannot be converted and `passThroughUnconvertible` is set. Otherwise
 * returns a complete AMQP frame. All unrelated bytes are preserved, except
 * classic-only tuning arguments which have no quorum equivalent and are
 * dropped.
 *
 * \throw QueueRewriteError for malformed declarations, declarations exceeding
 * `maxFrameSize` after rewriting, and unconvertible declarations unless
 * `passThroughUnconvertible` is set
 */
std::vector<uint8_t> rewriteQueueDeclare(const Frame &frame,
                                         std::size_t  maxFrameSize,
                                         bool passThroughUnconvertible = false);

/**
 * \brief Reason carried by a Connection.Close or Channel.Close method
 */
struct CloseReason {
    bool        connection = false;  // Connection.Close, else Channel.Close
    uint16_t    replyCode  = 0;
    std::string replyText;
    uint16_t    classId  = 0;  // failing method, 0 if none
    uint16_t    methodId = 0;
};

/**
 * \brief Decode a Connection.Close or Channel.Close frame for diagnostics.
 * Never throws: returns false for other frames and for malformed ones.
 */
bool decodeCloseReason(CloseReason *reason, const Frame &frame);

}
}
#endif
