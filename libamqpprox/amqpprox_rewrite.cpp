// SPDX-License-Identifier: Apache-2.0
#include <amqpprox_rewrite.h>

#include <amqpprox_frame.h>
#include <amqpprox_logging.h>
#include <amqpprox_reply.h>

#include <utility>

namespace Bloomberg {
namespace amqpprox {
namespace {

const uint8_t PASSIVE     = 1;
const uint8_t DURABLE     = 2;
const uint8_t EXCLUSIVE   = 4;
const uint8_t AUTO_DELETE = 8;

void require(bool ok, const char *message)
{
    if (!ok) {
        throw QueueRewriteError(Reply::Codes::syntax_error, message);
    }
}

// Bounded cursor: never decode/re-encode unrelated argument values. In
// particular floating point values and nested field tables remain byte exact.
class Cursor {
    const uint8_t *d_data;
    std::size_t    d_size;

  public:
    std::size_t pos = 0;

    Cursor(const uint8_t *data, std::size_t size)
    : d_data(data)
    , d_size(size)
    {
    }

    void skip(std::size_t n)
    {
        require(n <= d_size - pos, "Malformed Queue.Declare: truncated field");
        pos += n;
    }

    uint8_t byte()
    {
        skip(1);
        return d_data[pos - 1];
    }

    uint32_t longInt()
    {
        uint32_t n = 0;
        for (int i = 0; i < 4; ++i) {
            n = (n << 8) | byte();
        }
        return n;
    }

    std::string string(std::size_t n)
    {
        const auto start = pos;
        skip(n);
        return std::string(reinterpret_cast<const char *>(d_data + start), n);
    }

    void value(uint8_t type)
    {
        switch (type) {
        case 't':
        case 'b':
        case 'B': skip(1); break;
        case 's':
        case 'u': skip(2); break;
        case 'I':
        case 'i':
        case 'f': skip(4); break;
        case 'l':
        case 'L':
        case 'd':
        case 'T': skip(8); break;
        case 'D': skip(5); break;
        case 'S':
        case 'x':
        case 'A':
        case 'F': skip(longInt()); break;
        case 'V': break;
        default:
            require(false, "Malformed Queue.Declare: unknown field type");
        }
    }
};

// One field-table entry, located by its byte range in the frame payload.
struct Argument {
    std::size_t start;
    std::size_t end;
    std::string key;
    uint8_t     type;
    std::string text;  // value of long-string ('S') entries only
};

// Classic-queue tuning arguments with no quorum meaning. Dropping them does
// not change application-visible semantics; keeping them makes RabbitMQ
// refuse the quorum declaration.
bool isClassicOnlyTuning(const std::string &key)
{
    return key == "x-queue-mode" || key == "x-queue-version" ||
           key == "x-queue-master-locator";
}

// Returns why the declaration cannot become a quorum queue without changing
// its semantics, or an empty string when it can.
std::string unconvertibleReason(const std::string           &name,
                                uint8_t                      flags,
                                const std::vector<Argument> &arguments)
{
    if (name.empty()) {
        return "server-named queues cannot be quorum queues";
    }

    const std::string queue = "queue '" + name + "' ";
    if (!(flags & DURABLE)) {
        return queue + "is not durable; quorum queues are always durable";
    }
    if (flags & EXCLUSIVE) {
        return queue + "is exclusive; quorum queues cannot be exclusive";
    }
    if (flags & AUTO_DELETE) {
        return queue + "is auto-delete; quorum queues cannot be auto-delete";
    }

    for (const auto &argument : arguments) {
        if (argument.key == "x-max-priority") {
            return queue + "uses x-max-priority, which quorum queues do not "
                           "support";
        }
        if (argument.key == "x-overflow" && argument.type == 'S' &&
            argument.text == "reject-publish-dlx") {
            return queue + "uses x-overflow=reject-publish-dlx, which quorum "
                           "queues do not support";
        }
    }

    return {};
}

void put32(std::vector<uint8_t> &out, uint32_t value)
{
    for (int shift = 24; shift >= 0; shift -= 8) {
        out.push_back((value >> shift) & 0xff);
    }
}

}

QueueRewriteError::QueueRewriteError(uint16_t replyCode, const std::string &text)
: std::runtime_error(text)
, d_replyCode(replyCode)
{
}

void RewriteConfig::validate() const
{
    auto valid = [](const std::string &s) {
        return !s.empty() && s.size() <= 255 && s.find('\0') == std::string::npos;
    };
    if (!valid(sourceVhost) || (!targetVhost.empty() && !valid(targetVhost)) ||
        (quorumQueues && targetVhost.empty())) {
        throw std::invalid_argument(
            "Rewriting requires nonempty vhosts of at most 255 bytes; quorum "
            "rewriting requires --rewriteVhostTo");
    }
    if (passThroughUnconvertible && !quorumQueues) {
        throw std::invalid_argument(
            "--passThroughUnconvertibleQueues requires "
            "--rewriteClassicQueuesToQuorum");
    }
}

bool RewriteConfig::matches(const std::string &vhost) const
{
    return !targetVhost.empty() && vhost == sourceVhost;
}

std::vector<uint8_t> rewriteQueueDeclare(const Frame &frame,
                                         std::size_t  maxFrameSize,
                                         bool         passThroughUnconvertible)
{
    if (frame.type != 1 || frame.length < 4) {
        return {};
    }
    const auto *p = static_cast<const uint8_t *>(frame.payload);
    if (p[0] != 0 || p[1] != 50 || p[2] != 0 || p[3] != 10) {
        return {};
    }

    Cursor c(p, frame.length);
    c.skip(6);  // class, method, reserved ticket
    const auto name  = c.string(c.byte());
    const auto flags = c.byte();
    if (flags & PASSIVE) {
        return {};  // passive checks do not create/change queues
    }
    const auto tableLengthPos = c.pos;
    const auto tableLength    = c.longInt();
    const auto tableStart     = c.pos;
    require(tableLength == frame.length - tableStart,
            "Malformed Queue.Declare: field table length mismatch");

    std::vector<Argument> arguments;
    bool                  typeFound = false;
    std::string           queueType;
    while (c.pos < frame.length) {
        Argument argument;
        argument.start = c.pos;
        argument.key   = c.string(c.byte());
        argument.type  = c.byte();
        if (argument.type == 'S') {
            argument.text = c.string(c.longInt());
        }
        else {
            c.value(argument.type);
        }
        argument.end = c.pos;

        if (argument.key == "x-queue-type") {
            require(!typeFound && argument.type == 'S',
                    "Malformed Queue.Declare: duplicate or non-string "
                    "x-queue-type");
            typeFound = true;
            queueType = argument.text;
        }
        arguments.push_back(std::move(argument));
    }

    // Explicit quorum, stream and other types are the application's own
    // choice: the broker validates them unchanged.
    if (typeFound && queueType != "classic") {
        return {};
    }

    const auto reason = unconvertibleReason(name, flags, arguments);
    if (!reason.empty()) {
        if (passThroughUnconvertible) {
            LOG_DEBUG << "Forwarding unconvertible declaration unchanged: "
                      << reason;
            return {};
        }
        throw QueueRewriteError(Reply::Codes::not_implemented,
                                "amqpprox: cannot convert to quorum, " +
                                    reason);
    }

    const std::vector<uint8_t> typeEntry = {12,  'x', '-', 'q', 'u', 'e',
                                            'u', 'e', '-', 't', 'y', 'p',
                                            'e', 'S', 0,   0,   0,   6,
                                            'q', 'u', 'o', 'r', 'u', 'm'};

    // Rebuild the table entry by entry: the queue type is replaced in place
    // (or appended), classic-only tuning is dropped, the rest is copied.
    std::vector<uint8_t> table;
    bool                 typeWritten = false;
    for (const auto &argument : arguments) {
        if (argument.key == "x-queue-type") {
            table.insert(table.end(), typeEntry.begin(), typeEntry.end());
            typeWritten = true;
        }
        else if (isClassicOnlyTuning(argument.key)) {
            LOG_DEBUG << "Dropping classic-only argument " << argument.key
                      << " from queue '" << name << "'";
        }
        else {
            table.insert(table.end(), p + argument.start, p + argument.end);
        }
    }
    if (!typeWritten) {
        table.insert(table.end(), typeEntry.begin(), typeEntry.end());
    }

    const std::size_t newPayloadSize = tableStart + table.size();
    if (newPayloadSize + Frame::frameOverhead() > maxFrameSize) {
        throw QueueRewriteError(
            Reply::Codes::frame_error,
            "amqpprox: rewritten Queue.Declare exceeds negotiated frame-max");
    }

    std::vector<uint8_t> out;
    out.reserve(newPayloadSize + Frame::frameOverhead());
    out.push_back(frame.type);
    out.push_back(static_cast<uint16_t>(frame.channel) >> 8);
    out.push_back(static_cast<uint16_t>(frame.channel) & 0xff);
    put32(out, static_cast<uint32_t>(newPayloadSize));
    out.insert(out.end(), p, p + tableLengthPos);
    put32(out, static_cast<uint32_t>(table.size()));
    out.insert(out.end(), table.begin(), table.end());
    out.push_back(0xce);
    return out;
}

}
}
