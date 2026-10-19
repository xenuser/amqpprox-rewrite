// SPDX-License-Identifier: Apache-2.0
#include <amqpprox_rewrite.h>
#include <amqpprox_frame.h>

#include <algorithm>
#include <stdexcept>

namespace Bloomberg {
namespace amqpprox {
namespace {

void require(bool ok, const char *message)
{
    if (!ok) throw std::runtime_error(message);
}

// Bounded cursor: never decode/re-encode unrelated argument values. In
// particular floating point values and nested field tables remain byte exact.
class Cursor {
    const uint8_t *d_data;
    std::size_t d_size;
  public:
    std::size_t pos = 0;
    Cursor(const uint8_t *data, std::size_t size) : d_data(data), d_size(size) {}
    void skip(std::size_t n) {
        require(n <= d_size - pos, "Malformed Queue.Declare: truncated field");
        pos += n;
    }
    uint8_t byte() { skip(1); return d_data[pos - 1]; }
    uint32_t longInt() {
        uint32_t n = 0;
        for (int i = 0; i < 4; ++i) n = (n << 8) | byte();
        return n;
    }
    std::string string(std::size_t n) {
        const auto start = pos;
        skip(n);
        return std::string(reinterpret_cast<const char *>(d_data + start), n);
    }
    void value(uint8_t type) {
        switch (type) {
        case 't': case 'b': case 'B': skip(1); break;
        case 's': case 'u': skip(2); break;
        case 'I': case 'i': case 'f': skip(4); break;
        case 'l': case 'L': case 'd': case 'T': skip(8); break;
        case 'D': skip(5); break;
        case 'S': case 'x': case 'A': case 'F': { auto n = longInt(); skip(n); break; }
        case 'V': break;
        default: throw std::runtime_error("Malformed Queue.Declare: unknown field type");
        }
    }
};

void put32(std::vector<uint8_t> &out, std::size_t pos, uint32_t value)
{
    for (int i = 3; i >= 0; --i) {
        out[pos + i] = value & 0xff;
        value >>= 8;
    }
}
}

void RewriteConfig::validate() const
{
    auto valid = [](const std::string &s) {
        return !s.empty() && s.size() <= 255 && s.find('\0') == std::string::npos;
    };
    if (!valid(sourceVhost) || (!targetVhost.empty() && !valid(targetVhost)) ||
        (quorumQueues && targetVhost.empty())) {
        throw std::invalid_argument("Rewriting requires nonempty vhosts of at most 255 bytes; quorum rewriting requires --rewriteVhostTo");
    }
}

bool RewriteConfig::matches(const std::string &vhost) const
{
    return !targetVhost.empty() && vhost == sourceVhost;
}

std::vector<uint8_t> rewriteQueueDeclare(const Frame &frame,
                                         std::size_t maxFrameSize)
{
    if (frame.type != 1 || frame.length < 4) return {};
    const auto *p = static_cast<const uint8_t *>(frame.payload);
    if (p[0] != 0 || p[1] != 50 || p[2] != 0 || p[3] != 10) return {};

    Cursor c(p, frame.length);
    c.skip(6); // class, method, reserved ticket
    auto name = c.string(c.byte());
    const auto flags = c.byte();
    if (flags & 1) return {}; // passive checks do not create/change queues
    const auto tableLengthPos = c.pos;
    const auto tableLength = c.longInt();
    const auto tableStart = c.pos;
    require(tableLength == frame.length - tableStart,
            "Malformed Queue.Declare: field table length mismatch");
    std::size_t typeStart = c.pos, typeEnd = c.pos;
    bool found = false;
    std::string queueType;
    while (c.pos < frame.length) {
        const auto start = c.pos;
        auto key = c.string(c.byte());
        auto type = c.byte();
        if (key == "x-queue-type") {
            require(!found && type == 'S', "Malformed Queue.Declare: duplicate or non-string x-queue-type");
            found = true;
            queueType = c.string(c.longInt());
            typeStart = start;
            typeEnd = c.pos;
        }
        else c.value(type);
    }
    // Explicit stream/other types are outside the migration policy.
    if (found && queueType != "classic" && queueType != "quorum") return {};
    require(!name.empty() && (flags & 2) && !(flags & (4 | 8)),
            "Quorum rewrite requires a named, durable, non-exclusive, non-auto-delete queue");
    if (queueType == "quorum") return {};

    const std::vector<uint8_t> entry = {
        12, 'x','-','q','u','e','u','e','-','t','y','p','e',
        'S', 0,0,0,6, 'q','u','o','r','u','m'};
    if (!found) typeStart = typeEnd = frame.length;
    const std::size_t newPayloadSize = frame.length - (typeEnd - typeStart) + entry.size();
    require(newPayloadSize + Frame::frameOverhead() <= maxFrameSize,
            "Rewritten Queue.Declare exceeds negotiated frame-max");
    std::vector<uint8_t> out(Frame::frameHeaderSize(), 0);
    out[0] = frame.type;
    out[1] = static_cast<uint16_t>(frame.channel) >> 8;
    out[2] = static_cast<uint16_t>(frame.channel) & 0xff;
    put32(out, 3, static_cast<uint32_t>(newPayloadSize));
    out.insert(out.end(), p, p + typeStart);
    out.insert(out.end(), entry.begin(), entry.end());
    out.insert(out.end(), p + typeEnd, p + frame.length);
    put32(out, Frame::frameHeaderSize() + tableLengthPos,
          static_cast<uint32_t>(newPayloadSize - tableStart));
    out.push_back(0xce);
    return out;
}
}
}
