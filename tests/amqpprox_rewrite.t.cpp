// SPDX-License-Identifier: Apache-2.0
#include <amqpprox_rewrite.h>

#include <amqpprox_buffer.h>
#include <amqpprox_bufferpool.h>
#include <amqpprox_connector.h>
#include <amqpprox_connectorutil.h>
#include <amqpprox_eventsource.h>
#include <amqpprox_frame.h>
#include <amqpprox_method.h>
#include <amqpprox_methods_close.h>
#include <amqpprox_methods_closeok.h>
#include <amqpprox_packetprocessor.h>
#include <amqpprox_reply.h>
#include <amqpprox_sessionstate.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <stdexcept>

namespace Bloomberg {
namespace amqpprox {
namespace {

using Bytes = std::vector<uint8_t>;

const std::size_t FRAME_MAX = 131072;

void u32(Bytes &b, uint32_t n)
{
    for (int shift = 24; shift >= 0; shift -= 8) {
        b.push_back((n >> shift) & 255);
    }
}

Bytes concat(std::initializer_list<Bytes> parts)
{
    Bytes result;
    for (const auto &part : parts) {
        result.insert(result.end(), part.begin(), part.end());
    }
    return result;
}

Bytes field(const std::string &key, const std::string &value)
{
    Bytes b{static_cast<uint8_t>(key.size())};
    b.insert(b.end(), key.begin(), key.end());
    b.push_back('S');
    u32(b, value.size());
    b.insert(b.end(), value.begin(), value.end());
    return b;
}

Bytes field(const std::string &key, uint8_t type, const Bytes &value)
{
    Bytes b{static_cast<uint8_t>(key.size())};
    b.insert(b.end(), key.begin(), key.end());
    b.push_back(type);
    b.insert(b.end(), value.begin(), value.end());
    return b;
}

Bytes declare(const Bytes       &args  = {},
              uint8_t            flags = 2,
              const std::string &name  = "jobs")
{
    Bytes b{0, 50, 0, 10, 0, 0, static_cast<uint8_t>(name.size())};
    b.insert(b.end(), name.begin(), name.end());
    b.push_back(flags);
    u32(b, args.size());
    b.insert(b.end(), args.begin(), args.end());
    return b;
}

Frame frame(const Bytes &payload, uint16_t channel = 1, uint8_t type = 1)
{
    Frame f;
    f.type    = type;
    f.channel = channel;
    f.length  = payload.size();
    f.payload = payload.data();
    return f;
}

Bytes wire(const Bytes &payload, uint16_t channel = 1, uint8_t type = 1)
{
    auto        f = frame(payload, channel, type);
    Bytes       b(payload.size() + 8);
    std::size_t n = 0;
    if (!Frame::encode(b.data(), &n, f)) {
        throw std::runtime_error("fixture encode");
    }
    return b;
}

Bytes copy(Buffer b)
{
    auto p = static_cast<const uint8_t *>(b.originalPtr());
    return b.size() ? Bytes(p, p + b.size()) : Bytes{};
}

void process(PacketProcessor &p,
             const Bytes     &b,
             FlowType         dir = FlowType::INGRESS)
{
    Buffer buf(b.data(), b.size());
    buf.seek(b.size());
    p.process(dir, buf);
}

void receive(Connector &c, const Bytes &b, FlowType dir)
{
    Method m;
    ASSERT_TRUE(Method::decode(&m, b.data(), b.size()));
    c.receive(m, dir);
}

template <class T>
void receive(Connector &c, const T &m, FlowType dir)
{
    Bytes  b(4096);
    Buffer buf(b.data(), b.size());
    ASSERT_TRUE(Method::encode(buf, m));
    b.resize(buf.offset());
    receive(c, b, dir);
}

methods::Close decodeClose(Buffer out)
{
    const void    *next = out.originalPtr();
    std::size_t    left = out.size();
    Frame          f;
    Method         m;
    methods::Close close;
    EXPECT_TRUE(Frame::decode(&f, &next, &left, next, left));
    EXPECT_TRUE(Method::decode(&m, f.payload, f.length));
    EXPECT_EQ(m.classType, methods::Close::classType());
    EXPECT_EQ(m.methodType, methods::Close::methodType());
    Buffer payload(m.payload, m.length);
    EXPECT_TRUE(methods::Close::decode(&close, payload));
    return close;
}

// Expects `rewriteQueueDeclare` to reject `payload` with `code`, mentioning
// `reason` in the text sent to the client.
void expectRejected(const Bytes       &payload,
                    uint16_t           code,
                    const std::string &reason)
{
    try {
        rewriteQueueDeclare(frame(payload), FRAME_MAX);
        ADD_FAILURE() << "expected rejection: " << reason;
    }
    catch (const QueueRewriteError &error) {
        EXPECT_EQ(error.replyCode(), code);
        EXPECT_NE(std::string(error.what()).find(reason), std::string::npos)
            << error.what();
    }
}

class Harness {
  public:
    BufferPool   pool{{32, 64, 256, 1024, 4096, 16384, 65536, 150000}};
    EventSource  events;
    SessionState state{nullptr};
    Connector    connector;
    Bytes        outgoingHandshake;

    Harness(RewriteConfig cfg      = {"/", "tenant/app", true},
            std::string   vhost    = "/",
            uint32_t      frameMax = FRAME_MAX)
    : connector(&state, &events, &pool, "test", cfg)
    {
        connector.setConnectionCreationHandler([] {});
        connector.receive(Buffer(Constants::protocolHeader(),
                                 Constants::protocolHeaderLength()));
        receive(connector, methods::StartOk(), FlowType::INGRESS);
        Bytes tune{0, 10, 0, 31, 0, 0};
        u32(tune, frameMax);
        tune.push_back(0);
        tune.push_back(60);
        receive(connector, tune, FlowType::INGRESS);
        methods::Open open;
        open.setVirtualHost(vhost);
        receive(connector, open, FlowType::INGRESS);
        receive(
            connector, ConnectorUtil::synthesizedStart(), FlowType::EGRESS);
        receive(connector, ConnectorUtil::synthesizedTune(), FlowType::EGRESS);
        outgoingHandshake = copy(connector.outBuffer());
        receive(connector, methods::OpenOk(), FlowType::EGRESS);
    }
};

}

TEST(Rewrite, MissingAndExplicitClassicProduceQuorum)
{
    for (auto args : {Bytes{}, field("x-queue-type", "classic")}) {
        auto payload = declare(args);
        auto result  = rewriteQueueDeclare(frame(payload, 513), FRAME_MAX);
        EXPECT_EQ(result, wire(declare(field("x-queue-type", "quorum")), 513));
    }
}

TEST(Rewrite, PreservesOtherArgumentsAndFlags)
{
    // TTL, opaque nested table, binary value, and floating point bits.
    Bytes extra{13,  'x', '-', 'm', 'e', 's', 's', 'a', 'g', 'e', '-',
                't', 't', 'l', 'I', 0,   0,   3,   232, 1,   'n', 'F',
                0,   0,   0,   4,   1,   'a', 't', 1,   1,   'b', 'x',
                0,   0,   0,   3,   0,   128, 255, 1,   'f', 'd', 0,
                0,   0,   0,   0,   0,   240, 63};
    auto  args     = concat({field("x-queue-type", "classic"), extra});
    auto  expected = concat({field("x-queue-type", "quorum"), extra});
    auto  payload  = declare(args, 18);  // durable and no-wait
    EXPECT_EQ(rewriteQueueDeclare(frame(payload), FRAME_MAX),
              wire(declare(expected, 18)));
}

TEST(Rewrite, PassiveAndExplicitNonClassicAreUnchanged)
{
    for (auto type : {"quorum", "stream"}) {
        auto payload = declare(field("x-queue-type", type));
        EXPECT_TRUE(rewriteQueueDeclare(frame(payload), FRAME_MAX).empty());
    }
    auto passive = declare(field("x-queue-type", "classic"), 1);
    EXPECT_TRUE(rewriteQueueDeclare(frame(passive), FRAME_MAX).empty());
}

TEST(Rewrite, ExplicitQuorumIsLeftToTheBroker)
{
    // The application asked for quorum itself; the broker validates flags.
    auto exclusive = declare(field("x-queue-type", "quorum"), 6);
    EXPECT_TRUE(rewriteQueueDeclare(frame(exclusive), FRAME_MAX).empty());
}

TEST(Rewrite, UnconvertibleDeclarationsAreRejectedWithReason)
{
    const uint16_t notImplemented = Reply::Codes::not_implemented;
    expectRejected(declare({}, 0), notImplemented, "is not durable");
    expectRejected(declare({}, 6), notImplemented, "is exclusive");
    expectRejected(declare({}, 4), notImplemented, "is not durable");
    expectRejected(declare({}, 10), notImplemented, "is auto-delete");
    expectRejected(
        declare({}, 2, ""), notImplemented, "server-named queues cannot");
    expectRejected(declare(field("x-queue-type", "classic"), 6),
                   notImplemented,
                   "queue 'jobs' is exclusive");
}

TEST(Rewrite, PriorityAndRejectPublishDlxAreUnconvertible)
{
    const uint16_t notImplemented = Reply::Codes::not_implemented;
    expectRejected(declare(field("x-max-priority", 'b', {10})),
                   notImplemented,
                   "x-max-priority");
    expectRejected(declare(field("x-overflow", "reject-publish-dlx")),
                   notImplemented,
                   "reject-publish-dlx");

    // Overflow modes that quorum queues support are converted and kept.
    auto overflow = field("x-overflow", "reject-publish");
    EXPECT_EQ(rewriteQueueDeclare(frame(declare(overflow)), FRAME_MAX),
              wire(declare(concat({overflow, field("x-queue-type", "quorum")}))));
}

TEST(Rewrite, PassThroughForwardsUnconvertibleUnchanged)
{
    for (const auto &payload :
         {declare({}, 0),
          declare({}, 6),
          declare({}, 10),
          declare({}, 2, ""),
          declare(field("x-queue-type", "classic"), 6),
          declare(field("x-max-priority", 'b', {10}))}) {
        EXPECT_TRUE(rewriteQueueDeclare(frame(payload), FRAME_MAX, true)
                        .empty());
    }

    // Convertible declarations are still converted in pass-through mode.
    EXPECT_EQ(rewriteQueueDeclare(frame(declare()), FRAME_MAX, true),
              wire(declare(field("x-queue-type", "quorum"))));
}

TEST(Rewrite, ClassicOnlyTuningArgumentsAreDropped)
{
    auto ttl  = field("x-message-ttl", 'I', {0, 0, 3, 232});
    auto args = concat({field("x-queue-mode", "lazy"),
                        ttl,
                        field("x-queue-type", "classic"),
                        field("x-queue-version", 'b', {2}),
                        field("x-queue-master-locator", "min-masters")});
    auto expected = concat({ttl, field("x-queue-type", "quorum")});
    EXPECT_EQ(rewriteQueueDeclare(frame(declare(args)), FRAME_MAX),
              wire(declare(expected)));

    // Also without an explicit type, where quorum is appended.
    args = concat({field("x-queue-mode", "lazy"), ttl});
    EXPECT_EQ(rewriteQueueDeclare(frame(declare(args)), FRAME_MAX),
              wire(declare(expected)));
}

TEST(Rewrite, MalformedAndDuplicateArgumentsAreRejected)
{
    const uint16_t syntaxError = Reply::Codes::syntax_error;
    auto           p           = declare(field("x-queue-type", "classic"));
    // Every truncated prefix of an active declaration fails once identified.
    for (std::size_t n = 4; n < p.size(); ++n) {
        Bytes cut(p.begin(), p.begin() + n);
        EXPECT_THROW(rewriteQueueDeclare(frame(cut), FRAME_MAX),
                     QueueRewriteError)
            << n;
    }
    auto type = field("x-queue-type", "classic");
    expectRejected(declare(concat({type, type})), syntaxError, "duplicate");
    expectRejected(declare(field("x-queue-type", 'I', {0, 0, 0, 1})),
                   syntaxError,
                   "non-string");
    expectRejected(declare({1, 'a', '?', 0}), syntaxError, "unknown field");
}

TEST(Rewrite, FrameLimitIncludesGrowthAndOverhead)
{
    auto p        = declare();
    auto expected = wire(declare(field("x-queue-type", "quorum")));
    EXPECT_EQ(rewriteQueueDeclare(frame(p), expected.size()), expected);
    try {
        rewriteQueueDeclare(frame(p), expected.size() - 1);
        ADD_FAILURE() << "expected frame-max rejection";
    }
    catch (const QueueRewriteError &error) {
        EXPECT_EQ(error.replyCode(), Reply::Codes::frame_error);
    }
}

TEST(Rewrite, ConfigurationValidation)
{
    EXPECT_NO_THROW(RewriteConfig().validate());
    RewriteConfig c;
    c.quorumQueues = true;
    EXPECT_THROW(c.validate(), std::invalid_argument);
    c.targetVhost = "tenant";
    EXPECT_NO_THROW(c.validate());
    c.passThroughUnconvertible = true;
    EXPECT_NO_THROW(c.validate());
    c.quorumQueues = false;
    EXPECT_THROW(c.validate(), std::invalid_argument);
    c.passThroughUnconvertible = false;
    c.targetVhost              = std::string(256, 'x');
    EXPECT_THROW(c.validate(), std::invalid_argument);
    c.targetVhost = std::string("a\0b", 3);
    EXPECT_THROW(c.validate(), std::invalid_argument);
}

TEST(Rewrite, VhostReachesBrokerAndSessionState)
{
    Harness h;
    ASSERT_EQ(h.connector.state(), Connector::State::OPEN);
    EXPECT_EQ(h.state.getVirtualHost(), "tenant/app");
    EXPECT_TRUE(h.connector.rewriteQueues());
    const void *next = h.outgoingHandshake.data();
    std::size_t left = h.outgoingHandshake.size();
    Frame       f;
    ASSERT_TRUE(Frame::decode(&f, &next, &left, next, left));  // tune-ok
    ASSERT_TRUE(Frame::decode(&f, &next, &left, next, left));  // open
    Method m;
    ASSERT_TRUE(Method::decode(&m, f.payload, f.length));
    methods::Open open;
    Buffer        b(m.payload, m.length);
    ASSERT_TRUE(methods::Open::decode(&open, b));
    EXPECT_EQ(open.virtualHost(), "tenant/app");
}

TEST(Rewrite, DisabledAndNonmatchingConnectionsStayByteExact)
{
    for (auto cfg :
         {RewriteConfig{}, RewriteConfig{"source", "destination", true}}) {
        Harness h(cfg);
        EXPECT_EQ(h.state.getVirtualHost(), "/");
        EXPECT_FALSE(h.connector.rewriteQueues());
        PacketProcessor p(h.state, h.connector);
        auto data = wire(declare(field("x-queue-type", "classic"), 6));
        process(p, data);
        EXPECT_EQ(copy(p.egressWrite()), data);
        EXPECT_FALSE(p.rewrittenStorage());
        EXPECT_FALSE(p.rewriteError());
    }
}

TEST(Rewrite, CustomSourceAndVhostOnlyMode)
{
    Harness h({"VHOST1", "VHOST2", false}, "VHOST1");
    EXPECT_EQ(h.state.getVirtualHost(), "VHOST2");
    EXPECT_FALSE(h.connector.rewriteQueues());
}

TEST(Rewrite, MixedFramesFragmentationAndBufferLifetime)
{
    Harness h;
    auto    unrelated   = wire(Bytes{0, 60, 0, 40});
    auto    declaration = wire(declare());
    auto    heartbeat   = wire({}, 0, 8);
    // Looks like a declaration, but is a body frame.
    auto body     = wire(Bytes{0, 50, 0, 10, 255, 0}, 7, 3);
    auto data     = concat({unrelated, declaration, heartbeat, body});
    auto changed  = wire(declare(field("x-queue-type", "quorum")));
    auto expected = concat({unrelated, changed, heartbeat, body});

    // Every possible TCP split, carrying remainder as Session does.
    for (std::size_t cut = 1; cut < data.size(); ++cut) {
        Bytes           chunk(data.begin(), data.begin() + cut);
        PacketProcessor first(h.state, h.connector);
        process(first, chunk);
        auto output = copy(first.egressWrite());
        auto rest   = copy(first.remaining());
        rest.insert(rest.end(), data.begin() + cut, data.end());
        PacketProcessor second(h.state, h.connector);
        process(second, rest);
        auto tail = copy(second.egressWrite());
        output.insert(output.end(), tail.begin(), tail.end());
        EXPECT_EQ(output, expected) << cut;
        EXPECT_EQ(second.remaining().size(), 0);
    }

    std::shared_ptr<Bytes> storage;
    {
        PacketProcessor p(h.state, h.connector);
        process(p, data);
        storage = p.rewrittenStorage();
    }
    ASSERT_TRUE(storage);
    EXPECT_EQ(*storage, expected);
    PacketProcessor back(h.state, h.connector);
    process(back, data, FlowType::EGRESS);
    EXPECT_EQ(copy(back.ingressWrite()), data);
}

TEST(Rewrite, MultipleDeclarationsInOneRead)
{
    Harness         h;
    auto            one      = wire(declare());
    auto            data     = concat({one, one});
    auto            expected = wire(declare(field("x-queue-type", "quorum")));
    PacketProcessor p(h.state, h.connector);
    process(p, data);
    EXPECT_EQ(copy(p.egressWrite()), concat({expected, expected}));
}

TEST(Rewrite, ProcessorForwardsFramesBeforeRejectedDeclaration)
{
    Harness h;
    auto    unrelated = wire(Bytes{0, 60, 0, 40});
    auto    converted = wire(declare());
    auto    rejected  = wire(declare({}, 6, "reply"));
    auto    heartbeat = wire({}, 0, 8);
    auto    quorum    = wire(declare(field("x-queue-type", "quorum")));

    // Untouched frames before the rejection are forwarded as read.
    {
        auto            data = concat({unrelated, rejected, heartbeat});
        PacketProcessor p(h.state, h.connector);
        process(p, data);
        ASSERT_TRUE(p.rewriteError());
        EXPECT_EQ(p.rewriteError()->replyCode(),
                  Reply::Codes::not_implemented);
        EXPECT_EQ(copy(p.egressWrite()), unrelated);
        EXPECT_EQ(p.remaining().size(), 0);
    }

    // Rewritten frames before the rejection are forwarded rewritten.
    {
        auto data = concat({unrelated, converted, rejected, heartbeat});
        PacketProcessor p(h.state, h.connector);
        process(p, data);
        ASSERT_TRUE(p.rewriteError());
        EXPECT_EQ(copy(p.egressWrite()), concat({unrelated, quorum}));
        EXPECT_EQ(p.remaining().size(), 0);
    }

    // A rejected first frame forwards nothing.
    {
        auto            data = concat({rejected, unrelated});
        PacketProcessor p(h.state, h.connector);
        process(p, data);
        ASSERT_TRUE(p.rewriteError());
        EXPECT_EQ(p.egressWrite().size(), 0);
    }
}

TEST(Rewrite, PassThroughConfigurationReachesProcessor)
{
    RewriteConfig cfg{"/", "tenant/app", true};
    cfg.passThroughUnconvertible = true;
    Harness h(cfg);
    EXPECT_TRUE(h.connector.passThroughUnconvertibleQueues());

    auto            rejected = wire(declare({}, 6, "reply"));
    auto            data     = concat({rejected, wire(declare())});
    PacketProcessor p(h.state, h.connector);
    process(p, data);
    EXPECT_FALSE(p.rewriteError());
    EXPECT_EQ(
        copy(p.egressWrite()),
        concat({rejected, wire(declare(field("x-queue-type", "quorum")))}));
}

TEST(Rewrite, NegotiatedLimitEnforcedInProcessor)
{
    Harness h({"/", "tenant", true}, "/", 4096);
    auto    payload = declare(field("padding", std::string(4050, 'x')));
    ASSERT_LT(wire(payload).size(), 4096u);
    PacketProcessor p(h.state, h.connector);
    process(p, wire(payload));
    ASSERT_TRUE(p.rewriteError());
    EXPECT_EQ(p.rewriteError()->replyCode(), Reply::Codes::frame_error);
    EXPECT_EQ(p.egressWrite().size(), 0);
}

TEST(Rewrite, CloseWithReasonStartsGracefulClose)
{
    Harness h;
    h.connector.synthesizeCloseWithReason(
        true,
        Reply::Codes::not_implemented,
        "amqpprox: cannot convert to quorum, queue 'reply' is exclusive",
        QueueRewriteError::classId(),
        QueueRewriteError::methodId());
    EXPECT_EQ(h.connector.state(), Connector::State::CLIENT_CLOSE_SENT);
    EXPECT_TRUE(h.connector.sendToIngressSide());
    auto close = decodeClose(h.connector.outBuffer());
    EXPECT_EQ(close.replyCode(), Reply::Codes::not_implemented);
    EXPECT_EQ(close.replyString(),
              "amqpprox: cannot convert to quorum, queue 'reply' is exclusive");
    EXPECT_EQ(close.classId(), 50);
    EXPECT_EQ(close.methodId(), 10);

    // The client's CloseOk closes the broker side normally.
    receive(h.connector, methods::CloseOk(), FlowType::INGRESS);
    EXPECT_EQ(h.connector.state(), Connector::State::SERVER_CLOSE_SENT);
    EXPECT_FALSE(h.connector.sendToIngressSide());
    EXPECT_EQ(decodeClose(h.connector.outBuffer()).replyCode(),
              Reply::Codes::reply_success);
    receive(h.connector, methods::CloseOk(), FlowType::EGRESS);
    EXPECT_EQ(h.connector.state(), Connector::State::CLOSED);
}

TEST(Rewrite, CloseReasonIsTruncatedToShortString)
{
    Harness h;
    h.connector.synthesizeCloseWithReason(
        true, Reply::Codes::not_implemented, std::string(300, 'x'));
    EXPECT_EQ(decodeClose(h.connector.outBuffer()).replyString(),
              std::string(255, 'x'));
}

}
}
