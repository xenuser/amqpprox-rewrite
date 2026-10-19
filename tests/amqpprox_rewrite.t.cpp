// SPDX-License-Identifier: Apache-2.0
#include <amqpprox_rewrite.h>
#include <amqpprox_buffer.h>
#include <amqpprox_bufferpool.h>
#include <amqpprox_connector.h>
#include <amqpprox_connectorutil.h>
#include <amqpprox_eventsource.h>
#include <amqpprox_frame.h>
#include <amqpprox_method.h>
#include <amqpprox_packetprocessor.h>
#include <amqpprox_sessionstate.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <stdexcept>

namespace Bloomberg {
namespace amqpprox {
namespace {
using Bytes = std::vector<uint8_t>;
void u32(Bytes &b, uint32_t n) {
    for (int shift = 24; shift >= 0; shift -= 8) b.push_back((n >> shift) & 255);
}
Bytes field(const std::string &key, const std::string &value) {
    Bytes b{static_cast<uint8_t>(key.size())};
    b.insert(b.end(), key.begin(), key.end()); b.push_back('S');
    u32(b, value.size()); b.insert(b.end(), value.begin(), value.end());
    return b;
}
Bytes declare(const Bytes &args = {}, uint8_t flags = 2, const std::string &name = "jobs") {
    Bytes b{0,50,0,10,0,0,static_cast<uint8_t>(name.size())};
    b.insert(b.end(), name.begin(), name.end()); b.push_back(flags);
    u32(b, args.size()); b.insert(b.end(), args.begin(), args.end()); return b;
}
Frame frame(const Bytes &payload, uint16_t channel = 1, uint8_t type = 1) {
    Frame f; f.type=type; f.channel=channel; f.length=payload.size(); f.payload=payload.data(); return f;
}
Bytes wire(const Bytes &payload, uint16_t channel = 1, uint8_t type = 1) {
    auto f=frame(payload,channel,type); Bytes b(payload.size()+8); std::size_t n=0;
    if (!Frame::encode(b.data(),&n,f)) throw std::runtime_error("fixture encode"); return b;
}
Bytes copy(Buffer b) {
    auto p=static_cast<const uint8_t *>(b.originalPtr()); return b.size() ? Bytes(p,p+b.size()) : Bytes{};
}
void process(PacketProcessor &p, const Bytes &b, FlowType dir=FlowType::INGRESS) {
    Buffer buf(b.data(),b.size()); buf.seek(b.size()); p.process(dir,buf);
}
void receive(Connector &c, const Bytes &b, FlowType dir) {
    Method m; ASSERT_TRUE(Method::decode(&m,b.data(),b.size())); c.receive(m,dir);
}
template<class T> void receive(Connector &c, const T &m, FlowType dir) {
    Bytes b(4096); Buffer buf(b.data(),b.size()); ASSERT_TRUE(Method::encode(buf,m));
    b.resize(buf.offset()); receive(c,b,dir);
}
class Harness {
  public:
    BufferPool pool{{32,64,256,1024,4096,16384,65536,150000}};
    EventSource events;
    SessionState state{nullptr};
    Connector connector;
    Bytes outgoingHandshake;
    Harness(RewriteConfig cfg = {"/", "tenant/app", true}, std::string vhost="/", uint32_t frameMax=131072)
      : connector(&state,&events,&pool,"test",cfg) {
        connector.setConnectionCreationHandler([]{});
        connector.receive(Buffer(Constants::protocolHeader(),Constants::protocolHeaderLength()));
        receive(connector,methods::StartOk(),FlowType::INGRESS);
        Bytes tune{0,10,0,31,0,0}; u32(tune,frameMax); tune.push_back(0); tune.push_back(60);
        receive(connector,tune,FlowType::INGRESS);
        methods::Open open; open.setVirtualHost(vhost); receive(connector,open,FlowType::INGRESS);
        receive(connector,ConnectorUtil::synthesizedStart(),FlowType::EGRESS);
        receive(connector,ConnectorUtil::synthesizedTune(),FlowType::EGRESS);
        outgoingHandshake=copy(connector.outBuffer());
        receive(connector,methods::OpenOk(),FlowType::EGRESS);
    }
};
}

TEST(Rewrite, MissingAndExplicitClassicProduceQuorum) {
    for (auto args : {Bytes{},field("x-queue-type","classic")}) {
        auto payload=declare(args); auto result=rewriteQueueDeclare(frame(payload,513),131072);
        EXPECT_EQ(result,wire(declare(field("x-queue-type","quorum")),513));
    }
}
TEST(Rewrite, PreservesOtherArgumentsAndFlags) {
    // TTL, opaque nested table, binary value, and floating point bits.
    Bytes extra{13,'x','-','m','e','s','s','a','g','e','-','t','t','l','I',0,0,3,232,
                1,'n','F',0,0,0,4,1,'a','t',1,
                1,'b','x',0,0,0,3,0,128,255,
                1,'f','d',0,0,0,0,0,0,240,63};
    auto args=field("x-queue-type","classic"); args.insert(args.end(),extra.begin(),extra.end());
    auto expected=field("x-queue-type","quorum"); expected.insert(expected.end(),extra.begin(),extra.end());
    auto payload=declare(args,18); // durable and no-wait
    EXPECT_EQ(rewriteQueueDeclare(frame(payload),131072),wire(declare(expected,18)));
}
TEST(Rewrite, PassiveQuorumAndStreamAreUnchanged) {
    for (auto type : {"quorum","stream"}) {
        auto payload=declare(field("x-queue-type",type));
        EXPECT_TRUE(rewriteQueueDeclare(frame(payload),131072).empty());
    }
    auto passive=declare(field("x-queue-type","classic"),1);
    EXPECT_TRUE(rewriteQueueDeclare(frame(passive),131072).empty());
}
TEST(Rewrite, RejectsIncompatibleFlagsAndAnonymousQueues) {
    for (uint8_t flags : {0,4,6,10}) {
        auto p=declare({},flags); EXPECT_THROW(rewriteQueueDeclare(frame(p),131072),std::runtime_error);
    }
    auto p=declare({},2,""); EXPECT_THROW(rewriteQueueDeclare(frame(p),131072),std::runtime_error);
}
TEST(Rewrite, MalformedAndDuplicateArgumentsAreRejected) {
    auto p=declare(field("x-queue-type","classic"));
    // Every truncated prefix of an active declaration fails once identified.
    for (std::size_t n=4;n<p.size();++n) {
        Bytes cut(p.begin(),p.begin()+n);
        EXPECT_THROW(rewriteQueueDeclare(frame(cut),131072),std::runtime_error) << n;
    }
    auto args=field("x-queue-type","classic"); auto duplicate=args;
    args.insert(args.end(),duplicate.begin(),duplicate.end()); p=declare(args);
    EXPECT_THROW(rewriteQueueDeclare(frame(p),131072),std::runtime_error);
    args={12,'x','-','q','u','e','u','e','-','t','y','p','e','I',0,0,0,1}; p=declare(args);
    EXPECT_THROW(rewriteQueueDeclare(frame(p),131072),std::runtime_error);
    p=declare({1,'a','?',0}); EXPECT_THROW(rewriteQueueDeclare(frame(p),131072),std::runtime_error);
}
TEST(Rewrite, FrameLimitIncludesGrowthAndOverhead) {
    auto p=declare(); auto expected=wire(declare(field("x-queue-type","quorum")));
    EXPECT_EQ(rewriteQueueDeclare(frame(p),expected.size()),expected);
    EXPECT_THROW(rewriteQueueDeclare(frame(p),expected.size()-1),std::runtime_error);
}
TEST(Rewrite, ConfigurationValidation) {
    EXPECT_NO_THROW(RewriteConfig().validate());
    RewriteConfig c; c.quorumQueues=true; EXPECT_THROW(c.validate(),std::invalid_argument);
    c.targetVhost="tenant"; EXPECT_NO_THROW(c.validate());
    c.targetVhost=std::string(256,'x'); EXPECT_THROW(c.validate(),std::invalid_argument);
    c.targetVhost=std::string("a\0b",3); EXPECT_THROW(c.validate(),std::invalid_argument);
}
TEST(Rewrite, VhostReachesBrokerAndSessionState) {
    Harness h; ASSERT_EQ(h.connector.state(),Connector::State::OPEN);
    EXPECT_EQ(h.state.getVirtualHost(),"tenant/app"); EXPECT_TRUE(h.connector.rewriteQueues());
    const void *next=h.outgoingHandshake.data(); std::size_t left=h.outgoingHandshake.size(); Frame f;
    ASSERT_TRUE(Frame::decode(&f,&next,&left,next,left)); // tune-ok
    ASSERT_TRUE(Frame::decode(&f,&next,&left,next,left)); // open
    Method m; ASSERT_TRUE(Method::decode(&m,f.payload,f.length));
    methods::Open open; Buffer b(m.payload,m.length); ASSERT_TRUE(methods::Open::decode(&open,b));
    EXPECT_EQ(open.virtualHost(),"tenant/app");
}
TEST(Rewrite, DisabledAndNonmatchingConnectionsStayByteExact) {
    for (auto cfg : {RewriteConfig{},RewriteConfig{"source","destination",true}}) {
        Harness h(cfg); EXPECT_EQ(h.state.getVirtualHost(),"/"); EXPECT_FALSE(h.connector.rewriteQueues());
        PacketProcessor p(h.state,h.connector); auto data=wire(declare(field("x-queue-type","classic")));
        process(p,data); EXPECT_EQ(copy(p.egressWrite()),data); EXPECT_FALSE(p.rewrittenStorage());
    }
}
TEST(Rewrite, CustomSourceAndVhostOnlyMode) {
    Harness h({"VHOST1","VHOST2",false},"VHOST1");
    EXPECT_EQ(h.state.getVirtualHost(),"VHOST2"); EXPECT_FALSE(h.connector.rewriteQueues());
}
TEST(Rewrite, MixedFramesFragmentationAndBufferLifetime) {
    Harness h; auto data=wire(Bytes{0,60,0,40}); // unrelated method
    auto declaration=wire(declare()); auto heartbeat=wire({},0,8);
    auto body=wire(Bytes{0,50,0,10,255,0},7,3); // looks like declaration, but body frame
    data.insert(data.end(),declaration.begin(),declaration.end());
    data.insert(data.end(),heartbeat.begin(),heartbeat.end()); data.insert(data.end(),body.begin(),body.end());
    auto expected=wire(Bytes{0,60,0,40}); auto changed=wire(declare(field("x-queue-type","quorum")));
    expected.insert(expected.end(),changed.begin(),changed.end());
    expected.insert(expected.end(),heartbeat.begin(),heartbeat.end()); expected.insert(expected.end(),body.begin(),body.end());
    // Every possible TCP split, carrying remainder as Session does.
    for (std::size_t cut=1;cut<data.size();++cut) {
        Bytes chunk(data.begin(),data.begin()+cut);
        PacketProcessor first(h.state,h.connector); process(first,chunk);
        auto output=copy(first.egressWrite()); auto rest=copy(first.remaining());
        rest.insert(rest.end(),data.begin()+cut,data.end());
        PacketProcessor second(h.state,h.connector); process(second,rest);
        auto tail=copy(second.egressWrite()); output.insert(output.end(),tail.begin(),tail.end());
        EXPECT_EQ(output,expected) << cut; EXPECT_EQ(second.remaining().size(),0);
    }
    std::shared_ptr<Bytes> storage;
    { PacketProcessor p(h.state,h.connector); process(p,data); storage=p.rewrittenStorage(); }
    ASSERT_TRUE(storage); EXPECT_EQ(*storage,expected);
    PacketProcessor back(h.state,h.connector); process(back,data,FlowType::EGRESS);
    EXPECT_EQ(copy(back.ingressWrite()),data);
}
TEST(Rewrite, MultipleDeclarationsInOneRead) {
    Harness h; auto one=wire(declare()); auto data=one; data.insert(data.end(),one.begin(),one.end());
    auto expected=wire(declare(field("x-queue-type","quorum"))); auto both=expected;
    both.insert(both.end(),expected.begin(),expected.end());
    PacketProcessor p(h.state,h.connector); process(p,data); EXPECT_EQ(copy(p.egressWrite()),both);
}
TEST(Rewrite, NegotiatedLimitEnforcedInProcessor) {
    Harness h({"/","tenant",true},"/",4096);
    auto payload=declare(field("padding",std::string(4050,'x')));
    ASSERT_LT(wire(payload).size(),4096u);
    PacketProcessor p(h.state,h.connector);
    EXPECT_THROW(process(p,wire(payload)),std::runtime_error);
}
}
}
