# Adapted from buildfiles/conan by the amqpprox-rewrite fork on 2026-10-19; see MODIFICATIONS.md.
set(AMQPPROX_TEST_LIBS GTest::gtest GTest::gtest_main Boost::log Boost::system Boost::thread OpenSSL::SSL OpenSSL::Crypto ${CMAKE_THREAD_LIBS_INIT})
