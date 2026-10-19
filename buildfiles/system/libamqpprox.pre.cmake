# Adapted from buildfiles/conan by the amqpprox-rewrite fork on 2026-10-19; see MODIFICATIONS.md.
set(LIBAMQPPROX_LIBS Boost::log Boost::log_setup Boost::system Boost::thread Boost::timer OpenSSL::SSL OpenSSL::Crypto ${CMAKE_THREAD_LIBS_INIT})
