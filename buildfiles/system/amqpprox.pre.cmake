# Adapted from buildfiles/conan by the amqpprox-rewrite fork on 2026-10-19; see MODIFICATIONS.md.
set(AMQPPROX_LIBS Boost::program_options OpenSSL::SSL OpenSSL::Crypto ${CMAKE_THREAD_LIBS_INIT})
