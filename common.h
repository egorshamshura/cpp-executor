#pragma once

#include <mutex>

#include <catch2/catch_test_macros.hpp>

namespace catch_mt_impl {

inline std::mutex mutex;

}  // namespace catch_mt_impl

#define COMMON_MT(macro, ...)                        \
    do {                                             \
        std::lock_guard guard{catch_mt_impl::mutex}; \
        macro(__VA_ARGS__);                          \
    } while (false);

#define CHECK_MT(...) COMMON_MT(CHECK, __VA_ARGS__)
#define CHECK_FALSE_MT(...) COMMON_MT(CHECK_FALSE, __VA_ARGS__)

#define CHECK_THAT_MT(...) COMMON_MT(CHECK_THAT, __VA_ARGS__)

#define FAIL_MT(...) COMMON_MT(FAIL, __VA_ARGS__)
#define FAIL_CHECK_MT(...) COMMON_MT(FAIL_CHECK, __VA_ARGS__)

#define CHECK_THROWS_AS_MT(...) COMMON_MT(CHECK_THROWS_AS, __VA_ARGS__)
#define CHECK_THROWS_MATCHES_MT(...) COMMON_MT(CHECK_THROWS_MATCHES, __VA_ARGS__)
