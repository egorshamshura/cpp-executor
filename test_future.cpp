#include "executor/executor.h"
#include "common.h"

#include <thread>
#include <chrono>
#include <atomic>
#include <ranges>
#include <numeric>
#include <random>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/matchers/catch_matchers_exception.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>
#include <catch2/matchers/catch_matchers_range_equals.hpp>

using namespace std::chrono_literals;
using Catch::Matchers::RangeEquals;

namespace {

auto Now() {
    return std::chrono::steady_clock::now();
}

auto SysNow() {
    return std::chrono::system_clock::now();
}

#define TEST(name) TEMPLATE_TEST_CASE_SIG(name, "", ((uint32_t N), N), 2, 8, 32)

}  // namespace

TEST("InvokeVoid") {
    auto pool = MakeThreadPoolExecutor(N);
    auto x = 0;
    auto future = pool->Invoke<Unit>([&] {
        x = 42;
        return Unit{};
    });

    future->Get();
    CHECK_MT(x == 42);
}

TEST("InvokeString") {
    auto pool = MakeThreadPoolExecutor(N);
    auto future = pool->Invoke<std::string>([] { return "Hello World"; });
    STATIC_REQUIRE(std::is_same_v<decltype(future->Get()), std::string>);

    CHECK_MT(future->Get() == std::string{"Hello World"});
}

TEST("InvokeException") {
    static constexpr auto kMessage = "Test";

    auto pool = MakeThreadPoolExecutor(N);
    auto future = pool->Invoke<Unit>([]() -> Unit { throw std::logic_error{kMessage}; });

    CHECK_THROWS_MATCHES_MT(future->Get(), std::logic_error, Catch::Matchers::Message(kMessage));
}

TEST("Then") {
    auto pool = MakeThreadPoolExecutor(N);
    auto future_a = pool->Invoke<std::string>([] { return "Foo Bar"; });

    auto future_b = pool->Then<Unit>(future_a, [future_a] {
        CHECK_MT(future_a->IsFinished());
        CHECK_MT(future_a->Get() == "Foo Bar");
        return Unit{};
    });

    future_b->Get();
    CHECK_MT(future_b->IsFinished());
}

TEST("ThenCanCancel") {
    auto pool = MakeThreadPoolExecutor(N);
    auto future = pool->Invoke<int>([&] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto res_future = pool->Then<Unit>(std::move(future), [] {
        FAIL_MT("Future was started");
        return Unit{};
    });
    std::this_thread::sleep_for(50ms);
    res_future->Cancel();
    CHECK_MT(res_future->IsCanceled());
}

TEST("ThenIsNonBlocking") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = Now();

    auto future_a = pool->Invoke<std::string>([] {
        std::this_thread::sleep_for(100ms);
        return "Foo Bar";
    });

    auto future_b = pool->Then<Unit>(future_a, [] {
        FAIL_CHECK_MT("Task was started");
        return Unit{};
    });

    std::this_thread::sleep_for(10ms);
    auto duration = Now() - start;
    CHECK_MT(duration < 50ms);
}

TEST("ThenChain") {
    static constexpr auto kN = 100;

    auto pool = MakeThreadPoolExecutor(N);
    std::vector futures = {pool->Invoke<int>([] { return 1; })};
    for (auto i = 1; i < kN; ++i) {
        auto prev = futures.back();
        futures.push_back(pool->Then<int>(prev, [i, prev] {
            CHECK_MT(prev->IsFinished());
            auto result = prev->Get();
            CHECK_MT(result == i);
            return result + 1;
        }));
    }

    auto result = futures.back()->Get();
    CHECK_MT(result == kN);
}

TEST("WhenAll") {
    static constexpr auto kRange = std::views::iota(0ul, 100ul);

    auto pool = MakeThreadPoolExecutor(N);
    auto start = Now();
    std::atomic<size_t> count = 0;

    std::vector<FuturePtr<size_t>> all;
    for (auto i : kRange) {
        all.emplace_back(pool->Invoke<size_t>([&count, i] {
            std::this_thread::sleep_for(1ms);
            count++;
            return i;
        }));
    }

    auto results = pool->WhenAll(all)->Get();
    auto duration = Now() - start;

    CHECK_MT(count.load() == kRange.size());
    CHECK_MT(std::ranges::equal(results, kRange));
    CHECK_MT(duration < 100ms);
}

TEST("WhenAllCanCancel") {
    auto pool = MakeThreadPoolExecutor(N);
    auto future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto res_future = pool->WhenAll(std::vector{std::move(future)});
    std::this_thread::sleep_for(50ms);
    res_future->Cancel();
    CHECK_MT(res_future->IsCanceled());
}

TEST("WhenAllCanCancelManyFutures") {
    auto pool = MakeThreadPoolExecutor(N);
    std::vector<std::shared_ptr<Future<int>>> futures;
    for (auto i = 0; i < 100'000; ++i) {
        futures.push_back(pool->Invoke<int>([] {
            std::this_thread::sleep_for(100ms);
            return 1;
        }));
    }
    auto res_future = pool->WhenAll(std::move(futures));
    std::this_thread::sleep_for(50ms);
    res_future->Cancel();
    CHECK_MT(res_future->IsCanceled());
}

TEST("WhenAllBeforeEnd") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = Now();

    auto future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto res_future = pool->WhenAll(std::vector{std::move(future)});

    auto result = res_future->Get();
    auto duration = Now() - start;
    CHECK_THAT_MT(result, Catch::Matchers::Equals(std::vector{1}));
    CHECK_MT(duration > 50ms);
}

TEST("WhenAllWithThen") {
    auto pool = MakeThreadPoolExecutor(N);
    auto future = pool->WhenAll(std::vector{pool->Invoke<int>([] { return 1; }),
                                            pool->Invoke<int>([] { return 2; }),
                                            pool->Invoke<int>([] { return 3; })});

    auto sum_future = pool->Then<int>(future, [future] {
        CHECK_MT(future->IsFinished());
        auto results = future->Get();
        CHECK_MT(results.size() == 3);
        auto sum = std::reduce(results.begin(), results.end());
        CHECK_MT(sum == 6);
        return sum;
    });

    auto second_future = pool->WhenAll(std::vector{pool->Invoke<int>([] { return 4; }), sum_future,
                                                   pool->Invoke<int>([] { return 5; })});
    auto results = second_future->Get();
    CHECK_MT(results == std::vector{4, 6, 5});
}

TEST("WhenFirst") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = Now();

    auto first_future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto second_future = pool->Invoke<int>([] { return 2; });
    auto res_future = pool->WhenFirst(std::vector{first_future, second_future});

    auto result = res_future->Get();
    auto duration = Now() - start;
    CHECK_MT(result == 2);
    CHECK_MT(duration < 50ms);
}

TEST("WhenFirstCanCancel") {
    auto pool = MakeThreadPoolExecutor(N);
    auto future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto res_future = pool->WhenFirst(std::vector{std::move(future)});
    std::this_thread::sleep_for(50ms);
    res_future->Cancel();
    CHECK_MT(res_future->IsCanceled());
}

TEST("WhenFirstBeforeEnd") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = Now();

    auto future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto res_future = pool->WhenFirst(std::vector{std::move(future)});

    auto result = res_future->Get();
    auto duration = Now() - start;
    CHECK_MT(result == 1);
    CHECK_MT(duration > 50ms);
}

TEST("WhenAllWhenFirst") {
    static constexpr auto kRange = std::views::iota(0, 100);

    auto pool = MakeThreadPoolExecutor(N);
    std::vector<std::vector<std::shared_ptr<Future<int>>>> futures(kRange.size());
    std::vector<int> indexes;
    for (auto i : kRange) {
        for (auto j = 0; j < 10; ++j) {
            indexes.push_back(i);
        }
    }
    std::mt19937 gen{42};
    std::ranges::shuffle(indexes, gen);

    for (auto i : indexes) {
        futures[i].push_back(pool->Invoke<int>([i] { return i; }));
    }
    std::ranges::shuffle(futures, gen);

    std::vector<std::shared_ptr<Future<int>>> when_all;
    for (auto& when_first : futures) {
        when_all.push_back(pool->WhenFirst(std::move(when_first)));
    }

    auto result = pool->WhenAll(std::move(when_all))->Get();
    std::ranges::sort(result);
    CHECK_THAT(result, RangeEquals(kRange));
}

TEST("WhenFirstWhenAll") {
    static constexpr auto kRange = std::views::iota(0, 100);
    static constexpr auto kNumElements = 10;

    auto pool = MakeThreadPoolExecutor(N);
    std::vector<std::vector<std::shared_ptr<Future<int>>>> futures(kRange.size());
    std::vector<int> indexes;
    for (auto i : kRange) {
        for (auto j = 0; j < kNumElements; ++j) {
            indexes.push_back(i);
        }
    }
    std::mt19937 gen{42};
    std::ranges::shuffle(indexes, gen);

    for (auto i : indexes) {
        futures[i].push_back(pool->Invoke<int>([i] { return i; }));
    }
    std::ranges::shuffle(futures, gen);

    std::vector<std::shared_ptr<Future<std::vector<int>>>> when_first;
    for (auto& when_all : futures) {
        when_first.push_back(pool->WhenAll(std::move(when_all)));
    }

    auto result = pool->WhenFirst(std::move(when_first))->Get();
    REQUIRE(result.size() == kNumElements);
    REQUIRE(std::ranges::count(result, result.front()) == kNumElements);
}

TEST("WhenFirstWithThen") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = Now();

    auto first_future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(100ms);
        return 1;
    });
    auto second_future = pool->Invoke<int>([] {
        std::this_thread::sleep_for(50ms);
        return 2;
    });

    auto future = pool->WhenFirst(std::vector{first_future, second_future});
    auto res_future = pool->Then<int>(future, [future, first_future, second_future] {
        CHECK_FALSE_MT(first_future->IsFinished());
        CHECK_MT(second_future->IsFinished());
        CHECK_MT(future->IsFinished());
        return future->Get();
    });

    auto result = res_future->Get();
    auto duration = Now() - start;
    CHECK_MT(result == 2);
    CHECK_MT(duration > 45ms);
    CHECK_MT(duration < 75ms);
}

TEST("WhenAllBeforeDeadline") {
    STATIC_CHECK(N > 1);
    static constexpr auto kN = N - 1;

    auto pool = MakeThreadPoolExecutor(N);
    auto start = SysNow();

    std::vector<FuturePtr<int>> all;
    for (auto i = 0u; i < kN; i++) {
        all.push_back(pool->Invoke<int>([i] { return i; }));
    }

    auto slow_future = pool->Invoke<Unit>([] {
        std::this_thread::sleep_for(100ms);
        return Unit{};
    });

    for (auto i = 0u; i < kN; i++) {
        all.push_back(pool->Then<int>(slow_future, [i] { return kN + i; }));
    }

    auto result = pool->WhenAllBeforeDeadline(std::move(all), start + 50ms)->Get();
    auto duration = SysNow() - start;

    std::ranges::sort(result);
    CHECK_THAT_MT(result, RangeEquals(std::views::iota(0u, kN)));
    CHECK_MT(duration < 80ms);
}

TEST("WhenAllBeforeDeadlineEmpty") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = SysNow();

    auto slow_future = pool->Invoke<Unit>([] {
        std::this_thread::sleep_for(50ms);
        return Unit{};
    });
    std::vector futures = {std::move(slow_future)};

    auto result = pool->WhenAllBeforeDeadline(std::move(futures), start + 25ms)->Get();
    auto duration = SysNow() - start;

    CHECK_MT(result.empty());
    CHECK_MT(duration < 45ms);
}

TEST("WhenAllBeforeDeadlineOne") {
    auto pool = MakeThreadPoolExecutor(N);
    auto start = SysNow();

    auto slow_future = pool->Invoke<Unit>([] {
        std::this_thread::sleep_for(50ms);
        return Unit{};
    });
    std::vector futures = {std::move(slow_future)};

    auto result = pool->WhenAllBeforeDeadline(std::move(futures), start + 75ms)->Get();
    auto duration = SysNow() - start;

    CHECK_MT(result.size() == 1);
    CHECK_MT(duration < 100ms);
}

TEST("WhenAllBeforeDeadlineWaitHalf") {
    STATIC_CHECK(N > 1);
    static constexpr auto kStep = 20ms;
    static constexpr auto kWaitUntilIndex = N / 2;
    static constexpr auto kWaitDuration = kWaitUntilIndex * kStep - kStep / 2;

    auto pool = MakeThreadPoolExecutor(N);
    auto start = SysNow();

    std::vector<FuturePtr<int>> all;
    for (auto i : std::views::iota(0u, N) | std::views::reverse) {
        all.push_back(pool->Invoke<int>([i, start] {
            std::this_thread::sleep_until(start + i * kStep);
            return i;
        }));
    }
    std::this_thread::sleep_until(start + kStep / 2);

    auto result = pool->WhenAllBeforeDeadline(std::move(all), start + kWaitDuration)->Get();
    std::ranges::sort(result);
    CHECK_THAT_MT(result, RangeEquals(std::views::iota(0u, kWaitUntilIndex)));
}
