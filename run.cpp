#include "executor/executor.h"

#include <latch>
#include <ranges>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

using namespace std::chrono_literals;

namespace {

struct EmptyTask : Task {
    void Run() override {
    }
};

class LatchSignaler : public Task {
public:
    LatchSignaler(std::latch* latch) : latch_{latch} {
    }

    void Run() override {
        latch_->count_down();
    }

private:
    std::latch* latch_;
};

}  // namespace

TEST_CASE("Simple") {
    const auto num_threads = GENERATE(1, 2, 4);
    auto pool = MakeThreadPoolExecutor(num_threads);

    BENCHMARK("Simple:" + std::to_string(num_threads)) {
        auto task = std::make_shared<EmptyTask>();
        pool->Submit(task);
        task->Wait();
    };
}

TEST_CASE("FanoutFanin") {
    const auto num_threads = GENERATE(1, 2, 10);
    const auto size = GENERATE(1, 10, 100);
    auto pool = MakeThreadPoolExecutor(num_threads);

    BENCHMARK("FanoutFanin:" + std::to_string(num_threads) + ':' + std::to_string(size)) {
        auto first_task = std::make_shared<EmptyTask>();
        auto last_task = std::make_shared<EmptyTask>();

        for (auto i = 0; i < size; ++i) {
            auto middle_task = std::make_shared<EmptyTask>();
            middle_task->AddDependency(first_task);
            last_task->AddDependency(middle_task);
            pool->Submit(middle_task);
        }

        pool->Submit(first_task);
        pool->Submit(last_task);
        last_task->Wait();
    };
}

TEST_CASE("ScalableTimers") {
    static constexpr auto kCount = 100'000ul;
    const auto num_threads = GENERATE(1, 2, 5);
    auto pool = MakeThreadPoolExecutor(num_threads);

    BENCHMARK("ScalableTimers:" + std::to_string(num_threads)) {
        std::latch latch{kCount};
        auto at = std::chrono::system_clock::now() + 5ms;

        for (auto i : std::views::iota(0ul, kCount)) {
            auto task = std::make_shared<LatchSignaler>(&latch);
            task->SetTimeTrigger(at + std::chrono::microseconds{(i * i) % 1'000});
            pool->Submit(task);
        }

        latch.wait();
    };
}
