#include "executor/executor.h"
#include "common.h"

#include <thread>
#include <chrono>
#include <atomic>
#include <ranges>
#include <algorithm>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

using namespace std::chrono_literals;

namespace {

#define TEST(name) TEMPLATE_TEST_CASE_SIG(name, "", ((uint32_t N), N), 1, 2, 10)

struct TestTask : Task {
    void Run() override {
        completed = true;
        ++counter;
    }

    ~TestTask() {
        if (counter >= 2) {
            FAIL_CHECK_MT("Seems like task body was run multiple times")
        }
    }

    bool completed = false;
    int counter = 0;
};

template <size_t msec>
struct SlowTask : Task {
    void Run() override {
        std::this_thread::sleep_for(msec * 1ms);
        completed = true;
    }

    bool completed = false;
};

struct FailedTask : Task {
    void Run() override {
        throw std::logic_error{"Failed"};
    }
};

struct RecursiveTask : Task {
    RecursiveTask(uint32_t n, std::shared_ptr<Executor> executor)
        : n_{n}, executor_{std::move(executor)} {
    }

    void Run() override {
        if (n_) {
            executor_->Submit(std::make_shared<RecursiveTask>(n_ - 1, executor_));
        } else {
            executor_->StartShutdown();
        }
    }

private:
    const uint32_t n_;
    const std::shared_ptr<Executor> executor_;
};

struct RecursiveGrowingTask : Task {
    RecursiveGrowingTask(uint32_t n, uint32_t fanout, std::shared_ptr<Executor> executor)
        : n_{n}, fanout_{fanout}, executor_{std::move(executor)} {
    }

    void Run() override {
        if (n_) {
            for (auto i = 0u; i < fanout_; ++i) {
                auto task = std::make_shared<RecursiveGrowingTask>(n_ - 1, fanout_, executor_);
                executor_->Submit(std::move(task));
            }
        } else {
            executor_->StartShutdown();
        }
    }

private:
    const uint32_t n_;
    const uint32_t fanout_;
    const std::shared_ptr<Executor> executor_;
};

auto Now() {
    return std::chrono::system_clock::now();
}

}  // namespace

TEST("Destructor") {
    MakeThreadPoolExecutor(N);
}

TEST("StartShutdown") {
    auto pool = MakeThreadPoolExecutor(N);
    pool->StartShutdown();
}

TEST("StartTwiceAndWait") {
    auto pool = MakeThreadPoolExecutor(N);
    pool->StartShutdown();
    pool->StartShutdown();
    pool->WaitShutdown();
}

TEST("RunSingleTask") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();

    pool->Submit(task);
    task->Wait();

    CHECK_MT(task->completed);
    CHECK_MT(task->IsFinished());
    CHECK_MT(task->IsCompleted());
    CHECK_FALSE_MT(task->IsCanceled());
    CHECK_FALSE_MT(task->IsFailed());
}

TEST("RunSingleFailingTask") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<FailedTask>();

    pool->Submit(task);
    task->Wait();

    CHECK_FALSE_MT(task->IsCompleted());
    CHECK_FALSE_MT(task->IsCanceled());
    CHECK_MT(task->IsFailed());
    auto error = task->GetError();
    CHECK_THROWS_AS_MT(std::rethrow_exception(error), std::logic_error);
}

TEST("CancelSingleTask") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    task->Cancel();
    task->Wait();

    CHECK_FALSE_MT(task->IsCompleted());
    CHECK_MT(task->IsCanceled());
    CHECK_FALSE_MT(task->IsFailed());

    pool->Submit(task);
    task->Wait();

    CHECK_FALSE_MT(task->IsCompleted());
    CHECK_MT(task->IsCanceled());
    CHECK_FALSE_MT(task->IsFailed());
}

TEST("TaskWithSingleDependency") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dependency = std::make_shared<TestTask>();

    task->AddDependency(dependency);
    pool->Submit(task);

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(dependency);
    task->Wait();
    CHECK_MT(task->IsCompleted());
    CHECK_MT(dependency->IsCompleted());
}

TEST("TaskWithSingleCompletedDependency") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dependency = std::make_shared<TestTask>();

    task->AddDependency(dependency);
    pool->Submit(dependency);
    dependency->Wait();

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(task);
    task->Wait();
    CHECK_MT(task->IsCompleted());
}

TEST("FailedDependencyIsConsideredCompleted") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dependency = std::make_shared<FailedTask>();

    task->AddDependency(dependency);
    pool->Submit(task);

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(dependency);
    task->Wait();
    CHECK_MT(task->IsCompleted());
    CHECK_MT(dependency->IsFailed());
}

TEST("CanceledDependencyIsConsideredCompleted") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dependency = std::make_shared<TestTask>();

    task->AddDependency(dependency);
    pool->Submit(task);

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    dependency->Cancel();
    task->Wait();
    CHECK_MT(task->IsCompleted());
    CHECK_MT(dependency->IsCanceled());
}

TEST("RunRecursiveTask") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<RecursiveTask>(100u, pool);
    pool->Submit(task);
    pool->WaitShutdown();
}

TEST("TaskWithSingleTrigger") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto trigger = std::make_shared<TestTask>();

    task->AddTrigger(trigger);
    pool->Submit(task);

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(trigger);
    task->Wait();
    CHECK_MT(task->IsCompleted());
    CHECK_MT(trigger->IsCompleted());
}

TEST("TaskWithSingleCompletedTrigger") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto trigger = std::make_shared<TestTask>();

    task->AddTrigger(trigger);
    pool->Submit(trigger);
    trigger->Wait();
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(task);
    task->Wait();
    CHECK_MT(task->IsCompleted());
}

TEST("TaskWithTwoTrigger") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto trigger_a = std::make_shared<TestTask>();
    auto trigger_b = std::make_shared<TestTask>();

    task->AddTrigger(trigger_a);
    task->AddTrigger(trigger_b);

    std::this_thread::sleep_for(1ms);
    CHECK_FALSE_MT(task->IsCompleted());
    CHECK_FALSE_MT(trigger_a->IsCompleted());
    CHECK_FALSE_MT(trigger_b->IsCompleted());

    pool->Submit(task);
    std::this_thread::sleep_for(1ms);
    CHECK_FALSE_MT(task->IsCompleted());

    pool->Submit(trigger_b);
    task->Wait();
    CHECK_MT(task->IsCompleted());
    CHECK_MT(task->completed);
    CHECK_FALSE_MT(trigger_a->IsCompleted());
    CHECK_MT(trigger_b->IsCompleted());
}

TEST("TriggerAndDependencies") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dep1 = std::make_shared<TestTask>();
    auto dep2 = std::make_shared<TestTask>();
    auto trigger = std::make_shared<TestTask>();

    task->AddDependency(dep1);
    task->AddDependency(dep2);
    task->AddTrigger(trigger);
    dep2->AddTrigger(trigger);

    pool->Submit(dep1);
    pool->Submit(dep2);
    pool->Submit(task);
    std::this_thread::sleep_for(20ms);
    dep1->Wait();
    CHECK_MT(dep1->IsCompleted());
    CHECK_FALSE_MT(dep2->IsFinished());
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(trigger);
    task->Wait();
    CHECK_MT(task->IsCompleted());
    dep2->Wait();
    CHECK_MT(dep2->IsCompleted());
}

TEST("TriggerChain") {
    auto pool = MakeThreadPoolExecutor(N);
    std::vector tasks = {std::make_shared<TestTask>()};

    for (auto i = 0; i < 100'000; ++i) {
        auto task = std::make_shared<TestTask>();
        task->AddTrigger(tasks.back());
        tasks.emplace_back(std::move(task));
    }

    for (auto& task : tasks | std::views::drop(1)) {
        pool->Submit(task);
    }
    std::this_thread::sleep_for(10ms);

    CHECK_MT(std::ranges::none_of(tasks, [](const auto& task) { return task->IsCompleted(); }));

    pool->Submit(tasks.front());
    tasks.back()->Wait();

    CHECK_MT(std::ranges::all_of(tasks, [](const auto& task) { return task->IsCompleted(); }));
}

TEST("MultipleDependencies") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dep1 = std::make_shared<TestTask>();
    auto dep2 = std::make_shared<TestTask>();

    task->AddDependency(dep1);
    task->AddDependency(dep2);
    pool->Submit(task);

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(dep1);
    dep1->Wait();

    std::this_thread::sleep_for(3ms);
    CHECK_FALSE_MT(task->IsFinished());

    pool->Submit(dep2);
    task->Wait();
    CHECK_MT(task->IsCompleted());
}

TEST("TaskWithSingleTimeTrigger") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto start = Now();

    task->SetTimeTrigger(start + 40ms);
    pool->Submit(task);

    std::this_thread::sleep_for(20ms);
    CHECK_FALSE_MT(task->IsFinished());

    task->Wait();
    auto duration = Now() - start;
    CHECK_MT(task->IsCompleted());
    CHECK_MT(duration > 30ms);
    CHECK_MT(duration < 60ms);
}

TEST_CASE("CheckRescheduleAfterNewTask") {
    static constexpr auto kNumThreads = 32;
    auto pool = MakeThreadPoolExecutor(kNumThreads);
    std::this_thread::sleep_for(10ms);

    auto start = Now();
    std::vector<std::shared_ptr<Task>> tasks;
    for (auto i = 0; i < kNumThreads; ++i) {
        auto task = std::make_shared<SlowTask<200>>();
        task->SetTimeTrigger(start + 100ms + i * 1ms);
        tasks.push_back(std::move(task));
    }

    for (auto& task : tasks) {
        pool->Submit(task);
    }
    for (auto& task : tasks) {
        task->Wait();
    }

    auto duration = Now() - (start + 100ms + (kNumThreads - 1) * 1ms);
    CHECK_MT(duration > 190ms);
    CHECK_MT(duration < 210ms);
}

TEST("TaskTriggeredByTimeAndDep") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto dep = std::make_shared<TestTask>();

    task->AddDependency(dep);
    task->SetTimeTrigger(Now() + 40ms);

    pool->Submit(task);
    pool->Submit(dep);

    std::this_thread::sleep_for(20ms);
    CHECK_MT(task->IsCompleted());
}

TEST("MultipleTimerTriggers") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task_a = std::make_shared<TestTask>();
    auto task_b = std::make_shared<TestTask>();
    auto start = Now();

    task_a->SetTimeTrigger(start + 40ms);
    task_b->SetTimeTrigger(start + 5ms);

    pool->Submit(task_a);
    pool->Submit(task_b);

    std::this_thread::sleep_for(20ms);
    CHECK_FALSE_MT(task_a->IsFinished());
    CHECK_MT(task_b->IsCompleted());

    task_a->Wait();
    auto duration = Now() - start;
    CHECK_MT(task_a->IsCompleted());
    CHECK_MT(duration < 60ms);
}

TEST("MultipleTimerTriggersWithReverseOrder") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task_a = std::make_shared<TestTask>();
    auto task_b = std::make_shared<TestTask>();

    task_a->SetTimeTrigger(Now() + 50ms);
    task_b->SetTimeTrigger(Now() + 3ms);

    pool->Submit(task_b);
    pool->Submit(task_a);

    task_b->Wait();
    CHECK_FALSE_MT(task_a->IsFinished());
}

TEST("PossibleToCancelAfterSubmit") {
    auto pool = MakeThreadPoolExecutor(N);
    std::vector<std::shared_ptr<SlowTask<1>>> tasks;

    for (auto i = 0; i < 1'000; ++i) {
        auto& task = tasks.emplace_back(std::make_shared<SlowTask<1>>());
        pool->Submit(task);
        task->Cancel();
    }
    pool.reset();

    for (const auto& t : tasks) {
        if (!t->completed) {
            return;
        }
    }
    FAIL_MT("Seems like Cancel() doesn't affect Submitted tasks");
}

TEST("CancelAfterDetroyOfExecutor") {
    auto pool = MakeThreadPoolExecutor(N);
    auto first = std::make_shared<TestTask>();
    auto second = std::make_shared<TestTask>();

    first->AddDependency(second);
    pool->Submit(first);
    pool.reset();

    first->Cancel();
    second->Cancel();
}

TEST("NoDeadlockWhenSubmittingFromTaskBody") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<RecursiveGrowingTask>(5, 10, pool);
    pool->Submit(task);
    pool->WaitShutdown();
}

TEST("WaitInManyThreads") {
    auto pool = MakeThreadPoolExecutor(N);
    auto task = std::make_shared<TestTask>();
    auto canceled = std::make_shared<TestTask>();

    std::vector<std::jthread> threads;
    for (auto i = 0; i < 4; ++i) {
        threads.emplace_back([&] {
            task->Wait();
            CHECK_MT(task->completed);
        });
        threads.emplace_back([&] {
            canceled->Wait();
            CHECK_FALSE_MT(canceled->completed);
        });
    }

    std::this_thread::sleep_for(10ms);

    pool->Submit(task);
    task->Wait();
    CHECK_MT(task->completed);

    canceled->Cancel();
    CHECK_FALSE_MT(canceled->completed);

    threads.clear();
}
