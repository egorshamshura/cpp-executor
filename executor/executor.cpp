#include "executor.h"
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>

// Task
// -------------------------------

void Task::AddDependency(std::shared_ptr<Task> dep) {
    std::lock_guard guard{dep->task_mx_};
    if (dep->condition_ == TaskCondition::kRunning) {
        dep->dependencies_roots_.push_back(shared_from_this());
        dependencies_count_.fetch_add(1);
    }
}

void Task::AddTrigger(std::shared_ptr<Task> dep) {
    std::lock_guard guard{dep->task_mx_};
    if (dep->condition_ == TaskCondition::kRunning) {
        dep->triggers_roots_.push_back(shared_from_this());
        dependencies_count_.fetch_add(1);
    }
}

void Task::SetTimeTrigger(std::chrono::system_clock::time_point at) {
    std::lock_guard guard{task_mx_};
    time_trigger_ = at;
}

bool Task::IsCompleted() {
    std::lock_guard guard{task_mx_};
    return condition_ == TaskCondition::kCompleted;
}

bool Task::IsFailed() {
    std::lock_guard guard{task_mx_};
    return condition_ == TaskCondition::kFailed;
}

bool Task::IsCanceled() {
    std::lock_guard guard{task_mx_};
    return condition_ == TaskCondition::kCanceled;
}

bool Task::IsFinished() {
    std::lock_guard guard{task_mx_};
    return condition_ == TaskCondition::kFailed || condition_ == TaskCondition::kCanceled ||
           condition_ == TaskCondition::kCompleted;
}

std::exception_ptr Task::GetError() {
    std::lock_guard guard{task_mx_};
    return eptr_;
}

void Task::Cancel() {
    FinishTask(TaskCondition::kCanceled);
}

void Task::Wait() {
    end_task_.wait(false);
}

void Task::FinishTask(TaskCondition finished_condition) {
    task_mx_.lock();
    auto deps = dependencies_roots_;
    auto trs = triggers_roots_;
    condition_ = finished_condition;
    dependencies_roots_.clear();
    triggers_roots_.clear();
    end_task_.store(true);
    end_task_.notify_all();
    task_mx_.unlock();
    for (size_t i = 0; i != deps.size(); ++i) {
        if (deps[i]->dependencies_count_.fetch_sub(1) == 1) {
            deps[i]->Trigger();
        }
    }
    for (size_t i = 0; i != trs.size(); ++i) {
        trs[i]->dependencies_count_.store(0);
        trs[i]->Trigger();
    }
}

void Task::FinishTask(TaskCondition finished_condition, std::exception_ptr eptr) {
    task_mx_.lock();
    auto deps = dependencies_roots_;
    auto trs = triggers_roots_;
    dependencies_roots_.clear();
    triggers_roots_.clear();
    condition_ = finished_condition;
    eptr_ = eptr;
    end_task_.store(true);
    end_task_.notify_all();
    task_mx_.unlock();
    for (size_t i = 0; i != deps.size(); ++i) {
        if (deps[i]->dependencies_count_.fetch_sub(1) == 1) {
            deps[i]->Trigger();
        }
    }
    for (size_t i = 0; i != trs.size(); ++i) {
        trs[i]->dependencies_count_.store(0);
        trs[i]->Trigger();
    }
}

void Task::Trigger() {
    std::unique_lock guard{task_mx_};
    if (trigger_) {
        return;
    }
    trigger_ = true;
    auto pool = executor_.lock();
    if (pool) {
        guard.unlock();

        if (pool->shutdown_) {
            Cancel();
            return;
        }
        std::unique_lock guard{pool->exe_mx_};
        pool->wait_list_.erase(it_list_);
        guard.unlock();
        pool->task_queue_.Send(shared_from_this());
    }
}

// -------------------------------

// Executor
// -------------------------------

Executor::Executor(size_t num_threads) {
    for (size_t i = 0; i != num_threads; ++i) {
        run_threads_.emplace_back([&]() {
            for (;;) {
                auto task = task_queue_.Recv();
                if (!task) {
                    return;
                }
                if (task.value()->IsCanceled()) {
                    continue;
                }
                try {
                    task.value()->Run();
                    task.value()->FinishTask(TaskCondition::kCompleted);
                } catch (...) {
                    std::exception_ptr eptr = std::current_exception();
                    task.value()->FinishTask(TaskCondition::kFailed, eptr);
                }
            }
        });
    }

    for (size_t i = 0; i != std::max(static_cast<size_t>(1), num_threads / 2); ++i) {
        time_threads_.emplace_back([&]() {
            for (;;) {
                auto weak_tasks = timer_queue_.Pop();
                if (weak_tasks.empty()) {
                    return;
                }
                for (std::weak_ptr<Task>& x : weak_tasks) {
                    auto y = x.lock();
                    if (y) {
                        y->Trigger();
                    }
                }
            }
        });
    }
}

Executor::~Executor() {
    StartShutdown();
    WaitShutdown();
}

void Executor::Submit(std::shared_ptr<Task> task) {
    if (task->IsCanceled()) {
        return;
    }
    if (task->time_trigger_ != std::chrono::system_clock::time_point::max()) {
        if (task->dependencies_count_ == 0) {
            ++task->dependencies_count_;
        }
        timer_queue_.Add(task->time_trigger_, task);
    }

    if (task->dependencies_count_ == 0) {
        task_queue_.Send(task);
    } else {
        std::list<std::shared_ptr<Task>>::iterator it = task->it_list_;
        {
            std::lock_guard guard{exe_mx_};
            wait_list_.push_front(task);
            it = wait_list_.begin();
        }

        {
            std::lock_guard g{task->task_mx_};
            task->it_list_ = it;
            task->executor_ = shared_from_this();
        }
    }
}

void Executor::StartShutdown() {
    task_queue_.Close();
    timer_queue_.Shutdown();
    std::lock_guard guard{exe_mx_};
    for (auto& task : wait_list_) {
        task->Cancel();
    }
    wait_list_.clear();
    shutdown_ = true;
}

void Executor::WaitShutdown() {
    if (joined_.test_and_set()) {
        return;
    }
    for (size_t i = 0; i != run_threads_.size(); ++i) {
        run_threads_[i].join();
    }
    for (size_t i = 0; i != time_threads_.size(); ++i) {
        time_threads_[i].join();
    }
}

std::shared_ptr<Executor> MakeThreadPoolExecutor(uint32_t num_threads) {
    return std::make_shared<Executor>(num_threads);
}

// -------------------------------
