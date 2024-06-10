#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <deque>
#include <chrono>
#include <mutex>
#include <queue>
#include <vector>
#include <functional>
#include <exception>
#include <optional>
#include <thread>
#include <list>

struct Executor;
struct Task;

enum class TaskCondition { kRunning, kCompleted, kFailed, kCanceled };

struct Task : public std::enable_shared_from_this<Task> {
public:
    virtual ~Task() {
    }

    virtual void Run() = 0;

    void AddDependency(std::shared_ptr<Task> dep);
    void AddTrigger(std::shared_ptr<Task> dep);
    void SetTimeTrigger(std::chrono::system_clock::time_point at);

    // Task::Run() completed without throwing exception
    bool IsCompleted();

    // Task::Run() throwed exception
    bool IsFailed();

    // Task was Canceled
    bool IsCanceled();

    // Task either completed, failed or was Canceled
    bool IsFinished();

    std::exception_ptr GetError();

    void Cancel();

    void Wait();

    void FinishTask(TaskCondition finished_condition);

    void FinishTask(TaskCondition finished_condition, std::exception_ptr eptr);

    void Trigger();

private:
    std::mutex task_mx_;

    TaskCondition condition_ = TaskCondition::kRunning;
    std::exception_ptr eptr_;

    std::atomic<size_t> dependencies_count_ = 0;

    std::vector<std::shared_ptr<Task>> dependencies_roots_;
    std::vector<std::shared_ptr<Task>> triggers_roots_;

    std::list<std::shared_ptr<Task>>::iterator it_list_;
    std::weak_ptr<Executor> executor_;
    bool trigger_ = false;

    std::atomic<bool> end_task_ = false;

    std::chrono::system_clock::time_point time_trigger_ =
        std::chrono::system_clock::time_point::max();

    friend struct Executor;
};

struct TimerQueue {
public:
    TimerQueue() {
    }

    void Add(std::chrono::system_clock::time_point at, std::weak_ptr<Task> ptr) {
        std::lock_guard lock{mx_};
        heap_.emplace(Element(at, ptr));
        time_ = heap_.top().key_;
        cv_.notify_one();
    }

    std::vector<std::weak_ptr<Task>> Pop() {
        std::unique_lock lock{mx_};
        cv_.wait_until(lock, time_, [this]() { return time_ <= Now() || shutdown_.load(); });
        if (shutdown_.load()) {
            return {};
        }
        Key key = Now();
        std::vector<std::weak_ptr<Task>> result;
        while (!heap_.empty()) {
            time_ = heap_.top().key_;
            if (time_ > key) {
                break;
            }
            result.push_back(std::move(heap_.top().value_));
            heap_.pop();
        }
        time_ = heap_.empty() ? Key::max() : heap_.top().key_;
        return result;
    }

    void Shutdown() {
        std::unique_lock lock{mx_};
        shutdown_.store(true);
        cv_.notify_all();
    }

    ~TimerQueue() {
        Shutdown();
    }

private:
    using Key = std::chrono::system_clock::time_point;

    Key Now() const {
        return std::chrono::system_clock::now();
    }

    struct Element {
        Key key_;
        mutable std::weak_ptr<Task> value_;

        friend bool operator<(Element const& a, Element const& b) {
            return a.key_ > b.key_;
        }
    };
    std::mutex mx_;
    std::priority_queue<Element> heap_;
    std::chrono::system_clock::time_point time_ = std::chrono::system_clock::time_point::max();
    std::condition_variable cv_;
    std::atomic<bool> shutdown_ = false;
};

class BufferedChannel {
public:
    BufferedChannel() {
    }

    ~BufferedChannel() {
        Close();
    }

    void Send(std::shared_ptr<Task> value) {
        std::unique_lock lock{mx_};
        if (closed_) {
            throw std::runtime_error{"Closed"};
        }
        buffer_.push_back(value);
        cv_empty_.notify_one();
    }

    std::optional<std::shared_ptr<Task>> Recv() {
        std::unique_lock lock{mx_};
        cv_empty_.wait(lock, [this]() { return !buffer_.empty() || closed_; });
        if (closed_) {
            while (!buffer_.empty()) {
                buffer_.front()->Cancel();
                buffer_.pop_front();
            }
            return std::nullopt;
        }
        std::shared_ptr<Task> result = std::move(buffer_.front());
        buffer_.pop_front();
        return result;
    }

    void Close() {
        std::lock_guard guard{mx_};
        closed_ = true;
        cv_empty_.notify_all();
    }

    bool Empty() {
        return buffer_.empty();
    }

private:
    std::mutex mx_;
    std::condition_variable cv_empty_;
    std::deque<std::shared_ptr<Task>> buffer_;
    bool closed_ = false;
};

template <class T>
class Future;

template <class T>
using FuturePtr = std::shared_ptr<Future<T>>;

// Used instead of void in generic code
struct Unit {};

struct Executor : std::enable_shared_from_this<Executor> {
public:
    Executor(size_t num_threads);

    ~Executor();

    void Submit(std::shared_ptr<Task> task);

    void StartShutdown();
    void WaitShutdown();

    template <class T>
    FuturePtr<T> Invoke(std::function<T()> fn) {
        FuturePtr<T> future_task = std::make_shared<Future<T>>(fn);
        Submit(future_task);
        return future_task;
    }

    template <class Y, class T>
    FuturePtr<Y> Then(FuturePtr<T> input, std::function<Y()> fn) {
        FuturePtr<Y> task = std::make_shared<Future<Y>>(fn);
        task->AddDependency(input);
        Submit(task);
        return task;
    }

    template <class T>
    FuturePtr<std::vector<T>> WhenAll(std::vector<FuturePtr<T>> all) {
        return Invoke<std::vector<T>>([all = std::move(all)]() -> std::vector<T> {
            std::vector<T> array;
            for (size_t i = 0; i != all.size(); ++i) {
                array.push_back(all[i]->Get());
            }
            return array;
        });
    }

    template <class T>
    FuturePtr<T> WhenFirst(std::vector<FuturePtr<T>> all) {
        FuturePtr<T> task = std::make_shared<Future<T>>([all = std::move(all)]() {
            for (;;) {
                for (size_t i = 0; i != all.size(); ++i) {
                    if (all[i]->IsFinished()) {
                        return all[i]->Get();
                    }
                }
            }
        });
        Submit(task);
        return task;
    }

    template <class T>
    FuturePtr<std::vector<T>> WhenAllBeforeDeadline(
        std::vector<FuturePtr<T>> all, std::chrono::system_clock::time_point deadline) {
        FuturePtr<std::vector<T>> task =
            std::make_shared<Future<std::vector<T>>>([all = std::move(all)]() {
                std::vector<T> result;
                for (size_t i = 0; i != all.size(); ++i) {
                    if (all[i]->IsFinished()) {
                        result.push_back(all[i]->Get());
                    }
                }
                return result;
            });
        task->SetTimeTrigger(deadline);
        Submit(task);
        return task;
    }

private:
    std::mutex exe_mx_;

    std::atomic_flag joined_ = ATOMIC_FLAG_INIT;
    bool shutdown_ = false;
    BufferedChannel task_queue_;
    std::list<std::shared_ptr<Task>> wait_list_;

    TimerQueue timer_queue_;

    std::vector<std::thread> run_threads_;
    std::vector<std::thread> time_threads_;

    friend struct Task;
};

std::shared_ptr<Executor> MakeThreadPoolExecutor(uint32_t num_threads);

template <class T>
class Future : public Task {
public:
    Future(std::function<T()> x) : fn_{x} {
    }

    void Run() override {
        result_ = fn_();
    }

    T Get() {
        Wait();
        if (GetError()) {
            std::rethrow_exception(GetError());
        }
        return result_;
    }

private:
    T result_;
    std::function<T()> fn_;
};
