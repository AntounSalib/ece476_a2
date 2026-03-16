#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: ECE476 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): 
ITaskSystem(num_threads),
stop_(false) {
    //
    // TODO: ECE476 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    num_threads_ = num_threads;

    for (int i = 0; i < num_threads; i++){
        workers_.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::worker, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: ECE476 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    stop_.store(true);
    cv_work_.notify_all();
    for(int i = 0; i < num_threads_; i++){
        workers_[i].join();
    }
    return;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: ECE476 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: ECE476 students will implement this method in Part B.
    //
    bool ready = true;

    // determine whether the given task is ready or waiting
    mtx_.lock();
    for (TaskID id : deps){
        if(!completed_TaskIDs_.count(id)){
            ready = false;
            break;
        }
    }

    // add new launch info to appropriate list
    TaskID new_id = next_unique_launch_index_;
    auto info = std::make_shared<LaunchInfo>();
    info->id = new_id;
    info->runnable = runnable;
    info->next_task = 0;
    info->num_total_tasks = num_total_tasks;
    info->num_tasks_completed = 0;
    info->deps = deps;
    next_unique_launch_index_++;

    if(ready){
        ready_tasks_.push_back(std::move(info));
    }
    else{
        waiting_tasks_.push_back(std::move(info));
    }
    mtx_.unlock();

    cv_work_.notify_all();

    return new_id;
}

void TaskSystemParallelThreadPoolSleeping::worker(){
    std::unique_lock<std::mutex> lock(mtx_);

    while (true) {
        // wait to be woken up
        // Once woken, checks if next_task is ready or time to stop
        cv_work_.wait(lock, [this]{ 
            if (stop_) return true;
            for (auto& l : ready_tasks_) {
                if (l->next_task < l->num_total_tasks) return true;
            }
            return false;
         });

        // if stop, end
        if(stop_) return;

        // start working through tasks
        while(true){
            // if done with tasks, break loop
            if (ready_tasks_.empty()){
                break;
            }
            // grab next ready launch, then release lock
            std::shared_ptr<LaunchInfo> target;
            for (auto& l : ready_tasks_) {
                if (l->next_task < l->num_total_tasks) {
                    target = l;  // bumps refcount
                    break;
                }
            }
            if (!target) break;

            // LaunchInfo& launch_info = *target;
            // int task = launch_info.next_task++;

            lock.unlock();

            while (true) {
                int task = target->next_task.fetch_add(1, std::memory_order_relaxed);
                if (task >= target->num_total_tasks) break;

                target->runnable->runTask(task, target->num_total_tasks);

                if (target->num_tasks_completed.fetch_add(1, std::memory_order_acq_rel) + 1
                    == target->num_total_tasks) {
                   
                    lock.lock();
                    completed_TaskIDs_.insert(target->id);
                    for (auto it = ready_tasks_.begin(); it != ready_tasks_.end(); ++it) {
                        if (*it == target) {
                            ready_tasks_.erase(it);
                            break;
                        }
                    }

                    if (ready_tasks_.empty() && waiting_tasks_.empty()) {
                        cv_done_.notify_one();
                    }

                    bool newly_ready = false;
                    for (auto it = waiting_tasks_.begin(); it != waiting_tasks_.end();) {
                        bool rdy = true;
                        for (TaskID id : (*it)->deps) {
                            if (!completed_TaskIDs_.count(id)) { rdy = false; break; }
                        }
                        if (rdy) {
                            newly_ready = true;
                            ready_tasks_.push_back(std::move(*it));
                            it = waiting_tasks_.erase(it);
                        } else { ++it; }
                    }
                    if (newly_ready) cv_work_.notify_all();
                    lock.unlock();
                    break; 
                }
            }
            
            lock.lock();
        }
        
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: ECE476 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(mtx_);

    if(waiting_tasks_.empty() && ready_tasks_.empty()){
        lock.unlock();
        return;
    }

    cv_done_.wait(lock, [this]{ return  (waiting_tasks_.empty() && ready_tasks_.empty()); });
}
