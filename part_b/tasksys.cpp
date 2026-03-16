#include <thread>
#include <mutex>
#include <condition_variable>
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
    next_unique_launch_index_++;
    if(ready){
        ready_tasks_.emplace_back(LaunchInfo{new_id, runnable, 0, num_total_tasks, 0, deps});
    }
    else{
        waiting_tasks_.emplace_back(LaunchInfo{new_id, runnable, 0, num_total_tasks, 0, deps});
    }
    mtx_.unlock();

    cv_work_.notify_all();

    return new_id;
}

void TaskSystemParallelThreadPoolSleeping::worker(){
   while (true) {
        // wait to be woken up
        // Once woken, checks if next_task is ready or time to stop
        std::unique_lock<std::mutex> lock(mtx_);
        cv_work_.wait(lock, [this]{ return (ready_tasks_.size()>0) || stop_; });

        // if stop, end
        if(stop_) return;

        // start working through tasks
        while(true){
            // if done with tasks, break loop
            if (ready_tasks_.empty()){
                lock.unlock();
                break;
            }
            // grab next ready launch, then release lock
            LaunchInfo& launch_info = ready_tasks_[0];
            int task = launch_info.next_task;
            if (task >= launch_info.num_total_tasks){
                lock.unlock();
                lock.lock();
                continue;
            }

            launch_info.next_task++;
            lock.unlock();

            // run task
            launch_info.runnable->runTask(task, launch_info.num_total_tasks);
                        
            // last thread to complete a task in a given launch, finds
            // the next launch
            lock.lock();
            if (++launch_info.num_tasks_completed == launch_info.num_total_tasks){
                completed_TaskIDs_.insert(launch_info.id);
                ready_tasks_.erase(ready_tasks_.begin());
                
                if (ready_tasks_.empty() && waiting_tasks_.empty()){
                    cv_done_.notify_one();
                }

                // whether any task has become ready
                bool newly_ready_task = false;

                // update runnable tasks list
                for (auto curr_info = waiting_tasks_.begin(); curr_info != waiting_tasks_.end();){
                    bool ready = true;
                    for (TaskID id : curr_info->deps){
                        if(!completed_TaskIDs_.count(id)){
                            ready = false;
                            break;
                        }
                    }

                    // if ready, add to list and update 
                    // newly_ready_task boolean
                    if(ready){
                        newly_ready_task = true;
                        ready_tasks_.push_back(std::move(*curr_info));
                        curr_info = waiting_tasks_.erase(curr_info);
                        
                    }

                }

                // if new task is available, wakes up potentially
                // asleep workers
                if (newly_ready_task){
                    cv_work_.notify_all();
                }
            }
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
