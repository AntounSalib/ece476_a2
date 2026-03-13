#include <thread>
#include <mutex>
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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): 
    ITaskSystem(num_threads),
    num_threads_(num_threads) {
    //
    // TODO: ECE476 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::worker(IRunnable* runnable, int num_total_tasks, int* next_task, std::mutex* mtx){
    while(true){
        int task;
        {
            std::lock_guard<std::mutex> lock(*mtx);
            if (*next_task >= num_total_tasks) {
                return;
            }
            task = (*next_task)++;
        }
        runnable->runTask(task, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: ECE476 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::thread threads[num_threads_];
    std::mutex mtx;
    int next_task = 0;

    for (int i = 0; i < num_threads_; i++){
        threads[i] = std::thread(&TaskSystemParallelSpawn::worker, this,runnable, num_total_tasks, &next_task, &mtx);
    }

    // wait for all threads to finish
    for (int i = 0; i < num_threads_; i++){
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), 
runnable_(nullptr),
num_total_tasks_(0),
next_task_(0),
stop_(false), 
worker_locks_(num_threads){
    //
    // TODO: ECE476 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;

    for (int i = 0; i < num_threads; i++)
        worker_locks_[i].lock();

    for (int i = 0; i < num_threads; i++){
        workers_.emplace_back(std::thread(&TaskSystemParallelThreadPoolSpinning::worker, this, i));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop_.store(true);
    for (int i = 0; i < num_threads_; i++)
        worker_locks_[i].unlock();

    for(int i = 0; i < num_threads_; i++){
        workers_[i].join();
    }
    return;
}

void TaskSystemParallelThreadPoolSpinning::worker(int id){
   while (true) {
        // worker can only grab its lock if released by run
        worker_locks_[id].lock();  

        // if stop, then give lock back and return
        if (stop_.load()) { 
            worker_locks_[id].unlock(); 
            return; 
        }

        // once started, worker does next available task until all 
        // tasks completed
        while (true) {
            int task = next_task_.fetch_add(1);
            if (task >= num_total_tasks_) {
                break;
            }
            runnable_->runTask(task, num_total_tasks_.load());
        }

        // once done, worker gives back their lock
        worker_locks_[id].unlock();    
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: ECE476 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    
    runnable_ = runnable;
    next_task_ = 0;
    tasks_completed_ = 0;
    num_total_tasks_ = num_total_tasks;

    for (int i = 0; i < num_threads_; i++)
        worker_locks_[i].unlock();               

    for (int i = 0; i < num_threads_; i++)
        worker_locks_[i].lock();        

    return;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: ECE476 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: ECE476 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: ECE476 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: ECE476 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: ECE476 students will modify the implementation of this method in Part B.
    //

    return;
}
