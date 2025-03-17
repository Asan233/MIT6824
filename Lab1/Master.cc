#include <grpcpp/grpcpp.h>
#include <vector>
#include <queue>
#include <string>
#include <sys/types.h>
#include <dirent.h>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "MapReduce.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using mapreduce::MapReduce;

std::unordered_map<int, std::string> workers;   // workerID -> inputFiles
std::unordered_map<int, std::string> results;   // workerID -> outputFiles

std::queue<std::string> MapTasks;               // MapFiles
std::queue<std::string> ReducesTasks;           // ReduceFiles

std::atomic<int> MapTaskNumer(0);              // Map任务数量
std::atomic<int> MapFinish(0);                 // Map任务完成数量
std::atomic<int> ReduceTaskNumer(0);           // Reduce任务数量
std::atomic<int> ReduceFinish(0);              // Reduce任务完成数量

std::mutex mapMutex;                            // Map队列互斥锁
std::condition_variable MapCV;                  // Map队列条件变量
std::mutex reduceMutex;                         // Reduce队列互斥锁
std::condition_variable reduceCV;               // Reduce队列条件变量

void Timeover(int worker_id, int task_id) {
    if(task_id == 1) {
        std::string filename = workers[worker_id];
        std::cout << "Map Task Over Time " << filename << std::endl;
        std::unique_lock<std::mutex> lock(mapMutex);
        MapTasks.push(filename);
        MapTaskNumer.fetch_add(1, std::memory_order_release);
        MapCV.notify_one();
    }else {
        std::string filename = results[worker_id];
        std::cout << "Reduce Task Over Time " << filename << std::endl;
        std::unique_lock<std::mutex> lock(reduceMutex);
        ReducesTasks.push(filename);
        ReduceTaskNumer.fetch_add(1, std::memory_order_release);
        reduceCV.notify_one();
    }
}

struct time_tw {
    int slot;
    int worker_id;
    int task_id;
    time_tw* next;
    time_tw* prev;
};

class Time_Wheel {
    public:
        static const int TIME;
        static const int N;

        static Time_Wheel* GetInstance() {
            static Time_Wheel instance;
            return &instance;
        }

        /* 定时器开始 */
        void start() {
            while(true) {
                std::this_thread::sleep_for(std::chrono::seconds(TIME));
                tick();
            }
        }

        void add_time(int worker_id, int task_id) {
            time_tw* timer = new time_tw;
            worker_timer[worker_id] = timer;

            int n_slot = timer->slot = (cur + 2) % N;
            timer->worker_id = worker_id;
            timer->task_id = task_id;

            head[n_slot]->next->prev = timer;
            timer->next = head[n_slot]->next;
            head[n_slot]->next = timer;
            timer->prev = head[n_slot];
        }

        /* 删除定时器 */
        bool del_time(int worker_id) {
            time_tw* timer = worker_timer[worker_id];
            if(timer == nullptr) return false;
            timer->prev->next = timer->next;
            timer->next->prev = timer->prev;
            worker_timer[worker_id] = nullptr;
            delete timer;
            return true;
        }

        /* 触发超时心跳 */
        void tick() {
            //std::cout << "Tick " << cur << std::endl;
            cur = (cur + 1) % N;
            time_tw* timer = head[cur]->next;
            while(timer != tail[cur]) {
                std::cout << "Worker : " << timer->worker_id << " 任务超时 " << std::endl;
                Timeover(timer->worker_id, timer->task_id);
                del_time(timer->worker_id);
                timer = head[cur]->next;
            }
            
        }

    private:
        Time_Wheel(): time_wheel(N, nullptr), head(N, nullptr), tail(N, nullptr), cur(0) {
            for(int i = 0; i < N; ++i) {
                head[i] = new time_tw;
                tail[i] = new time_tw;
                time_wheel[i] = head[i];
                head[i]->next = tail[i];
                tail[i]->prev = head[i];
            }
        }

        std::vector<time_tw*> time_wheel;
        std::vector<time_tw*> head;
        std::vector<time_tw*> tail;
        std::unordered_map<int, time_tw*> worker_timer;
        int cur;
};

const int Time_Wheel::TIME = 5;
const int Time_Wheel::N = 64;


class MapReduceServiceImpl final : public MapReduce::Service {

    grpc::Status Map(grpc::ServerContext* context, const mapreduce::MapRequest* request, mapreduce::MapResponse* response) override {
        std::cout << "Map Request from Worker : " << request->map_id() << std::endl; 
        std::unique_lock<std::mutex> lock(mapMutex);
        // Map请求任务
        if( !MapTasks.empty() ) {
            // 设置返回消息体
            response->set_is_finished(false);
            response->set_filename( MapTasks.front() );
            // 记录workerID -> inputFiles
            workers[request->map_id()] = MapTasks.front();
            // 弹出任务
            MapTasks.pop();
            Time_Wheel::GetInstance()->add_time(request->map_id(), 1);
        }else {
            response->set_is_finished(true);
        }
        return grpc::Status::OK;
    }

    grpc::Status MapDone(grpc::ServerContext* context, const mapreduce::MapRequest* request, google::protobuf::Empty* response) override{
        int work_id = request->map_id();
        std::cout << "Map Done : " << work_id << "  Task : " << workers[work_id] << std::endl;
        Time_Wheel::GetInstance()->del_time(work_id);
        // Map需要完成任务减一
        MapFinish.fetch_sub(1, std::memory_order_release);
        // Reduce需要完成的任务加一
        std::unique_lock<std::mutex> lock(reduceMutex);
        ReducesTasks.push(std::to_string(ReduceFinish.load(std::memory_order_acquire)));
        lock.unlock();
        ReduceFinish.fetch_add(1, std::memory_order_release);
        ReduceTaskNumer.fetch_add(1, std::memory_order_release);
        MapCV.notify_one();
        return grpc::Status::OK;
    }

    grpc::Status Reduce(grpc::ServerContext* context, const mapreduce::ReduceRequest* request, mapreduce::ReduceResponse* response) override {
        std::cout << "Reduce Request from Worker : " << request->reduce_id() << std::endl;
        std::unique_lock<std::mutex> lock(reduceMutex);
        // Reduce请求任务
        if( !ReducesTasks.empty() ) {
            // 设置返回消息体
            response->set_is_finished(false);
            response->set_filename(ReducesTasks.front());
            results[request->reduce_id()] = ReducesTasks.front();
            // 弹出任务
            ReducesTasks.pop();
            Time_Wheel::GetInstance()->add_time(request->reduce_id(), 2);
        }
        return grpc::Status::OK;
    }

    grpc::Status ReduceDone(grpc::ServerContext* context, const mapreduce::ReduceRequest* request, google::protobuf::Empty* response) override{
        int work_id = request->reduce_id();
        bool ret = Time_Wheel::GetInstance()->del_time(work_id);
        if(!ret) {
            std::cout << "Reduce Done TimeOver : " << work_id << std::endl;
            return grpc::Status::OK;
        }
        std::cout << "Reduce Done : " << work_id << "  Task : " << results[work_id] << std::endl;
        // Reduce需要完成任务减一
        ReduceFinish.fetch_sub(1, std::memory_order_release);
        reduceCV.notify_one();
        return grpc::Status::OK;
    }

    grpc::Status SubscribeReduceTask(grpc::ServerContext* context, const ::google::protobuf::Empty* request, grpc::ServerWriter<mapreduce::TaskNotification>* write) override {
        mapreduce::TaskNotification notification;
        std::cout << "Reduce Client Subscribe " << std::endl;
        while(!context->IsCancelled()) {
            std::unique_lock<std::mutex> lock(reduceMutex);
            reduceCV.wait(lock, []{ return ReduceTaskNumer.load(std::memory_order_acquire) != 0 || ReduceFinish.load(std::memory_order_acquire) == 0; } );
            if(ReduceFinish.load(std::memory_order_acquire) == 0) {
                notification.set_task_type(mapreduce::TaskNotification::NONE);
                write->Write(notification);
                break;
            }else {
                ReduceTaskNumer.fetch_sub(1, std::memory_order_release);
                notification.set_task_type(mapreduce::TaskNotification::REDUCE);
                write->Write(notification);
            }
        }
        return grpc::Status::OK;
    }

    grpc::Status SubscribeMapTask(grpc::ServerContext* context, const ::google::protobuf::Empty* request, grpc::ServerWriter<mapreduce::TaskNotification>* write) override {
        mapreduce::TaskNotification notification;
        std::mutex tmpMutex;
        std::cout << "Map Client Subscribe " << std::endl;
        while(!context->IsCancelled()) {
            std::unique_lock<std::mutex> lock(tmpMutex);
            MapCV.wait(lock, []{ return MapTaskNumer.load(std::memory_order_acquire) != 0 || MapFinish.load(std::memory_order_acquire) == 0; } );
            if(MapFinish.load(std::memory_order_acquire) == 0) {
                notification.set_task_type(mapreduce::TaskNotification::NONE);
                write->Write(notification);
                break;
            }else {
                MapTaskNumer.fetch_sub(1, std::memory_order_release);
                notification.set_task_type(mapreduce::TaskNotification::MAP);
                write->Write(notification);
            }
        }
        return grpc::Status::OK;
    }

};

void Runserver() {
    std::string server_address("127.0.0.1:50051");
    MapReduceServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "gRPC Server Listening on " << server_address << std::endl;

    server->Wait();
}

/**
 *   读取输入文件目录
*/
void ReadFiles(std::queue<std::string>& inputFiles, std::string& inputDir)
{
    DIR *pDir;
    struct dirent* ptr;
    if( !(pDir = opendir(inputDir.c_str())) )
    {
        std::cout << "Input Folder dosen't Exist " << std::endl;
        return;
    }
    while( (ptr = readdir(pDir)) != 0 )
    {
        if( strcmp(ptr->d_name, ".") != 0 && strcmp(ptr->d_name, "..") != 0 )
        {
            inputFiles.push(inputDir + "/" + ptr->d_name);
            MapTaskNumer.fetch_add(1, std::memory_order_release);
            MapFinish.fetch_add(1, std::memory_order_release);
        }
    }
    closedir(pDir);
}


int main() {
    /* 开启定时器 */
    std::thread timer(&Time_Wheel::start, Time_Wheel::GetInstance());
    std::cout << "Timer Start " << std::endl;
    std::string inputDir = "../InputFiles";
    ReadFiles(MapTasks, inputDir );
    Runserver();
    return 0;
}


