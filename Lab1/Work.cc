#include <grpcpp/grpcpp.h>
#include <memory>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "MapReduce.grpc.pb.h"

std::atomic<int> MapTaskNumer(0);             // Map任务数量
std::atomic<int> ReduceTaskNumer(0);          // Reduce任务数量

class WorkMapReduce {

public:
    WorkMapReduce(std::shared_ptr<grpc::Channel> channel, int mapN = 13, int reduceN = 9): MapNumber(mapN), ReduceNumber(reduceN), _stub(mapreduce::MapReduce::NewStub(channel)), nonblock(false) {
        //std::cout << "Client Initing" << std::endl;
        // Map线程创建
        for(int i = 0; i < MapNumber; ++i) {
            MapThreads.emplace_back(&WorkMapReduce::MapF, this, i);
        }
        
        // Reduce线程创建
        for(int i = 0; i < ReduceNumber; ++i) {
            ReduceThreads.emplace_back(&WorkMapReduce::ReduceF, this, i + MapNumber);
        }

        mapClient = std::thread(&WorkMapReduce::MapClient, this);
        reduceClient = std::thread(&WorkMapReduce::ReduceClient, this);
        //std::cout << "Client Inited" << std::endl;
        reduceClient.join();
    }
    ~WorkMapReduce() {}

private:
    int MapNumber;          // Map线程数量
    std::vector<std::thread> MapThreads; // Map线程
    int ReduceNumber;       // Reduce线程数量
    std::vector<std::thread> ReduceThreads; // Reduce线程
    std::unique_ptr<mapreduce::MapReduce::Stub> _stub;  // Master服务器stub

    bool nonblock;          // 是否唤醒所有线程

    // 唤醒Map获取Map任务
    std::mutex mapMutex;
    std::condition_variable mapCV;

    // 唤醒Reduce获取Reduce任务
    std::mutex reduceMutex;
    std::condition_variable reduceCV;

    // 任务分配器
    std::thread mapClient;
    std::thread reduceClient;

    void MapF(int ID) {
        mapreduce::MapRequest request;          // 请求体
        mapreduce::MapResponse response;        // 返回体
        
        std::mutex tmpMutex;
        request.set_map_id(ID);
        while(1) {
            // 等待Client唤醒
            std::unique_lock<std::mutex> lock(tmpMutex);
            mapCV.wait(lock, [this]() {return 0 != MapTaskNumer.load(std::memory_order_acquire);});
            MapTaskNumer.fetch_sub(1, std::memory_order_release);
            /* 每次RPC调用都需要使用一个新的上下文，不能将一个ClientContext来发起多次RPC调用 */
            grpc::ClientContext context;            
            grpc::Status status = _stub->Map(&context, request, &response);
            if( status.ok() ) {
                if( response.is_finished() ) {
                    std::cout << "Map Finished : " << ID << std::endl;
                    break;
                }else {
                    std::cout << "Map ID : " << ID << " Task : " << response.filename() << std::endl;
                    // Map任务处理
                    sleep(10);
                }
            }else continue;
        }
    }

    void ReduceF(int ID) {

        mapreduce::ReduceRequest request;          // 请求体
        mapreduce::ReduceResponse response;        // 返回体
        grpc::ClientContext context;            // 上下文

        request.set_reduce_id(ID);
        while(1) {
            // 等待Client唤醒
            std::unique_lock<std::mutex> lock(reduceMutex);
            reduceCV.wait(lock);
            lock.unlock();
            grpc::Status status = _stub->Reduce(&context, request, &response);
            if( status.ok() ) {
                if( response.is_finished() ) {
                    std::cout << "Reduce Finished : " << ID << std::endl;
                    break;
                }else {
                    std::cout << "Reduce ID : " << ID << " Task : " << response.filename() << std::endl;
                    // Map任务处理
                    sleep(10);
                }
            }else continue;
        }
    }

    void MapClient() {
        mapreduce::TaskNotification notification;   // 通知体
        grpc::ClientContext context;                // 上下文
        google::protobuf::Empty empty;              // 空请求体
        /* 建立长连接，等待任务通知 */
        std::unique_ptr<grpc::ClientReader<mapreduce::TaskNotification>> reader(_stub->SubscribeMapTask(&context, empty));

        while( reader->Read(&notification) ) {
            if(notification.task_type() == mapreduce::TaskNotification::NONE) {
                std::cout << "No Task " << std::endl;
                break;
            }else if(notification.task_type() == mapreduce::TaskNotification::MAP) {
                std::cout << "Map Task " << std::endl;
                MapTaskNumer.fetch_add(1, std::memory_order_release);
                mapCV.notify_one();
            }
        }
    }

    void ReduceClient() {
        mapreduce::TaskNotification notification;   // 通知体
        grpc::ClientContext context;                // 上下文
        google::protobuf::Empty empty;              // 空请求体
        /* 建立长连接，等待任务通知 */
        std::unique_ptr<grpc::ClientReader<mapreduce::TaskNotification>> reader(_stub->SubscribeReduceTask(&context, empty));

        while( reader->Read(&notification) ) {
            if(notification.task_type() == mapreduce::TaskNotification::NONE) {
                std::cout << "No Task " << std::endl;
                break;
            }else if(notification.task_type() == mapreduce::TaskNotification::REDUCE) {
                std::cout << "Reduce Task " << std::endl;
                reduceCV.notify_one();
            }
        }
    }
};


int main(int argc, char* argv)
{
    WorkMapReduce work(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()) );
}




