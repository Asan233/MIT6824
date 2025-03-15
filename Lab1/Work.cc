#include <grpcpp/grpcpp.h>
#include <memory>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <fcntl.h>

#include "MapReduce.grpc.pb.h"

std::atomic<int> MapTaskNumer(0);             // Map任务数量
std::atomic<int> ReduceTaskNumer(0);          // Reduce任务数量

class KVPair {
public:
    KVPair() = default;
    KVPair(std::string& key, int value): Key(std::move(key)), value(value) {}
    std::string Key;
    int value;
    
};

class WorkMapReduce {

public:
    WorkMapReduce(std::shared_ptr<grpc::Channel> channel, int mapN = 13, int reduceN = 9): MapNumber(mapN), ReduceNumber(reduceN), _stub(mapreduce::MapReduce::NewStub(channel)), nonblock(false) {
        // Map线程创建
        for(int i = 0; i < MapNumber; ++i) {
            MapThreads.emplace_back(&WorkMapReduce::MapF, this, i);
        }
        
        // Reduce线程创建
        for(int i = 0; i < ReduceNumber; ++i) {
            ReduceThreads.emplace_back(&WorkMapReduce::ReduceF, this, i + MapNumber);
        }

        mapClient = std::thread(&WorkMapReduce::MapClient, this);
        mapClient.join();
        reduceClient = std::thread(&WorkMapReduce::ReduceClient, this);
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
    // std::mutex mapMutex;
    std::condition_variable mapCV;

    // 唤醒Reduce获取Reduce任务
    // std::mutex reduceMutex;
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
            mapCV.wait(lock, [this]() {return 0 != MapTaskNumer.load(std::memory_order_acquire) || nonblock; });
            if(nonblock) {
                break;
            }
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
                    std::vector<std::string> content = std::move(SplitStr(response.filename().c_str()));
                    std::vector<std::vector<KVPair>> reducefiles(ReduceNumber);
                    for(int i = 0; i < content.size(); ++i) {
                        int hash = Hash_str(content[i]);
                        reducefiles[hash].emplace_back(content[i], 1);
                    }
                    writeDisk(reducefiles, ID);
                    grpc::ClientContext context;
                    google::protobuf::Empty empty;
                    _stub->MapDone(&context, request, &empty);
                }
            }else continue;
        }
    }

    void ReduceF(int ID) {

        mapreduce::ReduceRequest request;          // 请求体
        mapreduce::ReduceResponse response;        // 返回体
        
        std::mutex tmpMutex;
        request.set_reduce_id(ID);
        while(1) {
            // 等待Client唤醒
            std::unique_lock<std::mutex> lock(tmpMutex);
            reduceCV.wait( lock, [this]() { return 0 != ReduceTaskNumer.load(std::memory_order_acquire); } );
            ReduceTaskNumer.fetch_sub(1, std::memory_order_release);
            //std::cout << "Reduce ID : " << ID << std::endl;
            /* 每次RPC调用都需要使用一个新的上下文，不能将一个ClientContext来发起多次RPC调用 */
            grpc::ClientContext context;
            grpc::Status status = _stub->Reduce(&context, request, &response);
            if( status.ok() ) {
                if( response.is_finished() ) {
                    std::cout << "Reduce Finished : " << ID << std::endl;
                    break;
                }else {
                    std::cout << "Reduce ID : " << ID << " Task : " << response.filename() << std::endl;
                    // Reduce任务处理
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
                nonblock = true;
                mapCV.notify_all();
                for(int i = 0; i < MapNumber; ++i) {
                    MapThreads[i].join();
                }
                std::cout << "Map Client Task Finish !" << std::endl;
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
                ReduceTaskNumer.fetch_add(1, std::memory_order_release);
                reduceCV.notify_one();
            }
        }
    }

    std::vector<std::string> SplitStr(const char* str) {
        int fd = open(str, O_RDONLY);
        int length = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        char buf[length];
        bzero(buf, length);
        int len = read(fd, buf, length);
        if(len != length) {
            std::cout << "Read Error !" << std::endl;
            return std::vector<std::string>();
        }
        std::vector<std::string> res;
        std::string tmp = "";
        for(int i = 0; i < len; ++i) {
            if( (buf[i] >= 'A' && buf[i] <= 'Z') || (buf[i] >= 'a' && buf[i] <= 'z')) 
                tmp += buf[i];
            else {
                if(tmp.size() != 0) {
                    res.push_back(tmp);
                    tmp = "";
                }
            }
        }
        close(fd);
        return res;
    }

    void writeDisk(std::vector<std::vector<KVPair>>& reducefiles, int ID) {
        int i = 0;
        for(auto& vec : reducefiles) {
            std::string filename = "../tmpFiles/tmp_Reduce_" + std::to_string(i) + "_Map_" +  std::to_string(ID) + ".txt";
            int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);

            if(fd == -1) {
                std::cout << "Open Error !" << std::endl;
                return;
            }

            for(auto& kv : vec) {
                std::string tmp = kv.Key + " " + std::to_string(kv.value) + "\n";
                write(fd, tmp.c_str(), tmp.size());
            }
            close(fd);
            ++i;
        }
    }

    int Hash_str(std::string& str) {
    int hash = 0;
    for(int i = 0; i < str.size(); ++i) {
        hash = (hash + str[i]) % ReduceNumber;
    }
    return hash;
}
};


int main(int argc, char* argv)
{
    WorkMapReduce work(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()) );
}




