#include <grpcpp/grpcpp.h>
#include <memory>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <fcntl.h>
#include <fstream>
#include <dirent.h>

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
    WorkMapReduce(std::shared_ptr<grpc::Channel> channel, int mapN = 13, int reduceN = 8): MapNumber(mapN), ReduceNumber(reduceN), _stub(mapreduce::MapReduce::NewStub(channel)),
                    nonblock_map(false), nonblock_reduce(false) {
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

    bool nonblock_map;          // 是否唤醒map所有线程
    bool nonblock_reduce;       // 是否唤醒reduce所有线程

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
            mapCV.wait(lock, [this]() {return 0 != MapTaskNumer.load(std::memory_order_acquire) || nonblock_map; });
            if(nonblock_map) {
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
                    writeMapDisk(reducefiles, ID);
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
            reduceCV.wait( lock, [this]() { return 0 != ReduceTaskNumer.load(std::memory_order_acquire) || nonblock_reduce; } );
            if(nonblock_reduce) {
                break;
            }
            ReduceTaskNumer.fetch_sub(1, std::memory_order_release);
            /* 每次RPC调用都需要使用一个新的上下文，不能将一个ClientContext来发起多次RPC调用 */
            grpc::ClientContext context;
            grpc::Status status = _stub->Reduce(&context, request, &response);
            if( status.ok() ) {
                if( response.is_finished() ) {
                    std::cout << "Reduce Finished : " << ID << std::endl;
                    break;
                }else {
                    int reduceID = response.filename()[0] - '0';
                    std::cout << "Reduce ID : " << ID << " Task : " << reduceID << std::endl;
                    // Reduce任务处理
                    std::multimap<std::string, int> res = std::move(shuffle(response.filename()));
                    std::map<std::string, int> result;
                    for(auto& kv : res) {
                        result[kv.first] += kv.second;
                    }
                    if( writeReduceDisk(result, response.filename()) ) {
                        grpc::ClientContext context;
                        google::protobuf::Empty empty;
                        _stub->ReduceDone(&context, request, &empty);
                    }else {
                        std::cout << "Reduce Task " << response.filename() << " Error !" << std::endl;
                    }
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
                nonblock_map = true;
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
                nonblock_reduce = true;
                reduceCV.notify_all();
                for(int i = 0; i < ReduceNumber; ++i) {
                    ReduceThreads[i].join();
                }
                std::cout << "Reduce Client Task Finish !" << std::endl;
                break;
            }else if( notification.task_type() == mapreduce::TaskNotification::REDUCE ) {
                std::cout << "Reduce Task " << std::endl;
                ReduceTaskNumer.fetch_add(1, std::memory_order_release);
                reduceCV.notify_one();
            }
        }
    }

    std::multimap<std::string, int> shuffle(std::string reduceID) {

        std::string path = "../tmpFiles/";
        std::vector<std::string> files;
        /* 遍历Path文件里面的所有文件，得到Map Tmp文件 */
        DIR* dir = opendir(path.c_str());
        if(!dir) {
            std::cerr << "无法打开目录： " << path << std::endl;
        }
        struct dirent* entry;
        std::string fileN = "tmp_Reduce_" + reduceID;
        while( (entry = readdir(dir)) != nullptr ) {
            if(strncmp(entry->d_name, fileN.c_str(), fileN.size() )) {
                files.push_back(path + entry->d_name);
            }
        }
        closedir(dir);

        std::multimap<std::string, int> res;
        for(int i = 0; i < files.size(); ++i) {
            std::string filename = files[i];
            std::ifstream file (filename.c_str());
            if(!file.is_open()) {
                std::cout << "Open File : " << filename << " Error " << std::endl;
                return std::multimap<std::string, int>();
            }
            KVPair kv;
            while(file >> kv.Key >> kv.value) {
                res.insert(std::make_pair(kv.Key, kv.value));
            }
        }
        return res;
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

    void writeMapDisk(std::vector<std::vector<KVPair>>& reducefiles, int ID) {
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

    bool writeReduceDisk(std::map<std::string, int>& res, std::string ID) {
        std::string filename = "../OutputFiles/Reduce_" + ID + ".txt";
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
        if(fd == -1) {
            std::cout << "Open File : " << filename << " Error " << std::endl;
            return false;    
        }
        for(auto& kv : res) {
            std::string tmp = kv.first + " " + std::to_string(kv.second) + "\n";
            write(fd, tmp.c_str(), tmp.size());
        }
        close(fd);
        return true;
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




