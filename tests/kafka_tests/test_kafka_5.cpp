#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <kafka/windflow_kafka.hpp>


#include <chrono>
#include <thread>


using namespace std;
using namespace ff;
using namespace wf;

bool first = true;
int  succes = 0;
int  failed = 0;
vector<string> tokens;

/** 
 *  @brief Parse the input file
 *  
 *  The file is parsed and saved in memory.
 *  
 *  @param file_path the path of the input dataset file
 */ 
void parse_dataset(const string& file_path) {
    ifstream file(file_path);
    if (file.is_open()) {
        size_t all_records = 0;         // counter of all records (dataset line) read
        size_t incomplete_records = 0;  // counter of the incomplete records
        string line;
        while (getline(file, line)) {
            tokens.push_back(line);
            all_records++;
        }
        file.close();
        //print_parsing_info(parsed_file, all_records, incomplete_records);
    }
}

class ExDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
    void dr_cb(RdKafka::Message &message)
    {
        /* If message.err() is non-zero the message delivery failed permanently
         * for the message. */
        if (message.err())
            failed++;
            //std::cerr << "% Message delivery failed: " << message.errstr() << std::endl;
        else
            succes++;
            //std::cerr << "% Message delivered to topic " << message.topic_name() << " [" << message.partition() << "] at offset " << message.offset() << std::endl;
    }
};

int main(int argc, char* argv[]) {
    //KAFKA//
    std::string broker = "localhost:9092";
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;
    RdKafka::Producer *producer;
    ExDeliveryReportCb ex_dr_cb;
    int index = 0;

    unsigned long current_time;
    unsigned long start_time;
    unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

    //SET UP PRODUCER
    //SET UP PRODUCER
        if (conf->set("bootstrap.servers", broker, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
        
        if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
        
        producer = RdKafka::Producer::create(conf, errstr);
        if (!producer) {
            std::cerr << "Failed to create producer: " << errstr << std::endl;
            exit(1);
        }
    std::cout << "Producer created: " << producer->name() << std::endl;
    //kafka//
    string file_path = "/home/della/git/WindFlow-Kafka/Datasets/SD/sensors.dat";
    ifstream file(file_path);
    int count = 0;
    parse_dataset(file_path);
    cout << "dataset size: " << tokens.size() << endl;
    start_time = current_time_nsecs();
    current_time = current_time_nsecs();
    index = 0;
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    while (current_time - start_time <= (app_run_time  + app_run_time)) {
        RdKafka::ErrorCode err = producer->produce("provatop", //topic
                                                RdKafka::Topic::PARTITION_UA,  //partition
                                                RdKafka::Producer::RK_MSG_COPY, // Copy payload,
                                                const_cast<char *>(tokens.at(index % tokens.size()).c_str()), //payload
                                                tokens.at(index % tokens.size()).size(),        //
                                                NULL, 0,  //
                                                0,        //
                                                NULL);    //
        producer->poll(0);
        cout << count << endl;
        //std::this_thread::sleep_for(std::chrono::microseconds(10));
        count++;
        index++;
        current_time = current_time_nsecs();        
    }
    volatile unsigned long end_time_main_usecs = current_time_usecs();
            //KAFKA SEND DATA
            /*
            RdKafka::ErrorCode err = producer->produce("test", //topic
                                                0,  //partition
                                                RdKafka::Producer::RK_MSG_COPY, // Copy payload,
                                                const_cast<char *>(line.c_str()), //payload
                                                line.size(),        //
                                                NULL, 0,  //
                                                0,        //
                                                NULL,     //
                                                NULL);    //
                                            */

            producer->poll(0);
            //std::this_thread::sleep_for(std::chrono::milliseconds(1));
            //KAFKA SEND DATA
    std::cout << "COUNT (AFTER == TIMEOUT): " << count << std::endl;
    std::cout << "CALLBACK COUNT DONE (AFTER == TIMEOUT): " << succes << std::endl;
    std::cout << "CALLBACK COUNT FAILED (AFTER == TIMEOUT): " << failed << std::endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = count / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
}