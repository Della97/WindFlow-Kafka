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
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            //std::cout << "DATA: " << line << std::endl;
            //KAFKA SEND DATA
            RdKafka::ErrorCode err = producer->produce("test", //topic
                                                0,  //partition
                                                RdKafka::Producer::RK_MSG_COPY, // Copy payload,
                                                const_cast<char *>(line.c_str()), //payload
                                                line.size(),        //
                                                NULL, 0,  //
                                                0,        //
                                                NULL,     //
                                                NULL);    //

            producer->poll(0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            //KAFKA SEND DATA
            count++;
        }
        file.close();
        std::cout << "COUNT: " << count << std::endl;
    }
    std::cout << "CALLBACK COUNT DONE: " << succes << std::endl;
    std::cout << "CALLBACK COUNT FAILED: " << failed << std::endl;
}