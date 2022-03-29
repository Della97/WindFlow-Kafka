#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include<kafka/windflow_kafka.hpp>

#include "includes/util/tuple.hpp"
#include "includes/nodes/sink.hpp"
#include "includes/nodes/source.hpp"
#include "includes/util/cli_util.hpp"
#include "includes/nodes/detector.hpp"
#include "includes/util/constants.hpp"
#include "includes/nodes/average_calculator_map.hpp"
//#include "includes/nodes/generate_kafka_datas.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// type of the input records: < date_value, time_value, epoch_value, device_id_value, temp_value, humid_value, light_value, voltage_value>
using record_t = tuple<string, string, int, int, double, double, double, double>;

// global variables
vector<record_t> parsed_file;               // contains data extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources

int main(int argc, char* argv[]) {
    //KAFKA//
    std::string broker = "localhost:9093";
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;
    RdKafka::Producer *producer;

    //SET UP PRODUCER
    if (conf->set("bootstrap.servers", broker, errstr) != RdKafka::Conf::CONF_OK) {
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
            /*
            RdKafka::ErrorCode err = producer->produce("output", //topic
                                                0,  //partition
                                                RdKafka::Producer::RK_MSG_COPY, // Copy payload,
                                                line, //payload
                                                line.size(),        //
                                                NULL, 0,  //
                                                0,        //
                                                NULL,     //
                                                NULL);    //
            producer->poll(0);
            */
            //KAFKA SEND DATA
            count++;
        }
        file.close();
        std::cout << "COUNT: " << count << std::endl;
    }
}