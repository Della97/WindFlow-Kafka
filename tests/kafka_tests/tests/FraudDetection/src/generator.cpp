#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <kafka/windflow_kafka.hpp>

#include "../includes/util/tuple.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/util/metric_group.hpp"
#include "../includes/util/metric.hpp"
#include "../includes/util/result.hpp"
#include "../includes/util/sampler.hpp"
#include "../includes/util/tuple.hpp"
#include "../includes/markov_model_prediction/markov_model.hpp"
#include "../includes/markov_model_prediction/prediction.hpp"
#include "../includes/markov_model_prediction/model_based_predictor.hpp"


#include <chrono>
#include <thread>


using namespace std;
using namespace ff;
using namespace wf;

using count_key_t = pair<size_t, uint64_t>;
using key_map_t = unordered_map<string, count_key_t>;

// global variables
key_map_t entity_key_map;                   // contains a mapping between string keys and integer keys for each entity_id
size_t entity_unique_key = 0;               // unique integer key
vector<pair<string, string>> parsed_file;   // contains strings extracted from the input file

bool first = true;
int  succes = 0;
int  failed = 0;
// type of the input records: < date_value, time_value, epoch_value, device_id_value, temp_value, humid_value, light_value, voltage_value>
using record_t = tuple<string, string, int, int, double, double, double, double>;

// global variables
vector<tuple_t> dataset;                    // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources
vector<string> tokens;
vector<string> tuples_str;
int tmp = 0;

/**
 *  @brief Map keys and parse the input file
 *
 *  This method assigns to each string key entity_id a unique integer key (required by the current
 *  implementation of WindFlow). Moreover, the file is parsed and saved in memory.
 *
 *  @param file_path the path of the input dataset file
 *  @param split_regex the regular expression used to split the lines of the file
 *         (e.g. for a file input.csv the regular expression to be used is ",")
 */
void map_and_parse_dataset(const string& file_path, const string& split_regex) {
    ifstream file(file_path);
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            string entity_id = line.substr(0, line.find(split_regex));
            string record = line.substr(line.find(split_regex) + 1, line.size());
            // map keys
            if (entity_key_map.find(entity_id) == entity_key_map.end()) { // the key is not present in the hash map
                entity_key_map.insert(make_pair(entity_id, make_pair(entity_unique_key, 0)));
                entity_unique_key++;
            }
            // save parsed file
            parsed_file.insert(parsed_file.end(), make_pair(entity_id, record));
        }
        file.close();
    }
}


/**
 *  @brief Process parsed data and create all the tuples
 *
 *  The created tuples are maintained in memory. The source node will generate the stream by
 *  reading all the tuples from main memory.
 */
void create_tuples(int num_keys)
{
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, num_keys-1);
    mt19937 rng;
    rng.seed(0);
    for (int next_tuple_idx = 0; next_tuple_idx < parsed_file.size(); next_tuple_idx++) {
        // create tuple
        auto tuple_content = parsed_file.at(next_tuple_idx);
        tuple_t t;
        t.entity_id = tuple_content.first;
        t.record = tuple_content.second;
        if (num_keys == 0) {
            t.key = (entity_key_map.find(t.entity_id)->second).first;
        }
        else {
            t.key = dist(rng);
        }
        //t.id = ((entity_key_map.find(t.entity_id))->second).second++;
        dataset.insert(dataset.end(), t);
    }
}

//TEMP


std::string serialize_t(tuple_t in) {
    std::string one = in.entity_id;
    std::string two = in.record;
    std::string three = std::to_string(in.key);
    std::string four = std::to_string(in.ts);
    std::string tmp = one + "+" + two + "+" + three + "+" + four;
    return tmp;
}

void create_string() {

    for (int next_tuple_idx = 0; next_tuple_idx < dataset.size(); next_tuple_idx++) {
        auto record = dataset.at(next_tuple_idx);
        tuples_str.push_back(serialize_t(record));
    }
}


int main(int argc, char* argv[]) {
        //KAFKA//
    std::string broker = "131.114.3.249:9092";
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;
    RdKafka::Producer *producer;
    int index = 0;
    int num_keys = 0;

    unsigned long current_time;
    unsigned long start_time;
    unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

    string file_path = "/home/dbmatteo/git/WindFlow-Kafka/Datasets/FD/credit-card.dat";
    ifstream file(file_path);
    int count = 0;
    map_and_parse_dataset(file_path, ",");
    create_tuples(num_keys);
    std::cout << dataset.size() << std::endl;
    create_string();
    int range = dataset.size();
    int next_tuple_idx = 0;

    //SET UP PRODUCER
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

    start_time = current_time_nsecs();
    current_time = current_time_nsecs();
    index = 0;
    next_tuple_idx = 0;
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    while (current_time - start_time <= app_run_time)
    	{

        auto record = dataset.at(next_tuple_idx);
        record.ts = current_time_nsecs();
        auto messagge = serialize_t(record);

    		RdKafka::ErrorCode err = producer->produce("fd", //topic
                                                RdKafka::Topic::PARTITION_UA,  //partition
                                                RdKafka::Producer::RK_MSG_COPY, // Copy payload,
                                                const_cast<char *>(messagge.c_str()), //payload
                                                messagge.size(),        //
                                                NULL, 0,  //
                                                0,        //
                                                NULL);    //

            producer->poll(0);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        next_tuple_idx = (next_tuple_idx + 1) % dataset.size();   // index of the next tuple to be sent (if any)
        count++;
        index++;
        current_time = current_time_nsecs();
    	}
    volatile unsigned long end_time_main_usecs = current_time_usecs();

            producer->poll(0);
            //KAFKA SEND DATA
    std::cout << "COUNT (AFTER == TIMEOUT): " << count << std::endl;
    std::cout << "CALLBACK COUNT DONE (AFTER == TIMEOUT): " << succes << std::endl;
    std::cout << "CALLBACK COUNT FAILED (AFTER == TIMEOUT): " << failed << std::endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    std::cout << "ELAPSED TIME: " << elapsed_time_seconds << std::endl;
    double throughput = count / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
}
