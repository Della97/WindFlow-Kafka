#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <kafka/windflow_kafka.hpp>

#include "../includes/util/tuple.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/nodes/detector.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/nodes/average_calculator_map.hpp"


#include <chrono>
#include <thread>


using namespace std;
using namespace ff;
using namespace wf;

bool first = true;
int  succes = 0;
int  failed = 0;
// type of the input records: < date_value, time_value, epoch_value, device_id_value, temp_value, humid_value, light_value, voltage_value>
using record_t = tuple<string, string, int, int, double, double, double, double>;

// global variables
vector<record_t> parsed_file;               // contains data extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources
vector<string> tokens;
vector<string> tuples_str;
int tmp = 0;

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
            // process file line
            int token_count = 0;
            vector<string> tokens;
            regex rgx("\\s+"); // regex quantifier (matches one or many whitespaces)
            sregex_token_iterator iter(line.begin(), line.end(), rgx, -1);
            sregex_token_iterator end;
            while (iter != end) {
                tokens.push_back(*iter);
                tmp++;
                token_count++;
                iter++;
            }
            // a record is valid if it contains at least 8 values (one for each field of interest)
            if (token_count >= 8) {
                // save parsed file
                record_t r(tokens.at(DATE_FIELD),
                           tokens.at(TIME_FIELD),
                           atoi(tokens.at(EPOCH_FIELD).c_str()),
                           atoi(tokens.at(DEVICE_ID_FIELD).c_str()),
                           atof(tokens.at(TEMP_FIELD).c_str()),
                           atof(tokens.at(HUMID_FIELD).c_str()),
                           atof(tokens.at(LIGHT_FIELD).c_str()),
                           atof(tokens.at(VOLT_FIELD).c_str()));
                parsed_file.push_back(r);
                // insert the key device_id in the map (if it is not present)
                if (key_occ.find(get<DEVICE_ID_FIELD>(r)) == key_occ.end()) {
                    key_occ.insert(make_pair(get<DEVICE_ID_FIELD>(r), 0));
                }
            }
            else
                incomplete_records++;

            all_records++;
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
        auto record = parsed_file.at(next_tuple_idx);
        tuple_t t;
        // select the value of the field the user chose to monitor (parameter set in constants.hpp)
        if (_field == TEMPERATURE) {
            t.property_value = get<TEMP_FIELD>(record);
        }
        else if (_field == HUMIDITY) {
            t.property_value = get<HUMID_FIELD>(record);
        }
        else if (_field == LIGHT) {
            t.property_value = get<LIGHT_FIELD>(record);
        }
        else if (_field == VOLTAGE) {
            t.property_value = get<VOLT_FIELD>(record);
        }
        t.incremental_average = 0;
        if (num_keys > 0) {
            t.key = dist(rng);
        }
        else {
            t.key = get<DEVICE_ID_FIELD>(record);
        }
        t.ts = 0L;
        dataset.insert(dataset.end(), t);
    }
}

//TEMP


std::string serialize_t(tuple_t in) {
    std::string one = std::to_string(in.property_value);
    std::string two= std::to_string(in.incremental_average);
    std::string three = std::to_string(in.key);
    std::string four = std::to_string(in.ts);
    std::string tmp = one + "+" + two + "+" + three + "+" + four;
    return tmp;
}

void create_string() {

    for (int next_tuple_idx = 0; next_tuple_idx < parsed_file.size(); next_tuple_idx++) {
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

    string file_path = "/home/dbmatteo/git/WindFlow-Kafka/Datasets/SD/sensors.dat";
    ifstream file(file_path);
    int count = 0;
    parse_dataset(file_path);
    create_tuples(num_keys);
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
    std::cout << range << std::endl;
    std::cout << "Producer created: " << producer->name() << std::endl;

    //kafka//

    start_time = current_time_nsecs();
    current_time = current_time_nsecs();
    index = 0;
    next_tuple_idx = 0;
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    while (current_time - start_time <= (app_run_time)) {
        //serialize tuple
        //send tuple
        //std::cout << tuples_str.at(next_tuple_idx).c_str() << std::endl;
        auto record = dataset.at(next_tuple_idx);
        record.ts = current_time_nsecs();
        auto messagge = serialize_t(record);
        std::cout << messagge << std::endl;
        RdKafka::ErrorCode err = producer->produce("sd", //topic
                                                RdKafka::Topic::PARTITION_UA,  //partition
                                                RdKafka::Producer::RK_MSG_COPY, // Copy payload,
                                                const_cast<char *>(messagge.c_str()), //payload
                                                messagge.size(),        //
                                                NULL, 0,  //
                                                0,        //
                                                NULL);    //

        producer->poll(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
