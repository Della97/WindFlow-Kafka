/**
 *  @file    fd.cpp
 *  @author  Alessandra Fais
 *  @date    11/01/2020
 *
 *  @brief Main of the FraudDetection application
 */
#include <cmath>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <ff/ff.hpp>
#include <kafka/windflow_kafka.hpp>

#include "../includes/nodes/sink.hpp"
//#include "../includes/nodes/sink_kafka.hpp"
#include "../includes/nodes/source.hpp"
#include "../includes/util/tuple.hpp"
#include "../includes/util/result.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/nodes/predictor.hpp"

using namespace std;
using namespace ff;
using namespace wf;

using count_key_t = pair<size_t, uint64_t>;
using key_map_t = unordered_map<string, count_key_t>;

// global variables
key_map_t entity_key_map;                   // contains a mapping between string keys and integer keys for each entity_id
size_t entity_unique_key = 0;               // unique integer key
vector<pair<string, string>> parsed_file;   // contains strings extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources

//***********************SOURCE*************************
atomic<long> source_arrived_tuple;                   // total number of tuples sent by all the sources
atomic<long> source_sent_tuple;                   // total number of tuples sent by all the sources


//***********************SINK*************************
atomic<long> sink_arrived_tuple;                   // total number of tuples sent by all the sources

int tmp = 0;

// closing functor (stub)
class closing_functor
{
public:
    void operator()(KafkaRuntimeContext &r) {}
};


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

// Main
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    string file_path;
    size_t source_par_deg = 0;
    size_t predictor_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    sent_tuples = 0;
    long sampling = 0;
    bool chaining = false;
    size_t batch_size = 0;
    size_t num_keys = 0;
    closing_functor c_functor;
    if (argc == 11 || argc == 12) {
        while ((option = getopt_long(argc, argv, "r:k:s:p:b:c:", long_opts, &index)) != -1) {
            file_path = _input_file;
            switch (option) {
                case 'r': {
                    rate = atoi(optarg);
                    break;
                }
                case 's': {
                    sampling = atoi(optarg);
                    break;
                }
                case 'b': {
                    batch_size = atoi(optarg);
                    break;
                }
                case 'k': {
                    num_keys = atoi(optarg);
                    break;
                }
                case 'p': {
                    vector<size_t> par_degs;
                    string pars(optarg);
                    stringstream ss(pars);
                    for (size_t i; ss >> i;) {
                        par_degs.push_back(i);
                        if (ss.peek() == ',')
                            ss.ignore();
                    }
                    if (par_degs.size() != 3) {
                        printf("Error in parsing the input arguments\n");
                        exit(EXIT_FAILURE);
                    }
                    else {
                        source_par_deg = par_degs[0];
                        predictor_par_deg = par_degs[1];
                        sink_par_deg = par_degs[2];
                    }
                    break;
                }
                case 'c': {
                    chaining = true;
                    break;
                }
                default: {
                    printf("Error in parsing the input arguments\n");
                    exit(EXIT_FAILURE);
                }
            }
        }
    }
    else if (argc == 2) {
        while ((option = getopt_long(argc, argv, "h", long_opts, &index)) != -1) {
            switch (option) {
                case 'h': {
                    printf("Parameters: --rate <value> --keys <value> --sampling <value> --batch <size> --parallelism <nSource,nPredictor,nSink> [--chaining]\n");
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }
    else {
        printf("Error in parsing the input arguments\n");
        exit(EXIT_FAILURE);
    }
    /// data pre-processing
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    cout << "Executing FraudDetection with parameters:" << endl;
    if (rate != 0) {
        cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tupes/second" << endl;
    }
    cout << "  * batch size: " << batch_size << endl;
    cout << "  * sampling: " << sampling << endl;
    cout << "  * source: " << source_par_deg << endl;
    cout << "  * predictor: " << predictor_par_deg << endl;
    cout << "  * sink: " << sink_par_deg << endl;
    cout << "  * topology: source -> predictor -> sink" << endl;
    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::INGRESS_TIME);
    if (!chaining) { // no chaining
        /// create the operators
        Kafka_Source_Functor source_functor(app_start_time, rate);
        Kafka_Source source = KafkaSource_Builder(source_functor)
                                .withName("kafka-source")
                                .withOutputBatchSize(batch_size)
                                .withClosingFunction(c_functor)
                                .withBrokers("131.114.3.249:9092")
                                .withTopics("fd")
                                .withGroupID("gruppo")
                                .withAssignmentPolicy("roundrobin")
                                .withIdleness(chrono::milliseconds(500))
                                .withParallelism(1)
                                .withOffsets(-1)
                                .build();
        Predictor_Functor predictor_functor(app_start_time);
        FlatMap predictor = FlatMap_Builder(predictor_functor)
                                .withParallelism(predictor_par_deg)
                                .withName(predictor_name)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .withOutputBatchSize(batch_size)
                                .build();
        Kafka_Sink_Functor sink_functor(sampling, app_start_time);
        Kafka_Sink sink = KafkaSink_Builder(sink_functor)
                        .withName("sink1")
                        .withParallelism(1)
                        .withBrokers("131.114.3.249:9092")
                        .build();

        /// create the application
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is disabled" << endl;
        mp.add(predictor);
        mp.add_sink(sink);
    }
    else { // chaining
        /// create the operators
        Kafka_Source_Functor source_functor(app_start_time, rate);
        Kafka_Source source = KafkaSource_Builder(source_functor)
                                .withName("kafka-source")
                                .withOutputBatchSize(0)
                                .withClosingFunction(c_functor)
                                .withBrokers("131.114.3.249:9092")
                                .withTopics("fd")
                                .withGroupID("gruppo")
                                .withAssignmentPolicy("roundrobin")
                                .withIdleness(chrono::milliseconds(500))
                                .withParallelism(1)
                                .withOffsets(-1)
                                .build();
        Predictor_Functor predictor_functor(app_start_time);
        FlatMap predictor = FlatMap_Builder(predictor_functor)
                                .withParallelism(predictor_par_deg)
                                .withName(predictor_name)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .build();
        Kafka_Sink_Functor sink_functor(sampling, app_start_time);
        Kafka_Sink sink = KafkaSink_Builder(sink_functor)
                        .withName("sink1")
                        .withParallelism(1)
                        .withBrokers("131.114.3.249:9092")
                        .build();

        /// create the application
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is enabled" << endl;
        mp.chain(predictor);
        mp.chain_sink(sink);
    }
    cout << "Executing topology" << endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    cout << "Exiting" << endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = source_arrived_tuple / 60;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    cout << "Elapsed time: " << elapsed_time_seconds << endl;
    cout << "*****************************" << endl;

    cout << "****************SOURCE***************" << endl;
    cout << "Source_arrived_tuple (FROM KAFKA) : " << source_arrived_tuple << endl;
    cout << "Source_sent_tuple (TO AVG) : " << source_sent_tuple << endl;
    cout << "*****************************" << endl;
    if (source_sent_tuple != 0) {
        cout << "****************SINK***************" << endl;
        cout << "Sink_Arrived_tuple (FROM PREV NODE): " << sink_arrived_tuple << endl;
        cout << "*****************************" << endl;
    }

    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    return 0;
}
