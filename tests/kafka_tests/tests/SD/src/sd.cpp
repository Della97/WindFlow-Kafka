/** 
 *  @file    sd.cpp
 *  @author  Gabriele Mencagli
 *  @date    13/01/2020
 *  
 *  @brief Main of the SpikeDetection application
 */ 

#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <kafka/windflow_kafka.hpp>

#include "../includes/util/tuple.hpp"
#include "../includes/nodes/sink.hpp"
#include "../includes/nodes/source.hpp"
#include "../includes/nodes/source1.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/nodes/detector.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/nodes/average_calculator_map.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// type of the input records: < date_value, time_value, epoch_value, device_id_value, temp_value, humid_value, light_value, voltage_value>
using record_t = tuple<string, string, int, int, double, double, double, double>;

// global variables
vector<record_t> parsed_file;               // contains data extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id

//***********************SOURCE*************************
atomic<long> source_arrived_tuple;                   // total number of tuples sent by all the sources
atomic<long> source_sent_tuple;                   // total number of tuples sent by all the sources
//***********************SOURCE1*************************
atomic<long> source1_arrived_tuple;                   // total number of tuples sent by all the sources
atomic<long> source1_sent_tuple;                  // total number of tuples sent by all the sources

//***********************AVG CALC*************************
atomic<long> avg_calc_arrived_tuple;                   // total number of tuples sent by all the sources

//***********************DETECTOR*************************
atomic<long> detector_arrived_tuple;                   // total number of tuples sent by all the sources

//***********************SINK*************************
atomic<long> sink_arrived_tuple;                   // total number of tuples sent by all the sources

int tmp = 0;

// closing functor (stub)
class closing_functor
{
public:
    void operator()(KafkaRuntimeContext &r) {}
};



// Main
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    string file_path;
    size_t source_par_deg = 0;
    size_t average_par_deg = 0;
    size_t detector_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    source_sent_tuple = 0;
    long sampling = 0;
    bool chaining = false;
    size_t batch_size = 0;
    int num_keys = 0;
    closing_functor c_functor;
    if (argc == 11 || argc == 12) {
        while ((option = getopt_long(argc, argv, "r:k:s:p:b:c:", long_opts, &index)) != -1) {
            file_path = _input_file;
            switch (option) {
                case 'r': {
                    rate = atoi(optarg);
                    break;
                }
                case 'k': {
                    num_keys = atoi(optarg);
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
                case 'p': {
                    vector<size_t> par_degs;
                    string pars(optarg);
                    stringstream ss(pars);
                    for (size_t i; ss >> i;) {
                        par_degs.push_back(i);
                        if (ss.peek() == ',')
                            ss.ignore();
                    }
                    if (par_degs.size() != 4) {
                        printf("Error in parsing the input arguments\n");
                        exit(EXIT_FAILURE);
                    }
                    else {
                        source_par_deg = par_degs[0];
                        average_par_deg = par_degs[1];
                        detector_par_deg = par_degs[2];
                        sink_par_deg = par_degs[3];
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
                    printf("Parameters: --rate <value> --keys <value> --sampling <value> --batch <size> --parallelism <nSource,nMoving-Average,nSpike-Detector,nSink> [--chaining]\n");
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }
    else {
        printf("Error in parsing the input arguments\n");
        exit(EXIT_FAILURE);
    }
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    //cout << "Executing SpikeDetection with parameters:" << endl;
    if (rate != 0) {
        //cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        //cout << "  * rate: full_speed tuples/second" << endl;
    }
    //cout << "  * batch size: " << batch_size << endl;
    //cout << "  * sampling: " << sampling << endl;
    //cout << "  * source: " << source_par_deg << endl;
    //cout << "  * moving-average: " << average_par_deg << endl;
    //cout << "  * spike-detector: " << detector_par_deg << endl;
    //cout << "  * sink: " << sink_par_deg << endl;
    //cout << "  * topology: source -> moving-average -> spike-detector -> sink" << endl;
    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    if (!chaining) { // no chaining
        /// create the operators
        Kafka_Source_Functor source_functor(app_start_time, rate);
        Kafka_Source source = Kafka_Source_Builder(source_functor)
                                .withName("kafka-source")
                                .withOutputBatchSize(1)
                                .withClosingFunction(c_functor)
                                .withBrokers("localhost:9092")
                                .withTopics("sd")
                                .withGroupID("gruppo")
                                .withAssignmentPolicy("roundrobin")
                                .withIdleness(500)
                                .withParallelism(1)
                                .withOffset(0)
                                .build();
        Average_Calculator_Map_Functor avg_calc_functor(app_start_time);
        Map average_calculator = Map_Builder(avg_calc_functor)
                .withParallelism(average_par_deg)
                .withName(avg_calc_name)
                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                .withOutputBatchSize(batch_size)
                .build();
        Detector_Functor detector_functor(app_start_time);
        Filter detector = Filter_Builder(detector_functor)
                .withParallelism(detector_par_deg)
                .withName(detector_name)
                .withOutputBatchSize(batch_size)
                .build();
        Kafka_Sink_Functor sink_functor(sampling, app_start_time);
        Kafka_Sink sink = Kafka_Sink_Builder(sink_functor)
                        .withName("sink1")
                        .withParallelism(1)
                        .withBrokers("localhost:9092")
                        .build();
        MultiPipe &mp = topology.add_source(source);
        //cout << "Chaining is disabled" << endl;
        mp.add(average_calculator);
        mp.add(detector);
        mp.add_sink(sink);
    }
    else { // chaining
        /// create the operators
        Kafka_Source_Functor source_functor(app_start_time, rate);
        Kafka_Source source = Kafka_Source_Builder(source_functor)
                                .withName("kafka-source")
                                .withOutputBatchSize(1)
                                .withClosingFunction(c_functor)
                                .withBrokers("localhost:9092")
                                .withTopics("sd")
                                .withGroupID("gruppo")
                                .withAssignmentPolicy("roundrobin")
                                .withIdleness(500)
                                .withParallelism(1)
                                .withOffset(0)
                                .build();
                                
        Average_Calculator_Map_Functor avg_calc_functor(app_start_time);
        Map average_calculator = Map_Builder(avg_calc_functor)
                .withParallelism(average_par_deg)
                .withName(avg_calc_name)
                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                .build();
        Detector_Functor detector_functor(app_start_time);
        Filter detector = Filter_Builder(detector_functor)
                .withParallelism(detector_par_deg)
                .withName(detector_name)
                .build();
        
        Kafka_Sink_Functor sink_functor(sampling, app_start_time);
        Kafka_Sink sink = Kafka_Sink_Builder(sink_functor)
                        .withName("sink1")
                        .withParallelism(1)
                        .withBrokers("localhost:9092")
                        .build();
                        
        MultiPipe &mp = topology.add_source(source);
        //cout << "Chaining is enabled" << endl;
        mp.chain(average_calculator);
        mp.chain(detector);
        mp.chain_sink(sink);
    }
    //cout << "Executing topology" << endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();

    cout << "Exiting" << endl;

    cout << "******************STATS***************" << endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = source_sent_tuple / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    cout << "Elapsed time: " << elapsed_time_seconds << endl;
    cout << "*****************************" << endl;
    cout << endl;

    cout << "****************SOURCE***************" << endl;
    cout << "Source_arrived_tuple (FROM KAFKA) : " << source_arrived_tuple << endl;
    cout << "Source_sent_tuple (TO AVG) : " << source_sent_tuple << endl;
    cout << "*****************************" << endl;
    if (source_sent_tuple != 0) {
        long loss1 = source_sent_tuple/avg_calc_arrived_tuple; 
        cout << "LOSS (source -> avg): " << loss1 << endl;
        cout << "****************AVG***************" << endl;
        cout << "Avg_arrived_tuple (FROM SOURCE) : " << avg_calc_arrived_tuple << endl;
        cout << "*****************************" << endl;
        long loss2 = avg_calc_arrived_tuple/detector_arrived_tuple; 
        cout << "LOSS (avg -> detector): " << loss2 << endl;
        cout << "****************DETECTOR***************" << endl;
        cout << "Detector_arrived_tuple (FROM AVG) : " << detector_arrived_tuple << endl;
        cout << "*****************************" << endl;
        long loss3 = detector_arrived_tuple/sink_arrived_tuple;
        cout << "LOSS (detector -> sink): " << loss3 << endl;
        cout << "****************SINK***************" << endl;
        cout << "Sink_Arrived_tuple (FROM DETECTOR): " << sink_arrived_tuple << endl;
        cout << "*****************************" << endl;
    }

    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    return 0;
}