/**
 *  @file    source.hpp
 *  @author  Alessandra Fais
 *  @date    18/06/2019
 *
 *  @brief Source node that generates the input stream
 *
 *  The source node generates the stream by reading the tuples from memory.
 */

#ifndef SPIKEDETECTION_LIGHT_SOURCE_HPP
#define SPIKEDETECTION_LIGHT_SOURCE_HPP

#include <fstream>
#include <regex>
#include <vector>
#include <iostream>
#include <sstream>
#include <ff/ff.hpp>
#include <kafka/windflow_kafka.hpp>
#include "../includes/util/tuple.hpp"
#include "../includes/nodes/sink.hpp"
#include "../includes/nodes/source.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/nodes/detector.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/nodes/average_calculator_map.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> source_arrived_tuple;                   // total number of tuples sent by all the sources
extern atomic<long> source_sent_tuple;                   // total number of tuples sent by all the sources


/**
 *  @class Kafka_Source_Functor
 *
 *  @brief Define the logic of the Source
 */
class Kafka_Source_Functor
{
private:
    long count = 0;
    long arrived = 0;
    vector<tuple_t> dataset;        // contains all the tuples
    int rate;                       // stream generation rate
    size_t next_tuple_idx;          // index of the next tuple to be sent
    int generations;                // counts the times the file is generated
    long generated_tuples;          // tuples counter
    int tmpp = 0;
    int ct = 0;

    // time variables
    unsigned long app_start_time;   // application start time
    unsigned long current_time;
    unsigned long interval;
    size_t batch_size;
    util::Sampler latency_sampler;

    /**
     *  @brief Add some active delay (busy-waiting function)
     *
     *  @param waste_time wait time in nanoseconds
     */
    void active_delay(unsigned long waste_time) {
        auto start_time = current_time_nsecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_nsecs();
            end = (end_time - start_time) >= waste_time;
        }
    }
public:
// Constructor
    Kafka_Source_Functor(const unsigned long _app_start_time,
                        const int _rate):
            app_start_time(_app_start_time),
            rate(_rate),
            next_tuple_idx(0),
            generations(0),
            generated_tuples(0),
            latency_sampler(100)
    {
        interval = 1000000L; // 1 second (microseconds)
    }
    /**
     *  @brief Generation function of the input stream
     *
     *  @param shipper Source_Shipper object used for generating inputs
     */
    bool operator()(std::optional<std::reference_wrapper<RdKafka::Message>> msg, Source_Shipper<tuple_t> &shipper)
    {
        //std::cout << static_cast<const char *>(msg->get().payload()) << std::endl;
        tuple_t t;
        std::uniform_int_distribution<> dist(0, 1);
        mt19937 rng;
        rng.seed(0);
        uint64_t next_ts = 0;
        current_time = current_time_nsecs(); // get the current time

       if (msg) {
            string tmp = static_cast<const char *>(msg->get().payload());
            tuple_t t;
            stringstream ss (tmp);
            string item;
            char delim = '+';

            arrived++;
            source_arrived_tuple++;
            current_time = current_time_nsecs(); // get the current time
            int pos = 0;
            while (getline (ss, item, delim)) {
                if (pos == 0) t.property_value = std::stod(item);  //cast to double
                else if (pos == 1) t.incremental_average = std::stod(item);  //cast to double
                else if (pos == 2) {
                    std::stringstream sstream(item);
                    sstream >> t.key;
                } else if (pos == 3) {
                    std::istringstream iss(item);
                    iss >> t.ts;
                }
                pos++;
            }
            t.ts = current_time_nsecs();
            //std::cout << "SOURCE: " <<  tuple_latency << std::endl;
            if (current_time - app_start_time > app_run_time) {
                return false;
            }
            shipper.pushWithTimestamp(std::move(t), next_ts);
            count++;
            source_sent_tuple++;
            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                //active_delay(delay_nsec);
            }
            next_ts++;
            return true;
       } else {
           current_time = current_time_nsecs(); // get the current time
            if (current_time - app_start_time > app_run_time) {
                //cout << "COUNT: " << count << endl;
                util::metric_group.add("latency", latency_sampler);
                return false;
            }
            //std::cout << "Received MSG as NULLPTR " << std::endl;
            return true;
       }
    }

    ~Kafka_Source_Functor() {}
};

#endif //SPIKEDETECTION_LIGHT_SOURCE_HPP
