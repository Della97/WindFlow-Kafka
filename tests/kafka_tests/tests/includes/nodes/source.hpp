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

    // time variables
    unsigned long app_start_time;   // application start time
    unsigned long current_time;
    unsigned long interval;
    size_t batch_size;

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
            generated_tuples(0)
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
        tuple_t t;
        std::uniform_int_distribution<> dist(0, 1);
        mt19937 rng;
        rng.seed(0);
        uint64_t next_ts = 0;
        current_time = current_time_nsecs(); // get the current time

        if (msg) {
            arrived++;
            source_arrived_tuple++;
            current_time = current_time_nsecs(); // get the current time
            if (current_time - app_start_time > app_run_time) {
                cout << "COUNT: " << count << endl;
                return false;
            }
            string tmp = static_cast<const char *>(msg->get().payload());
            int token_count = 0;
            vector<string> tokens;
            regex rgx("\\s+");
            sregex_token_iterator iter(tmp.begin(), tmp.end(), rgx, -1);
            sregex_token_iterator end;
            while (iter != end)
            {
                tokens.push_back(*iter);
                token_count++;
                iter++;
            }
            
            if (token_count >= 8) {
                //create record_t
                record_t r(tokens.at(DATE_FIELD),
                           tokens.at(TIME_FIELD),
                           atoi(tokens.at(EPOCH_FIELD).c_str()),
                           atoi(tokens.at(DEVICE_ID_FIELD).c_str()),
                           atof(tokens.at(TEMP_FIELD).c_str()),
                           atof(tokens.at(HUMID_FIELD).c_str()),
                           atof(tokens.at(LIGHT_FIELD).c_str()),
                           atof(tokens.at(VOLT_FIELD).c_str()));
                //create tuple
                if (_field == TEMPERATURE) {
                    t.property_value = get<TEMP_FIELD>(r);
                }
                else if (_field == HUMIDITY) {
                    t.property_value = get<HUMID_FIELD>(r);
                }
                else if (_field == LIGHT) {
                    t.property_value = get<LIGHT_FIELD>(r);
                }
                else if (_field == VOLTAGE) {
                    t.property_value = get<VOLT_FIELD>(r);
                }
                t.incremental_average = 0;
                t.key = get<DEVICE_ID_FIELD>(r);
                t.ts = 0L;

                //std::cout << "count: " << count << " MSG: " << t.key << std::endl;
                shipper.pushWithTimestamp(std::move(t), next_ts);
                count++;
                source_sent_tuple++;
                if (rate != 0) { // active waiting to respect the generation rate
                    long delay_nsec = (long) ((1.0d / rate) * 1e9);
                    active_delay(delay_nsec);
                }
                next_ts++;
            }
            return true;
        } else {
            current_time = current_time_nsecs(); // get the current time
            if (current_time - app_start_time > app_run_time) {
                cout << "COUNT: " << count << endl;
                return false;
            }
            //std::cout << "Received MSG as NULLPTR " << std::endl;
            return true;
        }
    }

    ~Kafka_Source_Functor() {}
};

#endif //SPIKEDETECTION_LIGHT_SOURCE_HPP