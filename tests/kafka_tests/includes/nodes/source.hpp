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
#include "../util/tuple.hpp"
#include "../util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> sent_tuples;
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id

/**
 *  @class Kafka_Source_Functor
 *
 *  @brief Define the logic of the Source
 */
class Kafka_Source_Functor {
private:
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
    Kakfa_Source_Functor(const vector<tuple_t>& _dataset,
                   const int _rate,
                   const unsigned long _app_start_time,
                   const size_t _batch_size):
            rate(_rate),
            app_start_time(_app_start_time),
            current_time(_app_start_time),
            next_tuple_idx(0),
            generations(0),
            generated_tuples(0),
            batch_size(_batch_size)
    {
        dataset = _dataset; // be careful, here there is a copy. Maybe it would be better to copy only the pointer or to use move semantics...
        interval = 1000000L; // 1 second (microseconds)
    }

    /** 
     *  @brief Generation function of the input stream
     *  
     *  @param shipper Source_Shipper object used for generating inputs
     */ 
    void operator()(std::optional<std::reference_wrapper<RdKafka::Message>> msg, Source_Shipper<tuple_t> &shipper)
    {
        std::string tmp;
        current_time = current_time_nsecs(); // get the current time
        // generation loop
        while (current_time - app_start_time <= app_run_time)
        {
            if (next_tuple_idx == 0) {
                generations++;
            }
            //tuple_t t(dataset.at(next_tuple_idx));
            if ((batch_size > 0) && (generated_tuples % batch_size == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            if (batch_size == 0) {
                current_time = current_time_nsecs(); // get the new current time
            }
            //t.ts = current_time;
            //get the payload aka string of the parsed file via kafka
            tmp = static_cast<const char *>(msg->get().payload());

            //PARSE

            regex rgx("\\s+"); // regex quantifier (matches one or many whitespaces)
            sregex_token_iterator iter(line.begin(), line.end(), rgx, -1);
            sregex_token_iterator end;
            while (iter != end) {
                tokens.push_back(*iter);
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
                // insert the key device_id in the map (if it is not present)
                if (key_occ.find(get<DEVICE_ID_FIELD>(r)) == key_occ.end()) {
                    key_occ.insert(make_pair(get<DEVICE_ID_FIELD>(r), 0));
                }
            }

            //CREATE TUPLE

            tuple_t t;
            // select the value of the field the user chose to monitor (parameter set in constants.hpp)
            if (_field == TEMPERATURE) {
                t.property_value = get<TEMP_FIELD>(t);
            }
            else if (_field == HUMIDITY) {
                t.property_value = get<HUMID_FIELD>(t);
            }
            else if (_field == LIGHT) {
                t.property_value = get<LIGHT_FIELD>(t);
            }
            else if (_field == VOLTAGE) {
                t.property_value = get<VOLT_FIELD>(t);
            }
            t.incremental_average = 0;
            if (num_keys > 0) {
                t.key = dist(rng);
            }
            else {
                t.key = get<DEVICE_ID_FIELD>(t);
            }
            t.ts = 0L;

            //END PARSE SEND TUPLE
            shipper.pushWithTimestamp(std::move(t), current_time); // send the next tuple
            generated_tuples++;
            next_tuple_idx = (next_tuple_idx + 1) % dataset.size();   // index of the next tuple to be sent (if any)
            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
        }
        sent_tuples.fetch_add(generated_tuples); // save the number of generated tuples
    }

    ~Kafka_Source_Functor() {}
};

#endif //SPIKEDETECTION_LIGHT_SOURCE_HPP
