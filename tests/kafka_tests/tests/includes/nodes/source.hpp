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


/**
 *  @class Kafka_Source_Functor
 *
 *  @brief Define the logic of the Source
 */
class Kafka_Source_Functor
{
private:
    int count = 0;
public:
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

        if (msg) {
            if (count == 60000) {
                std::cout << "ARRIVATI 60000 messaggi" << std::endl;
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
                next_ts++;
            }
            return true;
        } else {
            //std::cout << "Received MSG as NULLPTR " << std::endl;
            return true;
        }
    }

    ~Kafka_Source_Functor() {}
};

#endif //SPIKEDETECTION_LIGHT_SOURCE_HPP
