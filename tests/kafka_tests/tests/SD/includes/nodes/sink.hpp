/**
 *  @file    sink.hpp
 *  @author  Alessandra Fais
 *  @date    18/07/2019
 *
 *  @brief Sink node that receives and prints the results
 */

#ifndef SPIKEDETECTION_SINK_HPP
#define SPIKEDETECTION_SINK_HPP

#include <algorithm>
#include <iomanip>
#include <ff/ff.hpp>
#include <kafka/windflow_kafka.hpp>
#include "../util/cli_util.hpp"
#include "../util/tuple.hpp"
#include "../util/sampler.hpp"
#include "../util/metric_group.hpp"


using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> sink_arrived_tuple;                   // total number of tuples sent by all the sources

/**
 *  @class Kafka_Sink_Functor
 *
 *  @brief Defines the logic of the Sink
 */
class Kafka_Sink_Functor {
private:
    long sampling;
    long arrived = 0;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t processed;                       // tuples counter
    // runtime information
    size_t parallelism;
    size_t replica_id;
    util::Sampler latency_sampler;
    int tmpp = 0;
    std::string msg;
public:

    /**
     *  @brief Constructor
     *
     *  @param _sampling sampling rate
     *  @param _app_start_time application starting time
     */
    Kafka_Sink_Functor(const long _sampling,
                 const unsigned long _app_start_time):
                 sampling(_sampling),
                 app_start_time(_app_start_time),
                 current_time(_app_start_time),
                 processed(0),
                 latency_sampler(_sampling) {}

    /**
     * @brief Send results to kafka and evaluate latency statistics
     *
     * @param t input tuple
     */
    wf::wf_kafka_sink_msg operator()(const tuple_t &out, KafkaRuntimeContext &rc) {

        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }

        // always evaluate latency when compiling with FF_BOUNDED_BUFFER MACRO set
        unsigned long tuple_latency = (current_time_nsecs() - (out).ts) / 1e03;
        //std::cout << "SINK: " <<  tuple_latency << std::endl;
        tmpp = tmpp + tuple_latency;
        processed++;        // tuples counter


        current_time = current_time_nsecs();
        //latency_sampler.add(tuple_latency, current_time);


        arrived++;
        sink_arrived_tuple++;
        wf::wf_kafka_sink_msg tmp;
        RdKafka::Producer *producer = rc.getProducer();
        msg = out.incremental_average;

        std::cout << "MEDIA SINK: " << tmpp / processed << std::endl;
        //std::cout << "MESSAGGIO: " << msg << std::endl;

        tmp.partition = rc.getReplicaIndex();
        tmp.payload = msg;
        tmp.topic = "output";
        return tmp;
    }
};

#endif //SPIKEDETECTION_SINK_HPP
