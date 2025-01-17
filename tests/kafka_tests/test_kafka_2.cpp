/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Matteo Della Bartola
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/*  
 *  Test 2 of Kafka operators in WindFlow
 */ 

#include<random>
#include<string>
#include<iostream>
#include<functional>
#include<windflow.hpp>
#include<kafka/windflow_kafka.hpp>

using namespace std;
using namespace wf;


// tuple_t struct
struct tuple_t
{
    int key;
    int value;
};



// Sink functor
class Kafka_Sink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // Constructor
    Kafka_Sink_Functor():
                 received(0),
                 totalsum(0) {}

    // operator()
    wf::wf_kafka_sink_msg operator()(const tuple_t &out, KafkaRuntimeContext &rc)
    {
        wf::wf_kafka_sink_msg tmp;
        RdKafka::Producer *producer = rc.getProducer();
        std::string msg = std::to_string(out.key) + "-producer-" + std::to_string(rc.getReplicaIndex());

        tmp.partition = rc.getReplicaIndex();
        tmp.payload = const_cast<char *>(msg.c_str());
        tmp.topic = "output";
        return tmp;
    }
};

// deserialization function (stub)
bool deser_func(std::optional<std::reference_wrapper<RdKafka::Message>> msg, Source_Shipper<tuple_t> &shipper /*, tuple_t &output*/)
{
    if (msg) {
        tuple_t out;
        uint64_t next_ts = 0;
        //printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char *>(msg->payload()));
        //out.value = atoi(static_cast<const char *>(msg->payload()));

        //DESER THE MSG
        out.value = atoi(static_cast<const char *>(msg->get().payload()));

        //CHECK IF END OF STREAM
        if (out.value == 0) {
            return false;
        }
    
        //PACK THE TUPLE_T
        out.key = atoi(static_cast<const char *>(msg->get().payload()));
        //std::cout << "[DESER] -> msg: " << out.value << std::endl;
        //PUSH FORWARD THE TUPLE
        shipper.pushWithTimestamp(std::move(out), next_ts);
        next_ts++;
        return true;
    } else {
        std::cout << "Received MSG as NULLPTR " << std::endl;
        return true;
    }
}

// closing logic (stub)
void closing_func(KafkaRuntimeContext &r) {}

// deserialization functor (stub)
class deser_functor
{
public:
    bool operator()(std::optional<std::reference_wrapper<RdKafka::Message>> msg, Source_Shipper<tuple_t> &shipper)
    {
        if (msg) {
            tuple_t out;
            uint64_t next_ts = 0;
            //printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char *>(msg->payload()));
            //out.value = atoi(static_cast<const char *>(msg->payload()));

            //DESER THE MSG
            out.value = atoi(static_cast<const char *>(msg->get().payload()));

            //CHECK IF END OF STREAM
            if (out.value == 0) {
                return false;
            }
    
            //PACK THE TUPLE_T
            out.key = atoi(static_cast<const char *>(msg->get().payload()));
            std::cout << "[DESER] -> msg: " << out.value << std::endl;
            //PUSH FORWARD THE TUPLE
            shipper.pushWithTimestamp(std::move(out), next_ts);
            next_ts++;
            return true;
        } else {
            //std::cout << "Received MSG as NULLPTR " << std::endl;
            return true;
        }
    }
};

// closing functor (stub)
class closing_functor
{
public:
    void operator()(KafkaRuntimeContext &r) {}
};

// main
int main()
{
    std::cout << "Test creazione Kafka_Source mediante costruttore raw" << std::endl;
    string name = "my_kafka_source";
    string brokers = "localhost";
    string groupid = "group";
    size_t outputBactchSize = 1;
    int parallelism = 6;
    RdKafka::Message *msg;
    std::vector<std::string> topics = { "items" };
    Source_Shipper<tuple_t> *shipper;
    std::vector<int> offset;
    RdKafka::Conf *cconf;
    RdKafka::Conf *tconf;
    int sink1_degree = 2;
    std::string topic1 = "test";
    std::string topic2 = "provatop";
    std::string topic3 = "topic";
    std::string strat = "roundrobin";
    std::vector<int> offsetss = {10, 10};

    std::cout << "QUII" << std::endl;
    Kafka_Source source1 = Kafka_Source(deser_func, name, outputBactchSize, brokers, topics, groupid, strat, 2000, parallelism, offsetss, closing_func);
    std::cout << "Creazione con funzioni -> OK!" << std::endl;

    auto deser_lambda = [](std::optional<std::reference_wrapper<RdKafka::Message>> msg, Source_Shipper<tuple_t> &shipper /*, tuple_t &output */) { return true; };
    auto closing_lambda = [](KafkaRuntimeContext &) { return; };

    Kafka_Source source2 = Kafka_Source(deser_lambda, name, outputBactchSize, brokers, topics, groupid, strat, 2000, parallelism, offsetss, closing_lambda);
    std::cout << "Creazione con lambda -> OK!" << std::endl;    

    deser_functor d_functor;
    closing_functor c_functor;
    Kafka_Source source3 = Kafka_Source(d_functor, name, outputBactchSize, brokers, topics, groupid, strat, 2000, parallelism, offsetss, c_functor);
    std::cout << "Creazione con funtori -> OK!" << std::endl;

    Kafka_Source source4 = Kafka_Source_Builder(deser_func)
                                .withName(name)
                                .withOutputBatchSize(outputBactchSize)
                                .withClosingFunction(closing_func)                //check previus with(closing_func)
                                .withBrokers(brokers)
                                .withTopics(topic1)
                                .withGroupID(groupid)
                                .withAssignmentPolicy(strat)
                                .withIdleness(2000)
                                .withParallelism(parallelism)
                                .withOffset(10)
                                .build();
    std::cout << "Creazione con builder tramite funzioni -> OK!" <<  std::endl;

    Kafka_Source source5 = Kafka_Source_Builder(deser_lambda)
                                .withName(name)
                                .withOutputBatchSize(outputBactchSize)
                                .withClosingFunction(closing_lambda)
                                .withBrokers(brokers)
                                .withTopics(topic1, topic2)
                                .withGroupID(groupid)
                                .withAssignmentPolicy(strat)
                                .withIdleness(2000)
                                .withParallelism(parallelism)
                                .withOffset(10, 10)
                                .build();
    std::cout << "Creazione con builder tramite lambda -> OK!" <<  std::endl;

    Kafka_Source source6 = Kafka_Source_Builder(d_functor)
                                .withName(name)
                                .withOutputBatchSize(outputBactchSize)
                                .withClosingFunction(c_functor)
                                .withBrokers("localhost:9092", "localhost:9093")
                                .withTopics("provatop", "test", "topic-name")
                                .withGroupID(groupid)
                                .withAssignmentPolicy(strat)
                                .withIdleness(2000)
                                .withParallelism(parallelism)
                                .withOffset(-1, -1, -1)
                                .build();
    std::cout << "Creazione con builder tramite funtori -> OK!" <<  std::endl;
    PipeGraph graph("test_tracing_1", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    MultiPipe &pipe = graph.add_source(source6);



        //SINK
    Kafka_Sink_Functor sink_functor;
    Kafka_Sink sink1 = Kafka_Sink_Builder(sink_functor)
                        .withName("sink1")
                        .withParallelism(sink1_degree)
                        .withBrokers("localhost:9093")
                        .build();
        pipe.chain_sink(sink1);
    graph.run();
    std::cout << "Exiting..." <<  std::endl;
    return 0;
}
