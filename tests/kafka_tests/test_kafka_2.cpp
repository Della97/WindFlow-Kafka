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
class Sink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // Constructor
    Sink_Functor():
                 received(0),
                 totalsum(0) {}

    // operator()
    void operator()(optional<tuple_t> &out)
    {
        if (out) {
            std::cout << "[SINK] -> Received: " << (*out).key << std::endl;
            std::cout << "[SINK] -> Received: " << (*out).value << std::endl;
        }
        else {
            std::cout << "[SINK] -> Received nothing: " << std::endl;
        }
    }
};

// deserialization function (stub)
bool deser_func(RdKafka::Message &msg, Source_Shipper<tuple_t> &shipper)
{
    tuple_t out;
    uint64_t next_ts = 0;
    //printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char *>(msg->payload()));
    //out.value = atoi(static_cast<const char *>(msg->payload()));
    out.value = atoi(static_cast<const char *>(msg.payload()));

    if (out.value == -1) {
        return false;
    }

    out.key = atoi(static_cast<const char *>(msg.payload()));
    std::cout << "[DESER] -> msg: " << out.value << std::endl;
    shipper.pushWithTimestamp(std::move(out), next_ts);
    next_ts++;
    //shipper.push(out);
    return true;
}

// closing logic (stub)
void closing_func(RuntimeContext &r) {}

// deserialization functor (stub)
class deser_functor
{
public:
    bool operator()(RdKafka::Message &msg, Source_Shipper<tuple_t> &shipper)
    {
        tuple_t out;
        std::cout << "Entered deser" << std::endl;
        //printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char *>(msg.));
        out.value = 0;
        //out.value = msg.payload();
        out.key = 0;
        shipper.push(out);
        return true;
    }
};

// closing functor (stub)
class closing_functor
{
public:
    void operator()(RuntimeContext &r) {}
};

// main
int main()
{
    std::cout << "Test creazione Kafka_Source mediante costruttore raw" << std::endl;
    string name = "my_kafka_source";
    string brokers = "localhost:9094";
    string groupid = "group";
    size_t outputBactchSize = 1;
    int parallelism = 2;
    RdKafka::Message *msg;
    std::vector<std::string> topics = { "items" };
    Source_Shipper<tuple_t> *shipper;
    int32_t offset = 0;
    RdKafka::Conf *cconf;
    RdKafka::Conf *tconf;
    std:int sink1_degree = 1;
    std::string topic1 = "test:0:1";
    std::string topic2 = "provatop*";
    std::string topic3 = "topic";
/*
    Kafka_Source source1 = Kafka_Source(deser_func, name, outputBactchSize, brokers, topics, groupid, parallelism, offset, closing_func);
    std::cout << "Creazione con funzioni -> OK!" << std::endl;

    auto deser_lambda = [](RdKafka::Message &msg, Source_Shipper<tuple_t> &shipper) { return true; };
    auto closing_lambda = [](RuntimeContext &) { return; };

    Kafka_Source source2 = Kafka_Source(deser_lambda, name, outputBactchSize, brokers, topics, groupid, parallelism, offset, closing_lambda);
    std::cout << "Creazione con lambda -> OK!" << std::endl;    

    deser_functor deser_functor;
    closing_functor closing_functor;
    Kafka_Source source3 = Kafka_Source(deser_functor, name, outputBactchSize, brokers, topics, groupid, parallelism, offset, closing_functor);
    std::cout << "Creazione con funtori -> OK!" << std::endl;

    std::cout << "Test creazione Kafka_Source mediante builder" << std::endl;
    Kafka_Source source4 = Kafka_Source_Builder(deser_func)
                                .withName(name)
                                .withOutputBatchSize(outputBactchSize)
                                .withClosingFunction(closing_func)
                                .withBrokers(brokers)
                                .withTopics(topic1)
                                .withGroupID(groupid)
                                .withPartition(parallelism)
                                .withOffset(offset)
                                .build();
    std::cout << "Creazione con builder tramite funzioni -> OK!" <<  std::endl;

    Kafka_Source source5 = Kafka_Source_Builder(deser_lambda)
                                .withName(name)
                                .withOutputBatchSize(outputBactchSize)
                                .withClosingFunction(closing_lambda)
                                .withBrokers(brokers)
                                .withTopics(topic1, topic2)
                                .withGroupID(groupid)
                                .withPartition(parallelism)
                                .withOffset(offset)
                                .build();
    std::cout << "Creazione con builder tramite lambda -> OK!" <<  std::endl;
*/

    closing_functor closing_functor;

    Kafka_Source source6 = Kafka_Source_Builder(deser_func)
                                .withName(name)
                                .withOutputBatchSize(outputBactchSize)
                                .withClosingFunction(closing_functor)
                                .withBrokers(brokers)
                                .withTopics(topic1, topic2)
                                .withGroupID(groupid)
                                .withPartition(parallelism)
                                .withOffset(offset)
                                .build();
    //std::cout << "Creazione con builder tramite funtori -> OK!" <<  std::endl;

    PipeGraph graph("test_tracing_1", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    MultiPipe &pipe = graph.add_source(source6);

        //SINK
    Sink_Functor sink_functor;
        Sink sink1 = Sink_Builder(sink_functor)
                        .withName("sink1")
                        .withParallelism(sink1_degree)
                        .build();
        pipe.chain_sink(sink1);
    graph.run();
    std::cout << "Exiting..." <<  std::endl;

    return 0;
}
