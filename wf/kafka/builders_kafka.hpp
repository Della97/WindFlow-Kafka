/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Matteo della Bartola
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

/** 
 *  @file    builders_kafka.hpp
 *  @author  Gabriele Mencagli and Matteo della Bartola
 *  
 *  @brief Builder classes used to create the WindFlow operators to communicate
 *         with Apache Kafka
 *  
 *  @section Builders-Kafka (Description)
 *  
 *  Builder classes used to create the WindFlow operators communicating with Apache Kafka.
 *  They are the Kafka_Source and Kafka_Sink operators.
 */ 

#ifndef BUILDERS_KAFKA_H
#define BUILDERS_KAFKA_H

/// includes
#include<chrono>
#include<vector>
#include<functional>
#include<string>
#include<basic.hpp>
#include<kafka/meta_kafka.hpp>
#include<kafka/kafkacontext.hpp>

struct createString {
    std::string strs;

    template<typename H>
    void add_strings(H first) {
        strs.append(first);
        strs.append(", ");
    }

    template <typename H, typename... Args>
    void add_strings(H first, Args... others) {
        strs.append(first);
        strs.append(", ");
        add_strings(others...);
    }
};

struct Sstring {
    std::vector<std::string> strs;

    template<typename G>
    void add_strings(G first) {
        strs.push_back(first);
    }

    template <typename G, typename... Args>
    void add_strings(G first, Args... others) {
        strs.push_back(first);
        add_strings(others...);
    }
};

struct Iint {
    std::vector<int> offsets;

    template<typename O>
    void add_ints(O first) {
        offsets.push_back(first);
    }

    template <typename O, typename... OSets>
    void add_ints(O first, OSets... others) {
        offsets.push_back(first);
        add_ints(others...);
    }
};

namespace wf {



template<typename kafka_deser_func_t>
class Kafka_Source_Builder
{
private:
    kafka_deser_func_t func; // deserialization logic of the Kafka_Source
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Kafka_Source functional logic
    static_assert(!(std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Kafka_Source_Builder:\n"
        "  Candidate 1 : bool(RdKafka::Message &, Source_Shipper<result_t> &)\n"
        "  Candidate 2 : bool(RdKafka::Message &, Source_Shipper<result_t> &, RuntimeContext &)\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (Kafka_Source_Builder):\n");
    using kafka_source_t = Kafka_Source<kafka_deser_func_t>; // type of the Kafka_Source to be created by the builder
    using closing_func_t = std::function<void(wf::KafkaRuntimeContext&)>; // type of the closing functional logic
    std::string name = "kafka_source"; // name of the Kafka_Source
    size_t parallelism; // parallelism of the Kafka_Source
    size_t outputBatchSize = 0; // output batch size of the Kafka_Source
    closing_func_t closing_func; // closing function logic of the Kafka_Source

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    Sstring topic;
    Iint offset;
    createString broker;
    int idleTime;
    std::vector< std::string > topics;
    std::string brokers;
    std::string groupid;
    std::string strat;
    int32_t partition;
    std::vector<int> offsets;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Kafka_Source (a function or a callable type)
     */ 
    Kafka_Source_Builder(kafka_deser_func_t _func):
                         func(_func) {}


    /** 
     *  \brief Set the name of the Kafka_Source
     *  
     *  \param _name of the Kafka_Source
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the output batch size of the Kafka_Source
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Kafka_Source
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Kafka_Source_Builder<kafka_deser_func_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_kafka_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Kafka_Source_Builder):\n"
            "  Candidate : void(KafkaRuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the Broker <--- cosa è un indirizzo un hostname???
     *  
     *  \param _brokers for kafka server
     *  \return a reference to the builder object
     */ 
    template <typename H, typename... Args>
    Kafka_Source_Builder<kafka_deser_func_t> &withBrokers(H first, Args... Ts)
    {
        broker.add_strings(first, Ts...);
        broker.strs.pop_back();
        broker.strs.pop_back();
        brokers = broker.strs;
        return *this;
    }

    /** 
     *  \brief Set the consumer groupid
     *  
     *  \param _groupid for the consumer
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withGroupID(std::string _groupid)   //merge group-id
    {
        groupid = _groupid;
        return *this;
    }

    /** 
     *  \brief Set the partition assignment strategy
     *  
     *  \param _strat for the assignment
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withAssignmentPolicy(std::string _strat)   //merge group-id
    {
        strat = _strat;
        return *this;
    }

    /** 
     *  \brief Set the idle time while fetching from broker
     *  
     *  \param _idelTime for the assignment
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withIdleness(int _idleTime)   //merge group-id
    {
        idleTime = _idleTime;
        return *this;
    }

    /** 
     *  \brief Set the topic partition
     *  
     *  \param _partition for the consumer
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withParallelism(int32_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the topic offset
     *  
     *  \param _offset for the consumer
     *  \return a reference to the builder object
     */ 

    template <typename O, typename... OSets>
    Kafka_Source_Builder<kafka_deser_func_t> &withOffset(O first, OSets... Os)
    {
        offset.add_ints(first, Os...);
        offsets = offset.offsets;
        return *this;
    }

    /** 
     *  \brief Set the topic
     *  
     *  \param _topics for the consumer
     *  \return a reference to the builder object
     */ 

    template <typename G, typename... Args>
    Kafka_Source_Builder<kafka_deser_func_t> &withTopics(G first, Args... Ts)
    {
        //std::vector<std::string> topics; <- declaration 
        topic.add_strings(first, Ts...);
        topics = topic.strs;
        return *this;
    }
    

    /** 
     *  \brief Create the Source
     *  
     *  \return a new Source instance
     */ 
    kafka_source_t build()
    {
        return kafka_source_t(func,
                              name,
                              outputBatchSize,
                              brokers,
                              topics,
                              groupid,
                              strat,
                              idleTime,
                              parallelism,
                              offsets,
                              closing_func);
    }
};

//**************************************************SINK_BUILDER*******************************************************

template<typename kafka_sink_func_t, typename key_t=empty_key_t>
class Kafka_Sink_Builder
{
private:
    template<typename T1, typename T2> friend class Kafka_Sink_Builder;
    kafka_sink_func_t func; // deserialization logic of the Kafka_Source
    using tuple_t = decltype(get_tuple_t_KafkaSink(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Kafka_Source functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Kafka_Sink_Builder:\n"
        "  Candidate 1 : void(std::optional<tuple_t> &)\n"
        "  Candidate 2 : void(std::optional<tuple_t> &, KafkaRuntimeContext &)\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Kafka_Sink_Builder):\n");
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>;
    using kafka_sink_t = Kafka_Sink<kafka_sink_func_t, key_extractor_func_t>; // type of the Kafka_Source to be created by the builder
    using kafka_closing_func_t = std::function<void(wf::KafkaRuntimeContext&)>; // type of the closing functional logic
    std::string name = "kafka_sink"; // name of the Kafka_Source
    size_t parallelism; // parallelism of the Kafka_Source
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD;
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); };
    kafka_closing_func_t closing_func = [](KafkaRuntimeContext &r) -> void { return; }; // closing function logic of the Kafka_Sink

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    Iint offset;
    int idleTime;
    std::vector< std::string > topics;
    std::string topic;
    std::string broker;
    std::string groupid;
    std::string strat;
    int32_t partition;
    std::vector<int> offsets;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Kafka_Source (a function or a callable type)
     */ 
    Kafka_Sink_Builder(kafka_sink_func_t _func):
                         func(_func) {}


    /** 
     *  \brief Set the name of the Kafka_Source
     *  
     *  \param _name of the Kafka_Source
     *  \return a reference to the builder object
     */ 
    Kafka_Sink_Builder<kafka_sink_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }


    /** 
     *  \brief Set the parallelism
     *  
     *  \param _parallelism for the sink
     *  \return a reference to the builder object
     */ 
    Kafka_Sink_Builder<kafka_sink_func_t> &withParallelism(int32_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the Broker <--- cosa è un indirizzo un hostname???
     *  
     *  \param _brokers for kafka server
     *  \return a reference to the builder object
     */ 
    Kafka_Sink_Builder<kafka_sink_func_t> &withBroker(std::string _broker)
    {
        broker = _broker;
        return *this;
    }

    /** 
     *  \brief Set the topic offset
     *  
     *  \param _offset for the producer
     *  \return a reference to the builder object
     */ 

    template <typename O, typename... OSets>
    Kafka_Sink_Builder<kafka_sink_func_t> &withOffset(O first, OSets... Os)
    {
        offset.add_ints(first, Os...);
        offsets = offset.offsets;
        return *this;
    }

    /** 
     *  \brief Set the topic
     *  
     *  \param _topic for the producer
     *  \return a reference to the builder object
     */ 


    Kafka_Sink_Builder<kafka_sink_func_t> &withTopic(std::string _topic)
    {
        topic = _topic;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Kafka_Source
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Kafka_Sink_Builder<kafka_sink_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_kafka_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Kafka_Sink_Builder):\n"
            "  Candidate : void(KafkaRuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    

/**
 * kafka_sink_func_t _func,
         key_extractor_func_t _key_extr,
         size_t _parallelism,
         std::string _name,
         Routing_Mode_t _input_routing_mode,
         std::function<void(RuntimeContext &)> _closing_func):
*/

    /** 
     *  \brief Create the Source
     *  
     *  \return a new Source instance
     */ 
    kafka_sink_t build()
    {
        return kafka_sink_t(func,
                            key_extr,
                            parallelism,
                            broker,
                            offsets,
                            topic,
                            name,
                            input_routing_mode,
                            closing_func);
    }
};

} // namespace wf

#endif
