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
#include<functional>
#include<basic.hpp>
#include<kafka/meta_kafka.hpp>

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
    using closing_func_t = std::function<void(wf::RuntimeContext&)>; // type of the closing functional logic
    std::string name = "kafka_source"; // name of the Kafka_Source
    size_t parallelism = 1; // parallelism of the Kafka_Source
    size_t outputBatchSize = 0; // output batch size of the Kafka_Source
    closing_func_t closing_func; // closing function logic of the Kafka_Source

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    //std::string topics;
    std::vector<std::string> topics;
    std::string brokers = "localhost";
    std::string groupid = "id";
    int32_t partition;
    int32_t offset;

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
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Kafka_Source_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the Broker <--- cosa è un indirizzo un hostname???
     *  
     *  \param _brokers for kafka server
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withBrokers(std::string _brokers)
    {
        brokers = _brokers;
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
     *  \brief Set the topic partition
     *  
     *  \param _partition for the consumer
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withPartition(int32_t _partition)
    {
        partition = _partition;
        return *this;
    }

    /** 
     *  \brief Set the topic offset
     *  
     *  \param _offset for the consumer
     *  \return a reference to the builder object
     */ 
    Kafka_Source_Builder<kafka_deser_func_t> &withOffset(int32_t _offset)
    {
        offset = _offset;
        return *this;
    }

    /** 
     *  \brief Set the topic
     *  
     *  \param _topics for the consumer
     *  \return a reference to the builder object
     */ 

    template <class T>
    Kafka_Source_Builder<kafka_deser_func_t> &withTopics(std::initializer_list<T> _topics)
    {
        for( auto elem : list )
        {
            //std::cout << elem << std::endl ;
            //parse partition:topic or topic (all partition)
        }
        //topics = _topics;
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
                              partition,
                              offset,
                              closing_func);
    }
};

} // namespace wf

#endif
