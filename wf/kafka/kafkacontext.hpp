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
 *  @file    context.hpp
 *  @author  Gabriele Mencagli and Matteo Della Bartola
 *  
 *  @brief KafkaRuntimeContext class to access the run-time system information by the
 *         Kafka operators
 *  
 *  @section KafkaRuntimeContext (Description)
 *  
 *  This file implements the KafkaRuntimeContext class used to access the run-time system
 *  information accessible with the "riched" functional logic supported by the Kafka operators.
 */ 

#ifndef KAFKACONTEXT_H
#define KAFKACONTEXT_H

/// includes
#include<chrono>
#include<vector>
#include<string>
#include<functional>
#include<basic.hpp>
#include<context.hpp>
#include<local_storage.hpp>
#include<kafka/kafkacontext.hpp>
#include<librdkafka/rdkafkacpp.h>

namespace wf {

/** 
 *  \class KafkaRuntimeContext
 *  
 *  \brief KafkaRuntimeContext class used to access to run-time system information by the
 *         Kafka operators
 *  
 *  This class implements the KafkaRuntimeContext object used to access the run-time system
 *  information accessible with the "riched" variants of the functional logic of Kafka operators.
 */ 
class KafkaRuntimeContext: public RuntimeContext
{
private:
    template<typename T> friend class Kafka_Source_Replica; // friendship with Kafka_Source_Replica class
    RdKafka::KafkaConsumer *consumer;
    RdKafka::Producer *producer;
    uint64_t timestamp;
    uint64_t watermark;

public:
    KafkaRuntimeContext (size_t _parallelism,
                         size_t _index):
                         RuntimeContext(_parallelism, _index) {}

    //NOT SURE
    void setProducerContext (uint64_t _timestamp, uint64_t _watermark, RdKafka::Producer *_producer) {
        timestamp = _timestamp;
        watermark = _watermark;
        producer = _producer;
    }

    void setConsumerContext (RdKafka::KafkaConsumer *_consumer) {
        consumer = _consumer;
    }

    std::string getConsumerName () {
        return consumer->name();
    }

    std::string getProducerName () {
        return producer->name();
    }

};

} // namespace wf

#endif
