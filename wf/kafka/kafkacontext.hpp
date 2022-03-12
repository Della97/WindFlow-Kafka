#ifndef KAFKACONTEXT_H
#define KAFKACONTEXT_H

/// includes
#include<chrono>
#include<vector>
#include<functional>
#include<string>
#include<context.hpp>
#include<local_storage.hpp>
#include"meta_kafka.hpp"

namespace wf {

/** 
 *  \class KafkaRuntimeContext
 *  
 *  \brief KafkaRuntimeContext class used to access to run-time system information
 *  
 *  This class implements the KafkaRuntimeContext object used to access the run-time system
 *  information accessible with the "riched" variants of the functional logic of some
 *  operators.
 */ 
class KafkaRuntimeContext
{
private:
    template<typename T> friend class Source_Replica; // friendship with Source_Replica class
    template<typename T> friend class Kafka_Source_Replica; // friendship with Kafka_Source_Replica class
    template<typename T> friend class RuntimeContext; // friendship with RuntimeContext class
    template<typename T1> friend class Map_Replica; // friendship with Map_Replica class
    template<typename T1> friend class Filter_Replica; // friendship with Filter_Replica class
    template<typename T1, typename T2> friend class Reduce_Replica; // friendship with Reduce_Replica class
    template<typename T1> friend class FlatMap_Replica; // friendship with FlatMap_Replica class
    template<typename T1> friend class Sink_Replica; // friendship with Sink_Replica class
    template<typename T1, typename T2> friend class Window_Replica; // friendship with Window_Replica class
    template<typename T1, typename T2, typename T3> friend class FFAT_Replica; // friendship with FFAT_Replica class
    std::string kafkaName;
    std::vector<RdKafka::TopicPartition *> partitions;

    

public:
    /// Constructor
    KafkaRuntimeContext(std::string _kafkaName,
                   std::vector<RdKafka::TopicPartition *> _partitions):
                   kafkaName(_kafkaName),
                   partitions(_partitions) {}

    /// Copy Constructor
    KafkaRuntimeContext(const KafkaRuntimeContext &_other): // do not copy the storage
                   kafkaName(_other.kafkaName),
                   partitions(_other.partitions) {}

    /// Copy Assignment Operator
    KafkaRuntimeContext &operator=(const KafkaRuntimeContext &_other) // do not copy the storage
    {
        if (this != &_other) {
            kafkaName = _other.kafkaName;
            partitions = _other.partitions;
        }
        return *this;
    }


};

} // namespace wf

#endif