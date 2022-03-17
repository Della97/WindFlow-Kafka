#ifndef KAFKACONTEXT_H
#define KAFKACONTEXT_H

/// includes
#include<chrono>
#include<vector>
#include<functional>
#include<string>
#include<context.hpp>
#include<local_storage.hpp>
#include<kafka/kafkacontext.hpp>
#include<librdkafka/rdkafkacpp.h>

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
class KafkaRuntimeContext : public RuntimeContext
{
private:
    std::string kafkaName;
    std::vector<RdKafka::TopicPartition *> partitions;
    size_t parallelism; // parallelism of the operator
    size_t index; // index of the replica
    uint64_t timestamp; // timestamp of the current input
    uint64_t watermark; // last received watermark

    // Set the configuration parameters
    void setContextParameters(std::string _kafkaName,
                              std::vector<RdKafka::TopicPartition *> _partitions)
    {
        kafkaName = _kafkaName;
        partitions = _partitions;
    }

public:
    KafkaRuntimeContext(size_t _parallelism,
                   size_t _index):
                   parallelism(_parallelism),
                   index(_index),
                   timestamp(0),
                   watermark(0) {}

    /// Copy Constructor
    KafkaRuntimeContext(const KafkaRuntimeContext &_other): // do not copy the storage
                   parallelism(_other.parallelism),
                   index(_other.index),
                   timestamp(_other.timestamp),
                   watermark(_other.watermark) {}
    
    std::string getName () {
        return kafkaName;
    }

    std::vector<RdKafka::TopicPartition *> getPartitions () {
        return partitions;
    }

};

} // namespace wf

#endif