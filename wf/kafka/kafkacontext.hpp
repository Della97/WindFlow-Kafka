#ifndef KAFKACONTEXT_H
#define KAFKACONTEXT_H

/// includes
#include<chrono>
#include<vector>
#include<functional>
#include<string>
#include<context.hpp>
#include<basic.hpp>
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
    template<typename T> friend class Kafka_Source_Replica; // friendship with Kafka_Source_Replica class
    std::string kafkaName;
    std::vector<RdKafka::TopicPartition *> partitions;
    uint64_t timestamp;
    uint64_t watermark;

    void setPartitions (std::vector<RdKafka::TopicPartition *> _partitions) {
        partitions = _partitions;
    }

public:
    KafkaRuntimeContext (std::string _kafkaName,
                         size_t _parallelism,
                         size_t _index):
                         RuntimeContext(_parallelism, _index),
                         kafkaName(_kafkaName) {}

    
    //NOT SURE
    void setContext (uint64_t _timestamp, uint64_t _watermark) {
        timestamp = _timestamp;
        watermark = _watermark;
    }

    std::string getName () {
        return kafkaName;
    }

    std::vector<RdKafka::TopicPartition *> getPartitions () {
        return partitions;
    }

};

} // namespace wf

#endif