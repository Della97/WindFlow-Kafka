#ifndef KAFKACONTEXT_H
#define KAFKACONTEXT_H

/// includes
#include<chrono>
#include<vector>
#include<functional>
#include<string>
#include<context.hpp>
#include<local_storage.hpp>

namespace wf {

/** 
 *  \class KafkaRuntimeContext
 *  
 *  \brief kafkaRuntimeContext class used to access to run-time system information
 *  
 *  This class implements the KafkaRuntimeContext object used to access the run-time system
 *  information accessible with the "riched" variants of the functional logic of some
 *  operators.
 */ 
class KafkaRuntimeContext: public RuntimeContext
{
private:
    std::string kafkaName;
    std::vector<std::string> topics;
    std::vector<int> offsets;

    

public:
    /// Constructor
    KafkaRuntimeContext(std::string _kafkaName,
                   std::vector<std::string> _topics,
                   std::vector<int> _offsets):
                   kafkaName(_kafkaName),
                   topics(_topics),
                   offsets(_offsets) {}

    /// Copy Constructor
    KafkaRuntimeContext(const KafkaRuntimeContext &_other): // do not copy the storage
                   kafkaName(_other.kafkaName),
                   topics(_other.topics),
                   offsets(_other.offsets) {}

    /// Copy Assignment Operator
    KafkaRuntimeContext &operator=(const KafkaRuntimeContext &_other) // do not copy the storage
    {
        if (this != &_other) {
            kafkaName = _other.kafkaName;
            topics = _other.topics;
            offsets = _other.offsets;
        }
        return *this;
    }


};

} // namespace wf

#endif