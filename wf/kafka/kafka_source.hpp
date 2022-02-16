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

/** 
 *  @file    kafka_source.hpp
 *  @author  Gabriele Mencagli and Matteo Della Bartola
 *  
 *  @brief Kafka_Source operator
 *  
 *  @section Kafka_Source (Description)
 *  
 *  This file implements the Kafka_Source operator able to generate output streams
 *  of messages read from Apache Kafka.
 */ 

#ifndef KAFKA_SOURCE_H
#define KAFKA_SOURCE_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<source_shipper.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class Kafka_Source_Replica
template<typename kafka_deser_func_t>
class Kafka_Source_Replica: public ff::ff_monode
{
private:
    kafka_deser_func_t func; // logic for deserializing messages from Apache Kafka
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the deserialization logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), RdKafka::Message &, Source_Shipper<result_t> &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), RdKafka::Message &, Source_Shipper<result_t> &, RuntimeContext &>::value;
    // check the presence of a valid deserialization logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Kafka_Source_Replica does not have a valid deserialization logic:\n");
    std::string opName; // name of the Kafka_Source containing the replica
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the Kafka_Source replica
    bool terminated; // true if the Kafka_Source replica has finished its work
    Execution_Mode_t execution_mode;// execution mode of the Kafka_Source replica
    Time_Policy_t time_policy; // time policy of the Kafka_Source replica
    Source_Shipper<result_t> *shipper; // pointer to the shipper object used by the Kafka_Source replica to send outputs

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    RdKafka::KafkaConsumer *consumer = nullptr;
    RdKafka::Conf *conf = nullptr;
    RdKafka::Conf *tconf = nullptr;
    RdKafka::Topic *topic = nullptr;
    std::string brokers = "localhost";
    std::string groupid = "id";
    std::string errstr;
    std::vector<std::string> topics;
    std::vector<RdKafka::TopicPartition*> partitions;
    int32_t partition = 0;
    int64_t start_offset = 0;
    int use_ccb = 0;
    bool run = true;
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
#endif

public:
    // Constructor
    Kafka_Source_Replica(kafka_deser_func_t _func,
                         std::string _opName,
                         RuntimeContext _context,
                         std::vector<std::string> _topics,
                         std::function<void(RuntimeContext &)> _closing_func):
                         func(_func),
                         opName(_opName),
                         context(_context),
                         topics(_topics),
                         closing_func(_closing_func),
                         terminated(false),
                         execution_mode(Execution_Mode_t::DEFAULT),
                         time_policy(Time_Policy_t::INGRESS_TIME),
                         shipper(nullptr) {
                             for (auto s : topics) {
                                std::cout << s << " INSIDE SOURCE REPLICA " << std::endl;
                            }
                         }

    // Copy Constructor
    Kafka_Source_Replica(const Kafka_Source_Replica &_other):
                         func(_other.func),
                         opName(_other.opName),
                         context(_other.context),
                         topics(_other.topics),
                         closing_func(_other.closing_func),
                         terminated(_other.terminated),
                         execution_mode(_other.execution_mode),
                         time_policy(_other.time_policy)
    {
        if (_other.shipper != nullptr) {
            shipper = new Source_Shipper<decltype(get_result_t_KafkaSource(func))>(*(_other.shipper));
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
#endif
        }
        else {
            shipper = nullptr;
        }
    }

    // Move Constructor
    Kafka_Source_Replica(Kafka_Source_Replica &&_other):
                         func(std::move(_other.func)),
                         opName(std::move(_other.opName)),
                         context(std::move(_other.context)),
                         topics(std::move(_other.topics)),
                         closing_func(std::move(_other.closing_func)),
                         terminated(_other.terminated),
                         execution_mode(_other.execution_mod),
                         time_policy(_other.time_policy)
    {
        shipper = std::exchange(_other.shipper, nullptr);
        if (shipper != nullptr) {
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
#endif
        }
    }

    // Destructor
    ~Kafka_Source_Replica()
    {
        if (shipper != nullptr) {
            delete shipper;
        }

        /* Qui iniziano una serie di delete della parte Kafka che vanno sistemate */
        delete consumer;
        delete conf;
        delete tconf;
        delete topic;
    }

    // Copy Assignment Operator
    Kafka_Source_Replica &operator=(const Kafka_Source_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            opName = _other.opName;
            context = _other.context;
            topics = _other.topics;
            closing_func = _other.closing_func;
            terminated = _other.terminated;
            execution_mode = _other.execution_mode;
            time_policy = _other.time_policy;
            if (shipper != nullptr) {
                delete shipper;
            }
            if (_other.shipper != nullptr) {
                shipper = new Source_Shipper<decltype(get_result_t_KafkaSource(func))>(*(_other.shipper));
                shipper->node = this; // change the node referred by the shipper
    #if defined (WF_TRACING_ENABLED)
                shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
    #endif
            }
            else {
                shipper = nullptr;
            }
        }
        return *this;
    }

    // Move Assignment Operator
    Kafka_Source_Replica &operator=(Kafka_Source_Replica &_other)
    {
        func = std::move(_other.func);
        opName = std::move(_other.opName);
        context = std::move(_other.context);
        topics = std::move(_other.topics);
        closing_func = std::move(_other.closing_func);
        terminated = _other.terminated;
        execution_mode = _other.execution_mode;
        time_policy = _other.time_policy;
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = std::exchange(_other.shipper, nullptr);
        if (shipper != nullptr) {
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
#endif
        }
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
        conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
        conf->set("metadata.broker.list", brokers, errstr);
        conf->set("enable.partition.eof", "true", errstr);
        conf->set("group.id", groupid, errstr);              //NEED TO GET GROUP ID AS PARAMATER!!! TO-DO


        if (topics.empty()) {
            std::cout << "TOPICS NON ESISTE: " << std::endl;
        } else {
            std::cout << "TOPICS ESISTE: " << std::endl;
        }

        for (auto s : topics) {
            std::cout << "SVC_INIT: " << s << std::endl;
        }

        consumer = RdKafka::KafkaConsumer::create(conf, errstr);
        if (!consumer) {
            std::cerr << "Failed to create consumer: " << errstr << std::endl;
            exit(1);
        }
        std::cout << "% Created consumer " << consumer->name() << std::endl;
        
        /* Subscribe to topics */
        RdKafka::ErrorCode err = consumer->subscribe(topics);
        if (err) {
            std::cerr << "Failed to subscribe to " << topics.size()
                        << " topics: " << RdKafka::err2str(err) << std::endl;
            exit(1);
        }


#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), false, false);
#endif
        shipper->setInitialTime(current_time_usecs()); // set the initial time
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *) override
    {
        while (run) { // main loop
            RdKafka::Message *msg = consumer->consume(1000); // qui si può fare qualcosa di carino per gestire il timeout
            switch (msg->err()) {
                case RdKafka::ERR__TIMED_OUT:
                    //std::cout << "Timed out while fetching msg from broker" << std::endl; // bisogna usare cout non printf (solo per essere omogenei)
                    break;
                case RdKafka::ERR_NO_ERROR:
                    std::cout << "[PAYLOAD - SVC-SRC NUM " << consumer->name() <<"] -> " << static_cast<const char *>(msg->payload()) <<
                       " from partition " << msg->partition() << std::endl;
                    //printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char *>(msg->payload()));
                    if constexpr (isNonRiched) {
                        run = func(*msg, *shipper); //get payload -> deser -> push forward if valid
                    }
                    if constexpr (isRiched) {
                        run = func(*msg, *shipper, context); //get payload -> deser -> push forward if valid
                    }
                    break;
                default:
                    /* Errors */
                    std::cerr << "Consume failed: " << msg->errstr() << std::endl;
                    //run = 0;
            }
            delete msg;
        }
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
        return this->EOS; // end-of-stream
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        // <---------------------- qui ci possono essere memory leak!!!
        // stop consumer
        consumer->close();
        delete consumer;
        /*
        * Wait for RdKafka to decommission.
        * This is not strictly needed (when check outq_len() above), but
        * allows RdKafka to clean up all its resources before the application
        * exits so that memory profilers such as valgrind wont complain about
        * memory leaks.
        */
        //RdKafka::wait_destroyed(5000);
        closing_func(context); // call the closing function
    }

    // Set the emitter used to route outputs generated by the Kafka_Source replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        // if a shipper already exists, it is destroyed
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = new Source_Shipper<decltype(get_result_t_KafkaSource(func))>(_emitter, this, execution_mode, time_policy); // create the shipper
        shipper->setInitialTime(current_time_usecs()); // set the initial time
#if defined (WF_TRACING_ENABLED)
        shipper->setStatsRecord(&stats_record);
#endif
    }

    // Check the termination of the Kafka_Source replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution and time mode of the Kafka_Source replica
    void setConfiguration(Execution_Mode_t _execution_mode,
                          Time_Policy_t _time_policy)
    {
        execution_mode = _execution_mode;
        time_policy = _time_policy;
        if (shipper != nullptr) {
            shipper->setConfiguration(execution_mode, time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Kafka_Source replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Kafka_Source
 *  
 *  \brief Kafka_Source operator
 *  
 *  This class implements the Kafka_Source operator able to generate a stream of outputs
 *  all having the same type, by reading messages from Apache Kafka.
 */ 
template<typename kafka_deser_func_t>
class Kafka_Source: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    kafka_deser_func_t func; // functional logic to deserialize messages from Apache Kafka
    size_t parallelism; // parallelism of the Kafka_Source
    std::string name; // name of the Kafka_Source
    size_t outputBatchSize; // batch size of the outputs produced by the Kafka_Source
    std::vector<Kafka_Source_Replica<kafka_deser_func_t>*> replicas; // vector of pointers to the replicas of the Kafka_Source

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    std::string brokers;
    std::string groupid;
    std::vector<std::string> topics;
    int32_t partition;
    int32_t offset;

    // Configure the Kafka_Source to receive batches instead of individual inputs (cannot be called for the Kafka_Source)
    void receiveBatches(bool _input_batching) override
    {
        abort(); // <-- this method cannot be used!
    }

    // Set the emitter used to route outputs from the Kafka_Source
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Kafka_Source has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode and the time policy of the Kafka_Source (i.e., the ones of its PipeGraph)
    void setConfiguration(Execution_Mode_t _execution_mode,
                          Time_Policy_t _time_policy)
    {
        for(auto *r: replicas) {
            r->setConfiguration(_execution_mode, _time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Kafka_Source
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the LOG_DIR directory
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        if (mkdir(log_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        rapidjson::StringBuffer buffer; // create the rapidjson writer
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        this->appendStats(writer); // append the statistics of the Kafka_Source
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Kafka_Source to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Kafka_Source");
        writer.Key("Distribution");
        writer.String("NONE");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Parallelism");
        writer.Uint(parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Kafka_Source
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func deserialization logic of the Kafak_Source (a function or a callable type)
     *  \param _parallelism internal parallelism of the Kafka_Source
     *  \param _name name of the Kafka Source
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Kafka_Source (a function or callable type)
     * 
     *  <------qui devi sistemare il doxygen!!!
     */ 
    Kafka_Source(kafka_deser_func_t _func,
                 std::string _name,
                 size_t _outputBatchSize,
                 std::string _brokers,
                 std::vector<std::string> _topics,
                 std::string _groupid, //merge group-id
                 int32_t _partition,
                 int32_t _offset,
                 std::function<void(RuntimeContext &)> _closing_func):
                 func(_func),
                 parallelism(_partition),
                 name(_name),
                 outputBatchSize(_outputBatchSize),
                 brokers(_brokers),
                 topics(_topics),
                 groupid(_groupid), //merge group-id
                 offset(_offset)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Kafka_Source has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        /*
        if (parallelism > topics.size()) {  //check if the parallelism and the partitions are compatible /todo
            std::cerr << RED << "WindFlow Error: Kafka_Source parallelism too high??" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        */

        for (auto s : topics) {
            std::cout << s << " INSIDE CONSTRUCTOR IN KAFKA SOURCE " << std::endl;
        }

        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Kafka_Source
            replicas.push_back(new Kafka_Source_Replica<kafka_deser_func_t>(_func, name, RuntimeContext(parallelism, i), topics, _closing_func));
        }
    }

    /// Copy constructor
    Kafka_Source(const Kafka_Source &_other):
                 func(_other.func),
                 parallelism(_other.parallelism),
                 name(_other.name),
                 outputBatchSize(_other.outputBatchSize)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Kafka_Source replicas
            replicas.push_back(new Kafka_Source_Replica<kafka_deser_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Kafka_Source() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }

        // C'è da cancellare roba di Kafka?
    }

    /// Copy assignment operator
    Kafka_Source& operator=(const Kafka_Source &_other)
    {
        if (this != &_other) {
            func = _other.func;
            parallelism = _other.parallelism;
            name = _other.name;
            outputBatchSize = _other.outputBatchSize;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Source replicas
                replicas.push_back(new Kafka_Source_Replica<kafka_deser_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move assignment operator
    Kafka_Source& operator=(Kafka_Source &&_other)
    {
        func = std::move(_other.func);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        outputBatchSize = _other.outputBatchSize;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Kafka_Source as a string
     *  \return type of the Kafka_Source
     */ 
    std::string getType() const override
    {
        return std::string("Kafka_Source");
    }

    /** 
     *  \brief Get the name of the Kafka_Source as a string
     *  \return name of the Kafka_Source
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Kafka_Source
     *  \return total parallelism of the Kafka_Source
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Kafka_Source
     *  \return routing mode used to send inputs to the Kafka_Source
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::NONE;
    }

    /** 
     *  \brief Return the size of the output batches that the Kafka_Source should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }
};

} // namespace wf

#endif
