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
#include<optional>
#include<functional>
#include<context.hpp>
#include<source_shipper.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<kafka/kafkacontext.hpp>

namespace wf {

//@cond DOXY_IGNORE

class ExampleRebalanceCb : public RdKafka::RebalanceCb
{
private:
    std::vector<int> offsets;
    std::vector<std::string> topics;
    int size = 0;
    int init = 0;

    static void part_list_print(const std::vector<RdKafka::TopicPartition *> &partitions)
    {
        for (unsigned int i = 0; i < partitions.size(); i++)
            std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
        std::cerr << "\n";
    }

public:
    void initOffsetTopics (std::vector<int> _offsets,
                           std::vector<std::string> _topics)
    {
        offsets = std::move(_offsets);
        topics = std::move(_topics);
        size = topics.size();
        init = 0; //reload offset mid execution (at next rebalance callback) (need to test)
    }

    void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                      RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition *> &partitions)
    {
        std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";
        part_list_print(partitions);
        if (init == 0) {
            if (offsets.size() != 0){
                for (int i = 0; i<size; i++) {
                    for (auto j:partitions) {
                        if (j->topic() == topics[i]) {
                            if (offsets[i] > -1) {
                                j->set_offset(offsets[i]);
                            }
                        }
                    }
                }
            }
            init++;
        }
        RdKafka::Error *error      = NULL;
        RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            if (consumer->rebalance_protocol() == "COOPERATIVE") {
                error = consumer->incremental_assign(partitions);
            }
            else {
                ret_err = consumer->assign(partitions);
            }
        }
        else {
            if (consumer->rebalance_protocol() == "COOPERATIVE") {
                error = consumer->incremental_unassign(partitions);
            }
            else {
                ret_err = consumer->unassign();
            }
        }
        if (error) {
            std::cerr << "incremental assign failed: " << error->str() << "\n";
            delete error;
        }
        else if (ret_err) {
            std::cerr << "assign failed: " << RdKafka::err2str(ret_err) << "\n";
        }
    }
};

// class Kafka_Source_Replica
template<typename kafka_deser_func_t>
class Kafka_Source_Replica: public ff::ff_monode
{
private:
    kafka_deser_func_t func; // logic for deserializing messages from Apache Kafka
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the deserialization logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> & >::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &, KafkaRuntimeContext &>::value;
    // check the presence of a valid deserialization logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Kafka_Source_Replica does not have a valid deserialization logic:\n");
    std::string opName; // name of the Kafka_Source containing the replica
    KafkaRuntimeContext context; // RuntimeContext object
    std::function<void(KafkaRuntimeContext &)> closing_func; // closing functional logic used by the Kafka_Source replica
    bool terminated; // true if the Kafka_Source replica has finished its work
    Execution_Mode_t execution_mode;// execution mode of the Kafka_Source replica
    Time_Policy_t time_policy; // time policy of the Kafka_Source replica
    Source_Shipper<result_t> *shipper; // pointer to the shipper object used by the Kafka_Source replica to send outputs

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    RdKafka::KafkaConsumer *consumer = nullptr;
    RdKafka::Conf *conf = nullptr;
    RdKafka::ErrorCode error;
    int idleTime; //default value
    std::string brokers;
    std::string groupid;
    std::string strat;
    std::string errstr;
    int32_t partition;
    std::vector<int> offset;
    int32_t parallelism;
    size_t outputBatchSize;
    ExampleRebalanceCb ex_rebalance_cb; //partiotion manager
    int32_t tmp = 0;
    std::vector<std::string> topics;
    bool run = true;
    bool stop = true;
    bool fetch = true;
    pthread_barrier_t *bar;
    std::vector<RdKafka::TopicPartition *> partitions;
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
#endif

public:
    // Constructor
    Kafka_Source_Replica(kafka_deser_func_t _func,
                         std::string _opName,
                         KafkaRuntimeContext _context,
                         size_t _outputBatchSize,
                         std::string _brokers,
                         std::vector<std::string> _topics,
                         std::string _groupid, //merge group-id
                         std::string _strat,
                         int32_t _parallelism,
                         int _idleTime,
                         std::vector<int> _offset,
                         pthread_barrier_t *_bar,
                         std::function<void(KafkaRuntimeContext &)> _closing_func):
                         func(_func),
                         opName(_opName),
                         context(_context),
                         outputBatchSize(_outputBatchSize),
                         brokers(_brokers),
                         topics(_topics),
                         groupid(_groupid),
                         strat(_strat),
                         idleTime(_idleTime),
                         parallelism(_parallelism),
                         offset(_offset),
                         bar(_bar),
                         closing_func(_closing_func),
                         terminated(false),
                         execution_mode(Execution_Mode_t::DEFAULT),
                         time_policy(Time_Policy_t::INGRESS_TIME),
                         shipper(nullptr) {}

    // Copy Constructor
    Kafka_Source_Replica(const Kafka_Source_Replica &_other):
                         func(_other.func),
                         opName(_other.opName),
                         context(_other.context),
                         outputBatchSize(_other.outputBatchSize),
                         brokers(_other.brokers),
                         topics(_other.topics),
                         groupid(_other.groupid),
                         strat(_other.strat),
                         idleTime(_other.idleTime),
                         parallelism(_other.parallelism),
                         offset(_other.offset),
                         bar(_other.bar),
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
                         outputBatchSize(std::move(_other.outputBatchSize)),
                         brokers(std::move(_other.brokers)),
                         topics(std::move(_other.topics)),
                         groupid(std::move(_other.groupid)),
                         strat(std::move(_other.strat)),
                         idleTime(std::move(_other.idleTime)),
                         parallelism(std::move(_other.parallelism)),
                         offset(std::move(_other.offset)),
                         bar(std::move(_other.bar)),
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
    }

    // Copy Assignment Operator
    Kafka_Source_Replica &operator=(const Kafka_Source_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            opName = _other.opName;
            context = _other.context;
            topics = _other.topics;
            offset = _other.offset;
            bar = _other.bar;
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
        offset = std::move(_other.offset);
        bar = std::move(_other.bar);
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
        ex_rebalance_cb.initOffsetTopics(offset, topics);
        conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
        if (conf->set("rebalance_cb", &ex_rebalance_cb, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
        if (conf->set("group.id", groupid, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }
        if (conf->set("partition.assignment.strategy", strat, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << errstr << std::endl;
            exit(1);
        }

        consumer = RdKafka::KafkaConsumer::create(conf, errstr);
        if (!consumer) {
            std::cerr << "Failed to create consumer: " << errstr << std::endl;
            exit(1);

        }
        /* Subscribe to topics */
        RdKafka::ErrorCode err = consumer->subscribe(topics);
        if (err) {
            std::cerr << "Failed to subscribe to " << topics.size()
                    << " topics: " << RdKafka::err2str(err) << std::endl;
            exit(1);
        }
        //pthread barrier
        //std::cout << "before barrier id: " << consumer->name() << std::endl;
        context.setConsumer(consumer);
        consumer->poll(0);
        pthread_barrier_wait(bar);
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), false, false);
#endif
        shipper->setInitialTime(current_time_usecs()); // set the initial time
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *) override
    {
        std::cout << "Consuming datas..." << std::endl;
        while (run) { // main loop 
            RdKafka::Message *msg = consumer->consume(idleTime);
            switch (msg->err()) {
                case RdKafka::ERR__TIMED_OUT:
                    if constexpr (isNonRiched) {
                        run = func(std::nullopt, *shipper); //get payload -> deser -> push forward if valid
                    }
                    if constexpr (isRiched) {
                        run = func(std::nullopt, *shipper, context); //get payload -> deser -> push forward if valid
                    }
                    break;
                case RdKafka::ERR_NO_ERROR:
                    if constexpr (isNonRiched) {
                        std::cout << "received" << std::endl;
                        run = func(*msg, *shipper); //get payload -> deser -> push forward if valid
                        if (run == false) {
                            std::cout << "Reached End Of Stream from deser func: " << std::endl;
                        }
                    }
                    if constexpr (isRiched) {
                        std::cout << "received" << std::endl;
                        run = func(*msg, *shipper, context); //get payload -> deser -> push forward if valid
                        if (run == false) {
                            std::cout << "Reached End Of Stream from deser func: " << std::endl;
                        }
                    }
                    break;
            }
            delete msg;
        }
        std::cout << "Exiting from replica " << consumer->name() << std::endl;
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
        delete conf;
        /*
        * Wait for RdKafka to decommission.
        * This is not strictly needed (when check outq_len() above), but
        * allows RdKafka to clean up all its resources before the application
        * exits so that memory profilers such as valgrind wont complain about
        * memory leaks.
        */
        RdKafka::wait_destroyed(5000);
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
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    size_t parallelism; // parallelism of the Kafka_Source
    std::string name; // name of the Kafka_Source
    size_t outputBatchSize; // batch size of the outputs produced by the Kafka_Source
    std::vector<Kafka_Source_Replica<kafka_deser_func_t>*> replicas; // vector of pointers to the replicas of the Kafka_Source

    /* Da qui in poi abbiamo una serie di variabili che vanno sistemate */
    std::string brokers;
    std::string groupid;
    std::string strat;
    int idleTime;
    std::vector<std::string> topics;
    int32_t partition;
    std::vector<int> offset;
    pthread_barrier_t bar;

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
     *  \param _name name of the Kafka Source
     *  \param _outBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _brokers ip of the kafka broker
     *  \param _topics name of the topics to subscribe
     *  \param _groupid name of the group id (all replicas inthe same group)
     *  \param _strat strategy used by he group manager to assign the partitions of the topics among consumers
     *  \param _idleTime interval after each consume by the consumer
     *  \param _parallelism internal parallelism of the Kafka_Source
     *  \param _offset offset to start fetching
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
                 std::string _strat,
                 int _idleTime,
                 int32_t _parallelism,
                 std::vector<int> _offset,
                 std::function<void(KafkaRuntimeContext &)> _closing_func):
                 func(_func),
                 parallelism(_parallelism),
                 idleTime(_idleTime),
                 name(_name),
                 outputBatchSize(_outputBatchSize),
                 brokers(_brokers),
                 topics(_topics),
                 groupid(_groupid), //merge group-id
                 strat(_strat),
                 offset(_offset)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Kafka_Source has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (offset.size() < topics.size()) {
            std::cerr << RED << "WindFlow Error: Number of offsets given are less than the number of topics" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        pthread_barrier_init(&bar, NULL, parallelism);
        //parallelims check but we dont know the number of partitions
        //pthread barrier
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Kafka_Source
            replicas.push_back(new Kafka_Source_Replica<kafka_deser_func_t>(_func, name, KafkaRuntimeContext(parallelism, i), outputBatchSize, brokers, topics, groupid, strat, parallelism, idleTime, offset, &bar, _closing_func));
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
        pthread_barrier_destroy(&bar);
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