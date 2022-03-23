#include <fstream>
#include <vector>
#include <ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> sent_tuples;

/**
 *  @class Send_Kafka_Data
 *
 *  @brief Define the logic of the Source
 */
class Send_Kafka_Data {
private:
    vector<tuple_t> dataset;        // contains all the tuples
    int rate;                       // stream generation rate
    size_t next_tuple_idx;          // index of the next tuple to be sent
    int generations;                // counts the times the file is generated
    long generated_tuples;          // tuples counter

    // time variables
    unsigned long app_start_time;   // application start time
    unsigned long current_time;
    unsigned long interval;

    /**
     *  @brief Add some active delay (busy-waiting function)
     *
     *  @param waste_time wait time in nanoseconds
     */
    void active_delay(unsigned long waste_time) {
        auto start_time = current_time_nsecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_nsecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    // Constructor
    Send_Kafka_Data(const vector<tuple_t>& _dataset,
                   const int _rate,
                   const unsigned long _app_start_time:
            rate(_rate),
            app_start_time(_app_start_time),
            current_time(_app_start_time),
            next_tuple_idx(0),
            generations(0),
            generated_tuples(0)
    {
        dataset = _dataset; // be careful, here there is a copy. Maybe it would be better to copy only the pointer or to use move semantics...
        interval = 1000000L; // 1 second (microseconds)
    }

    /** 
     *  @brief Generation function of the input stream
     *  
     *  @param shipper Source_Shipper object used for generating inputs
     */ 
    void sendData()
    {
        current_time = current_time_nsecs(); // get the current time
        // generation loop
        while (current_time - app_start_time <= app_run_time)
        {
            if (next_tuple_idx == 0) {
                generations++;
            }
            tuple_t t(dataset.at(next_tuple_idx));
            if ((batch_size > 0) && (generated_tuples % batch_size == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            if (batch_size == 0) {
                current_time = current_time_nsecs(); // get the new current time
            }
            t.ts = current_time;

            //SEND DATA TO KAFKA
            //serialize the tuple into string (json?)
            //producer -> produce


            generated_tuples++;
            next_tuple_idx = (next_tuple_idx + 1) % dataset.size();   // index of the next tuple to be sent (if any)
            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
        }
        sent_tuples.fetch_add(generated_tuples); // save the number of generated tuples
    }

    ~Send_Kafka_Data() {}
};