#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include<kafka/windflow_kafka.hpp>

#include "includes/util/tuple.hpp"
#include "includes/nodes/sink.hpp"
#include "includes/nodes/source.hpp"
#include "includes/util/cli_util.hpp"
#include "includes/nodes/detector.hpp"
#include "includes/util/constants.hpp"
#include "includes/nodes/average_calculator_map.hpp"
#include "includes/nodes/generate_kafka_datas.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// type of the input records: < date_value, time_value, epoch_value, device_id_value, temp_value, humid_value, light_value, voltage_value>
using record_t = tuple<string, string, int, int, double, double, double, double>;

// global variables
vector<record_t> parsed_file;               // contains data extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources

/** 
 *  @brief Parse the input file
 *  
 *  The file is parsed and saved in memory.
 *  
 *  @param file_path the path of the input dataset file
 */ 
void parse_dataset(const string& file_path) {
    ifstream file(file_path);
    vector<string> tokens;
    if (file.is_open()) {
        size_t all_records = 0;         // counter of all records (dataset line) read
        size_t incomplete_records = 0;  // counter of the incomplete records
        string line;
        while (getline(file, line)) {
            // process file line
            int token_count = 0;
            regex rgx("\\s+"); // regex quantifier (matches one or many whitespaces)
            sregex_token_iterator iter(line.begin(), line.end(), rgx, -1);
            sregex_token_iterator end;
            while (iter != end) {
                tokens.push_back(*iter);
                token_count++;
                iter++;
            }
        }
        file.close();
    }
}