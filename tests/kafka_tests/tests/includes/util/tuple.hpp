/**
 *  @file    tuple.hpp
 *  @author  Alessandra Fais
 *  @date    16/05/2019
 *
 *  @brief Structure of a tuple
 *
 *  This file defines the structure of the tuples generated by the source.
 *  The data type tuple_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and it must provide and implement the setInfo() and
 *  getInfo() methods.
 */

#ifndef SPIKEDETECTION_TUPLE_HPP
#define SPIKEDETECTION_TUPLE_HPP

#include <windflow.hpp>

using namespace std;

/**
 *  The field incremental_average is initially set to 0. The value is computed and set
 *  by the average calculator node. Computations are executed in-place on the tuples.
 */
struct tuple_t {
    double property_value;        // next value contained in the dataset for the property that the user chose to monitor
    double incremental_average;   // incremental average value
    size_t key;                   // device_id that identifies the device (the sensor)
    uint64_t ts;

    // default constructor
    tuple_t() : property_value(0.0), incremental_average(0.0), key(0) {}

    // constructor
    tuple_t(double _property_value, double _incremental_average, size_t _key) :
        property_value(_property_value), incremental_average(_incremental_average), key(_key) {}
};

#endif //SPIKEDETECTION_TUPLE_HPP
