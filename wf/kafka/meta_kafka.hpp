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
 *  @file    meta_kafka.hpp
 *  @author  Gabriele Mencagli and Matteo Della Bartola
 *  
 *  @brief Metafunctions used by the Kafka Operators of the WindFlow library
 *  
 *  @section Metafunctions-Kafka (Description)
 *  
 *  Set of metafunctions used by the Kafka Operators of the WindFlow library.
 *  They are the Kafka_Source and Kafka_Sink operators.
 */ 

#ifndef META_KAFKA_H
#define META_KAFKA_H

// includes
#include<basic.hpp>
#include<context.hpp>
#include<functional>
#include<source_shipper.hpp>
#include<librdkafka/rdkafkacpp.h>
#include<optional>
#include<kafka/kafkacontext.hpp>

namespace wf {


/*************************************************** KAFKA_SOURCE OPERATOR ***************************************************/
// declaration of functions to extract the type of the result form the deserialization function
template<typename F_t, typename Arg> // non-riched
Arg get_result_t_KafkaSource(bool (F_t::*)(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<Arg>& /*, std::optional<tuple_t>& */) const);

template<typename F_t, typename Arg> // non-riched
Arg get_result_t_KafkaSource(bool (F_t::*)(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<Arg>& /*, std::optional<tuple_t>& */));

template<typename Arg> // non-riched
Arg get_result_t_KafkaSource(bool (*)(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<Arg>& /*, std::optional<tuple_t>& */));

template<typename F_t, typename Arg> // riched
Arg get_result_t_KafkaSource(bool (F_t::*)(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<Arg>&, /* std::optional<tuple_t>&, */ KafkaRuntimeContext&) const);

template<typename F_t, typename Arg> // riched
Arg get_result_t_KafkaSource(bool (F_t::*)(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<Arg>&, /* std::optional<tuple_t>&, */ KafkaRuntimeContext&));

template<typename Arg> // riched
Arg get_result_t_KafkaSource(bool (*)(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<Arg>&, /* std::optional<tuple_t>&, */ KafkaRuntimeContext&));

template<typename F_t>
decltype(get_result_t_KafkaSource(&F_t::operator())) get_result_t_KafkaSource(F_t);

std::false_type get_result_t_KafkaSource(...); // black hole
/*****************************************************************************************************************************/
/**************************************************CLOSING_FUNC KAFKA**************************************/
// declaration of functions to check the signature of the closing logic
template<typename F_t>
std::true_type check_kafka_closing_t(void (F_t::*)(KafkaRuntimeContext&) const);

template<typename F_t>
std::true_type check_kafka_closing_t(void (F_t::*)(KafkaRuntimeContext&));

std::true_type check_kafka_closing_t(void (*)(KafkaRuntimeContext&));

template<typename F_t>
decltype(check_kafka_closing_t(&F_t::operator())) check_kafka_closing_t(F_t);

std::false_type check_kafka_closing_t(...); // black hole
} // namespace wf

#endif
