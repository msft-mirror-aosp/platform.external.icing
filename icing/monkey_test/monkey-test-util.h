// Copyright (C) 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ICING_MONKEY_TEST_MONKEY_TEST_UTIL_H_
#define ICING_MONKEY_TEST_MONKEY_TEST_UTIL_H_

#include <algorithm>
#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/index/embed/quantizer.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/ann.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/section.h"
#include "icing/util/embedding-util.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

using MonkeyTestRandomEngine = std::mt19937;

class IcingMonkeyTestRunner;

struct IcingMonkeyTestRunnerConfiguration {
  explicit IcingMonkeyTestRunnerConfiguration(uint32_t seed, int num_types,
                                              int num_namespaces, int num_uris,
                                              int index_merge_size,
                                              bool initialize_by_existing_data)
      : seed(seed),
        num_types(num_types),
        num_namespaces(num_namespaces),
        num_uris(num_uris),
        index_merge_size(index_merge_size),
        initialize_by_existing_data(initialize_by_existing_data) {}

  uint32_t seed;
  int num_types;
  int num_namespaces;
  int num_uris;
  int index_merge_size;

  // Whether to initialize Icing with existing data. If true, the test will
  // start from the state of the existing Icing testing data stored in
  // GetTestTempDir() + "/icing/monkey". Otherwise, the test will start with an
  // empty Icing.
  bool initialize_by_existing_data;

  // To ensure that the random schema is generated with the best quality, the
  // number of properties for each type will only be randomly picked from this
  // list, instead of picking it from a range. For example, a vector of
  // [1, 2, 3, 4] means each generated types have a 25% chance of getting 1
  // property, 2 properties, 3 properties and 4 properties.
  // Optional.
  std::vector<int> possible_num_properties = {0,
                                              1,
                                              2,
                                              4,
                                              8,
                                              16,
                                              kTotalNumSections / 2,
                                              kTotalNumSections,
                                              kTotalNumSections + 1,
                                              kTotalNumSections * 2};

  // The possible number of tokens that may appear in a string property of
  // generated documents, with a noise factor from 0.5 to 1 applied.
  // - This number is also used for the number of qualified ids in join
  //   properties if the string property is qualified id joinable.
  // - However, unlike normal string tokens which will be concatenated together
  //   (separated by spaces) into a single string, qualified ids will be stored
  //   as multiple strings in the document.
  // - Also if the cardinality of the joinable property is optional or required,
  //   then only 1 qualified id will be generated. In this case,
  //   possible_num_tokens is ignored.
  // Required.
  std::vector<int> possible_num_tokens;

  // The possible number of embedding vectors that may appear in a repeated
  // vector property of generated documents.
  // Required.
  std::vector<int> possible_num_vectors;

  // The possible dimensions for the randomly generated embedding vectors.
  // Required.
  std::vector<int> possible_vector_dimensions;

  // The possible number of shards that may be used for embedding indexing.
  std::vector<int> possible_num_shards = {1, 2, 4, 8, 16, 32};

  // The options for ANN index maintenance.
  MaintainAnnIndexOptions maintain_ann_index_options = [] {
    MaintainAnnIndexOptions options;
    options.mutable_mini_batch_k_means_options()->set_target_cluster_size(50);
    options.set_min_size_for_ivf(1000);
    options.set_rebuild_threshold(0.2);
    return options;
  }();

  // The possible number of int64 values that may appear in a repeated
  // int64 property of generated documents.
  // Optional.
  std::vector<int> possible_num_int64s = {5, 10, 25};

  // The range [min, max] of values for randomly generated int64 properties.
  // Optional.
  std::pair<int64_t, int64_t> int64_value_range = {-100, 100};

  // An array of pairs of possible query tree depths and their frequencies.
  // If f_sum is the sum of all the frequencies, an operation with frequency f
  // means for every f_sum iterations, the operation is expected to run f times.
  // Optional.
  std::vector<std::pair<int, int>> possible_query_tree_depths = {
      {1, 90}, {2, 5}, {3, 5}};

  // The possible number of children that an n-ary node in the query tree may
  // have.
  // Optional.
  std::vector<int> possible_num_children_per_nary_node = {2, 3, 5, 8};

  // The possible random spaces for generating qualified ids for join
  // properties. When generating a qualified id:
  // - Pick a random space from this list.
  // - Then generate a namespace from [namespace_l, namespace_r) and a uri from
  //   [uri_l, uri_r).
  //
  // This will ensure that a decent join ratio can be achieved.
  //
  // Note: join monkey tests should have at least one of these spaces defined.
  struct QualifiedIdRandomSpace {
    int namespace_l;
    int namespace_r;
    int uri_l;
    int uri_r;
  };
  // Optional. If empty, join property generation is disabled.
  std::vector<QualifiedIdRandomSpace> possible_ref_qualified_id_random_spaces;

  // Whether to enable join delete propagation. If true, then the generator will
  // generate joinable properties with delete propagation in the schema.
  bool enable_join_delete_propagation = false;

  // An array of pairs of monkey test APIs with frequencies.
  // If f_sum is the sum of all the frequencies, an operation with frequency f
  // means for every f_sum iterations, the operation is expected to run f times.
  // Required.
  std::vector<std::pair<std::function<void(IcingMonkeyTestRunner*)>, int>>
      monkey_api_schedules;

  bool IsJoinEnabled() const {
    return !possible_ref_qualified_id_random_spaces.empty();
  }
};

// REQUIRES: candidate.values() is not empty.
//
// TODO(b/491571627): Consider moving this helper function to
// monkey-semantic-query-node after we switch InMemoryIcingSearchEngine to use
// the new query node
inline libtextclassifier3::StatusOr<bool> DoesVectorsMatch(
    EmbeddingScorer* embedding_scorer, double min_score, double max_score,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    const PropertyProto::VectorProto& query,
    const PropertyProto::VectorProto& candidate) {
  if (query.model_signature() != candidate.model_signature()) {
    return false;
  }

  ICING_ASSIGN_OR_RETURN(uint32_t query_dim,
                         embedding_util::GetDimension(query));
  ICING_ASSIGN_OR_RETURN(uint32_t cand_dim,
                         embedding_util::GetDimension(candidate));
  if (query_dim != cand_dim) {
    return false;
  }
  const int dimension = static_cast<int>(query_dim);
  if (dimension == 0) {
    return min_score <= 0 && 0 <= max_score;
  }

  const float* query_values_ptr = nullptr;
  std::vector<float> query_floats_storage;
  if (!query.quantized_values().empty()) {
    query_floats_storage.resize(dimension);
    embedding_util::Dequantize(query.quantized_values().data(), dimension,
                               query_floats_storage.data());
    query_values_ptr = query_floats_storage.data();
  } else {
    query_values_ptr = query.values().data();
  }

  float score;
  // If the candidate vector provides quantized values directly
  if (!candidate.quantized_values().empty()) {
    // For this case, if quantization_type in schema is not specified for
    // quantization, the candidate embedding will be ignore by the index.
    if (quantization_type !=
        EmbeddingIndexingConfig::QuantizationType::QUANTIZE_8_BIT) {
      return false;
    }
    Quantizer quantizer(candidate.quantized_values().data());
    const uint8_t* quantized_data = reinterpret_cast<const uint8_t*>(
        candidate.quantized_values().data() + sizeof(Quantizer));
    score = embedding_scorer->EigenScore(dimension, query_values_ptr,
                                         quantized_data, quantizer);
  } else if (quantization_type ==
             EmbeddingIndexingConfig::QuantizationType::NONE) {
    score = embedding_scorer->EigenScore(dimension, query_values_ptr,
                                         candidate.values().data());
  } else {
    // Quantize the candidate vector.
    // The candidate vector should never be empty, so dereferencing should be
    // safe.
    auto minmax_pair = std::minmax_element(candidate.values().begin(),
                                           candidate.values().end());
    Quantizer quantizer =
        Quantizer::Create(*minmax_pair.first, *minmax_pair.second).ValueOrDie();
    std::vector<uint8_t> quantized_candidate;
    quantized_candidate.reserve(dimension);
    for (float value : candidate.values()) {
      quantized_candidate.push_back(quantizer.Quantize(value));
    }
    // Score the quantized candidate against the original query.
    score = embedding_scorer->EigenScore(dimension, query_values_ptr,
                                         quantized_candidate.data(), quantizer);
  }
  return min_score <= score && score <= max_score;
}
inline bool DoesSchemaTypeMatch(
    const MonkeyTokenizedDocument& document,
    const std::vector<std::string>& schema_type_filters) {
  if (schema_type_filters.empty()) {
    return true;
  }
  return std::find(schema_type_filters.begin(), schema_type_filters.end(),
                   document.document.schema()) != schema_type_filters.end();
}

inline bool DoesNamespaceMatch(
    const MonkeyTokenizedDocument& document,
    const std::vector<std::string>& namespace_filters) {
  if (namespace_filters.empty()) {
    return true;
  }
  return std::find(namespace_filters.begin(), namespace_filters.end(),
                   document.document.namespace_()) != namespace_filters.end();
}

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_MONKEY_TEST_UTIL_H_
