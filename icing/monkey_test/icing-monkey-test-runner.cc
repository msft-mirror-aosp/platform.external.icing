// Copyright (C) 2022 Google LLC
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

#include "icing/monkey_test/icing-monkey-test-runner.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/icing-search-engine.h"
#include "icing/join/join-processor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-nary-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-and-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-has-property-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-not-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-numeric-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-or-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-property-defined-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-semantic-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-term-query-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-generators.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-features.h"
#include "icing/result/result-state-manager.h"
#include "icing/store/document-group-info.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::Le;
using ::testing::Not;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAreArray;

bool GetRandomBoolean(MonkeyTestRandomEngine* random) {
  std::uniform_int_distribution<> dist(0, 1);
  return dist(*random) == 1;
}

int GetRandomInt(MonkeyTestRandomEngine* random, int min, int max) {
  std::uniform_int_distribution<> dist(min, max);
  return dist(*random);
}

// Selects a random element from weighted_elements, where selection is weighted
// by pair::second.
// It is assumed that weighted_elements is non-empty and that the sum of
// weights is positive.
template <typename T>
const T& GetRandomWeightedElement(
    MonkeyTestRandomEngine* random,
    const std::vector<std::pair<T, int>>& weighted_elements) {
  int frequency_sum = 0;
  for (const auto& element : weighted_elements) {
    frequency_sum += element.second;
  }
  std::uniform_int_distribution<> dist(0, frequency_sum - 1);
  int p = dist(*random);
  for (const auto& element : weighted_elements) {
    if (p < element.second) {
      return element.first;
    }
    p -= element.second;
  }
  // This won't be reached if sum of weights > 0, which is assumed.
  ICING_LOG(FATAL)
      << "GetRandomWeightedElement called with empty list or only 0 weights.";
  return weighted_elements[0].first;  // Unreachable.
}

std::string PickRandomPropertyPathForNumericQuery(
    MonkeyTestRandomEngine* random, const SchemaProto* schema) {
  // - 60% chance: Valid indexable numeric property.
  // - 30% chance: Existing property of any type (STRING, VECTOR, non-indexable
  // INT64).
  // - 10% chance: Randomly generated property name (likely non-existent).

  std::vector<std::string> indexable_numeric_properties;
  std::vector<std::string> all_properties;

  if (schema != nullptr) {
    for (const auto& type : schema->types()) {
      for (const auto& prop : type.properties()) {
        if (prop.data_type() == PropertyConfigProto::DataType::INT64 &&
            prop.has_integer_indexing_config() &&
            prop.integer_indexing_config().numeric_match_type() ==
                IntegerIndexingConfig::NumericMatchType::RANGE) {
          indexable_numeric_properties.push_back(prop.property_name());
        }
        all_properties.push_back(prop.property_name());
      }
    }
  }

  int choice = GetRandomInt(random, 1, 100);
  if (choice <= 60 && !indexable_numeric_properties.empty()) {
    std::uniform_int_distribution<> prop_dist(
        0, indexable_numeric_properties.size() - 1);
    return indexable_numeric_properties[prop_dist(*random)];
  } else if (choice <= 90 && !all_properties.empty()) {
    std::uniform_int_distribution<> prop_dist(0, all_properties.size() - 1);
    return all_properties[prop_dist(*random)];
  } else {
    return "RandomProperty" + std::to_string(GetRandomInt(random, 0, 1000000));
  }
}

std::string GetRandomPropertyPath(MonkeyTestRandomEngine* random,
                                  MonkeyDocumentGenerator* document_generator) {
  const SchemaTypeConfigProto& type_config = document_generator->GetType();
  if (!type_config.properties().empty()) {
    std::uniform_int_distribution<> prop_dist(
        0, type_config.properties_size() - 1);
    return type_config.properties(prop_dist(*random)).property_name();
  }
  return "";
}

void GetRandomPropertyRestricts(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    std::unordered_set<std::string>& property_restricts) {
  std::string property_path = GetRandomPropertyPath(random, document_generator);
  if (!property_path.empty()) {
    property_restricts.insert(property_path);
  }
}

// A pair of SearchSpecProto and MonkeyAbstractQueryNode that should be
// equivalent.
struct MonkeyQueryPair {
  SearchSpecProto search_spec;
  std::unique_ptr<MonkeyAbstractQueryNode> query_node;
  struct PresentOperators {
    bool is_embedding_query = false;
    bool is_numeric_query = false;
    bool is_property_defined_query = false;
    bool is_has_property_query = false;
    bool is_negation_query = false;
  } present_operators;
};

// Forward declaration to allow for recursive generation of the query tree.
libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractQueryNode>>
GenerateRandomQueryTree(MonkeyTestRandomEngine* random,
                        MonkeyDocumentGenerator* document_generator,
                        SearchSpecProto& search_spec, int depth,
                        int num_children_per_nary_node,
                        MonkeyQueryPair::PresentOperators& present_operators);

std::unique_ptr<MonkeyTermQueryNode> GenerateRandomTermNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec,
    MonkeyQueryPair::PresentOperators& present_operators) {
  // 50% chance of getting a property restrict.
  std::unordered_set<std::string> property_restricts;
  if (GetRandomBoolean(random)) {
    GetRandomPropertyRestricts(random, document_generator, property_restricts);
  }

  // Get a random token from the language set as a single term query.
  std::string term = std::string(document_generator->GetToken());
  bool is_prefix = search_spec.term_match_type() == TermMatchType::PREFIX;
  if (is_prefix) {
    // Randomly drop a suffix of query to test prefix query.
    std::uniform_int_distribution<> size_dist(1, term.size());
    term.resize(size_dist(*random));
  }
  // TODO(b/491571627) - Decide on how to support queries with different match
  // types.
  auto query_node = std::make_unique<MonkeyTermQueryNode>(
      term, /*is_prefix=*/false, /*is_verbatim=*/false,
      search_spec.term_match_type(),
      /*document_namespaces=*/
      std::vector<std::string>(search_spec.namespace_filters().begin(),
                               search_spec.namespace_filters().end()),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()),
      /*property_restricts=*/
      std::move(property_restricts));
  return query_node;
}

std::unique_ptr<MonkeySemanticQueryNode> GenerateRandomSemanticNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec,
    MonkeyQueryPair::PresentOperators& present_operators) {
  // 50% chance of getting a property restrict.
  std::unordered_set<std::string> property_restricts;
  if (GetRandomBoolean(random)) {
    GetRandomPropertyRestricts(random, document_generator, property_restricts);
  }

  // Since our string representation of the query has a fixed precision of 2
  // decimal places, we'll compute the bounds as integers and then divide by 100
  // so that we can get values with 2 decimal places.
  std::uniform_int_distribution<int> range_dist(-100, 100);
  double low = range_dist(*random) / 100.0;
  double high = range_dist(*random) / 100.0;
  if (low > high) {
    std::swap(low, high);
  }

  static constexpr std::array<SearchSpecProto::EmbeddingQueryMetricType::Code,
                              3>
      kMetrics = {SearchSpecProto::EmbeddingQueryMetricType::COSINE,
                  SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
                  SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN};
  std::uniform_int_distribution<> metric_dist(0, kMetrics.size() - 1);
  SearchSpecProto::EmbeddingQueryMetricType::Code metric_type =
      kMetrics[metric_dist(*random)];
  PropertyProto::VectorProto vector =
      document_generator->GetRandomVector(/*allow_quantized_value=*/true);

  *search_spec.add_embedding_query_vectors() = vector;
  int vector_index = search_spec.embedding_query_vectors_size() - 1;

  // TODO(b/491571627) - Add support for multiple embedding query vectors.
  auto query_node = std::make_unique<MonkeySemanticQueryNode>(
      /*vector_index=*/vector_index, low, high, metric_type,
      search_spec.embedding_query_nprobe(), std::move(vector),
      /*property_restricts=*/std::move(property_restricts),
      /*document_namespaces=*/
      std::vector<std::string>(search_spec.namespace_filters().begin(),
                               search_spec.namespace_filters().end()),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  present_operators.is_embedding_query = true;
  return query_node;
}

std::unique_ptr<MonkeyPropertyDefinedQueryNode>
GenerateRandomPropertyDefinedNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec,
    MonkeyQueryPair::PresentOperators& present_operators) {
  std::string property_path = GetRandomPropertyPath(random, document_generator);
  search_spec.add_enabled_features(kListFilterQueryLanguageFeature);
  present_operators.is_property_defined_query = true;
  return std::make_unique<MonkeyPropertyDefinedQueryNode>(
      property_path,
      /*document_namespaces=*/
      std::vector<std::string>(search_spec.namespace_filters().begin(),
                               search_spec.namespace_filters().end()),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
}

std::unique_ptr<MonkeyAbstractLeafQueryNode> GenerateRandomNumericNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec,
    MonkeyQueryPair::PresentOperators& present_operators) {
  std::string property_path = PickRandomPropertyPathForNumericQuery(
      random, document_generator->schema());

  //'!=' (kNotEqual) is currently not supported by Icing.
  std::vector<MonkeyNumericQueryNode::NumericComparator> valid_comparators = {
      MonkeyNumericQueryNode::NumericComparator::kEqual,
      MonkeyNumericQueryNode::NumericComparator::kLessThan,
      MonkeyNumericQueryNode::NumericComparator::kLessThanEqual,
      MonkeyNumericQueryNode::NumericComparator::kGreaterThan,
      MonkeyNumericQueryNode::NumericComparator::kGreaterThanEqual,
  };
  std::uniform_int_distribution<> comp_dist(0, valid_comparators.size() - 1);
  MonkeyNumericQueryNode::NumericComparator comparator =
      valid_comparators[comp_dist(*random)];

  int64_t value = document_generator->GetRandomInt64Value();

  auto query_node = std::make_unique<MonkeyNumericQueryNode>(
      property_path, comparator, value,
      /*document_namespaces=*/
      std::vector<std::string>(search_spec.namespace_filters().begin(),
                               search_spec.namespace_filters().end()),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
  search_spec.add_enabled_features(std::string(kNumericSearchFeature));
  present_operators.is_numeric_query = true;
  return query_node;
}

std::unique_ptr<MonkeyHasPropertyQueryNode> GenerateRandomHasPropertyQueryNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec,
    MonkeyQueryPair::PresentOperators& present_operators) {
  std::string property_name = GetRandomPropertyPath(random, document_generator);
  search_spec.add_enabled_features(kListFilterQueryLanguageFeature);
  search_spec.add_enabled_features(kHasPropertyFunctionFeature);

  present_operators.is_has_property_query = true;
  return std::make_unique<MonkeyHasPropertyQueryNode>(
      property_name,
      /*document_namespaces=*/
      std::vector<std::string>(search_spec.namespace_filters().begin(),
                               search_spec.namespace_filters().end()),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
}

// Vector of functions that generate a random leaf node. Adding a new leaf node
// generator to this list will enable the generation of that type of leaf node.
std::vector<std::function<std::unique_ptr<MonkeyAbstractLeafQueryNode>(
    MonkeyTestRandomEngine*, MonkeyDocumentGenerator*, SearchSpecProto&,
    MonkeyQueryPair::PresentOperators&)>>
    leaf_node_generators = {GenerateRandomTermNode, GenerateRandomSemanticNode,
                            GenerateRandomNumericNode,
                            GenerateRandomPropertyDefinedNode,
                            GenerateRandomHasPropertyQueryNode};

libtextclassifier3::StatusOr<std::unique_ptr<MonkeyNotQueryNode>>
GenerateRandomNotNode(MonkeyTestRandomEngine* random,
                      MonkeyDocumentGenerator* document_generator,
                      SearchSpecProto& search_spec, int depth,
                      int num_children_per_nary_node,
                      MonkeyQueryPair::PresentOperators& present_operators) {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<MonkeyAbstractQueryNode> child_node,
                         GenerateRandomQueryTree(
                             random, document_generator, search_spec, depth - 1,
                             num_children_per_nary_node, present_operators));
  present_operators.is_negation_query = true;
  return std::make_unique<MonkeyNotQueryNode>(
      std::move(child_node),
      /*document_namespaces=*/
      std::vector<std::string>(search_spec.namespace_filters().begin(),
                               search_spec.namespace_filters().end()),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
}

libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractNaryQueryNode>>
GenerateRandomLogicalConjunctionNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec, int depth, int num_children_per_nary_node,
    bool is_and_node, MonkeyQueryPair::PresentOperators& present_operators) {
  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes;
  child_nodes.reserve(num_children_per_nary_node);
  for (int i = 0; i < num_children_per_nary_node; ++i) {
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<MonkeyAbstractQueryNode> child_node,
        GenerateRandomQueryTree(random, document_generator, search_spec,
                                depth - 1, num_children_per_nary_node,
                                present_operators));
    child_nodes.push_back(std::move(child_node));
  }
  if (is_and_node) {
    return std::make_unique<MonkeyAndQueryNode>(std::move(child_nodes));
  } else {
    return std::make_unique<MonkeyOrQueryNode>(std::move(child_nodes));
  }
}

libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractNaryQueryNode>>
GenerateRandomAndNode(MonkeyTestRandomEngine* random,
                      MonkeyDocumentGenerator* document_generator,
                      SearchSpecProto& search_spec, int depth,
                      int num_children_per_nary_node,
                      MonkeyQueryPair::PresentOperators& present_operators) {
  return GenerateRandomLogicalConjunctionNode(
      random, document_generator, search_spec, depth,
      num_children_per_nary_node, /*is_and_node=*/true, present_operators);
}

libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractNaryQueryNode>>
GenerateRandomOrNode(MonkeyTestRandomEngine* random,
                     MonkeyDocumentGenerator* document_generator,
                     SearchSpecProto& search_spec, int depth,
                     int num_children_per_nary_node,
                     MonkeyQueryPair::PresentOperators& present_operators) {
  return GenerateRandomLogicalConjunctionNode(
      random, document_generator, search_spec, depth,
      num_children_per_nary_node, /*is_and_node=*/false, present_operators);
}

std::vector<std::function<
    libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractNaryQueryNode>>(
        MonkeyTestRandomEngine*, MonkeyDocumentGenerator*, SearchSpecProto&,
        int, int, MonkeyQueryPair::PresentOperators&)>>
    nary_node_generators = {GenerateRandomAndNode, GenerateRandomOrNode,
                            GenerateRandomNotNode};

// Generates a random query tree with the given depth.
// As a part of generating the query tree, the some fields in the
// SearchSpecProto will also be written and read to (depending on the type of
// query generated).
libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractQueryNode>>
GenerateRandomQueryTree(MonkeyTestRandomEngine* random,
                        MonkeyDocumentGenerator* document_generator,
                        SearchSpecProto& search_spec, int depth,
                        int num_children_per_nary_node,
                        MonkeyQueryPair::PresentOperators& present_operators) {
  if (depth <= 0) {
    return absl_ports::InvalidArgumentError("Depth must be positive.");
  }
  // Generate a random leaf node.
  if (depth == 1) {
    int leaf_node_generator_index =
        GetRandomInt(random, 0, leaf_node_generators.size() - 1);
    return leaf_node_generators[leaf_node_generator_index](
        random, document_generator, search_spec, present_operators);
  } else {
    int nary_node_generator_index =
        GetRandomInt(random, 0, nary_node_generators.size() - 1);
    return nary_node_generators[nary_node_generator_index](
        random, document_generator, search_spec, depth,
        num_children_per_nary_node, present_operators);
  }
}

libtextclassifier3::StatusOr<MonkeyQueryPair> GenerateRandomMonkeyQueryPair(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    int depth = 1, int num_children_per_nary_node = 2) {
  SearchSpecProto search_spec;

  // %50 chance of getting one type filter
  // %25 chance of getting two type filters
  // %25 chance of getting no type filters
  std::vector<std::string> type_filters;
  for (int i = 0; i < 2; ++i) {
    if (GetRandomBoolean(random)) {
      type_filters.push_back(document_generator->GetType().schema_type());
    }
  }
  search_spec.mutable_schema_type_filters()->Add(type_filters.begin(),
                                                 type_filters.end());

  if (document_generator->num_namespaces() > 0) {
    // %50 chance of getting one namespace filter
    // %25 chance of getting two namespace filters
    // %25 chance of getting no namespace filters
    std::vector<std::string> namespace_filters;
    for (int i = 0; i < 2; ++i) {
      if (GetRandomBoolean(random)) {
        namespace_filters.push_back(document_generator->GetNamespace());
      }
    }
    search_spec.mutable_namespace_filters()->Add(namespace_filters.begin(),
                                                 namespace_filters.end());
  }
  if (GetRandomBoolean(random)) {
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  } else {
    search_spec.set_term_match_type(TermMatchType::PREFIX);
  }

  // 0 means ANN is not enabled. >0 means ANN is enabled and still includes
  // the linear search part. The in-memory Icing search engine cannot truly
  // verify the behavior of the ANN index algorithmically, as IVF-based ANN
  // is an approximate non-deterministic mapping. As a result, we set a wildly
  // large nprobe to let ANN degenerate into linear search, granting exact
  // matching for verification.
  int nprobe = GetRandomBoolean(random) ? 0 : 100000000;
  search_spec.set_embedding_query_nprobe(nprobe);

  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));

  // Set default metric. Semantic nodes generated in this monkey test explicitly
  // specify their metric in the query string, overriding this.
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::COSINE);

  MonkeyQueryPair::PresentOperators present_operators;
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<MonkeyAbstractQueryNode> query_node,
      GenerateRandomQueryTree(random, document_generator, search_spec, depth,
                              num_children_per_nary_node, present_operators));

  search_spec.set_query(query_node->GenerateQueryString());

  return MonkeyQueryPair{.search_spec = std::move(search_spec),
                         .query_node = std::move(query_node),
                         .present_operators = std::move(present_operators)};
}

ScoringSpecProto GenerateRandomScoringSpec(MonkeyTestRandomEngine* random) {
  ScoringSpecProto scoring_spec;

  // TODO(b/535253143):  Support RELEVANCE_SCORE for in-memory Icing search
  // engine. In order to test GetNextPage, we need to be able to replicate the
  // ordering of results in in-memory Icing. However, this is hard to do so for
  // RELEVANCE_SCORE. As such we will disable RELEVANCE_SCORE for now.
  constexpr std::array<ScoringSpecProto::RankingStrategy::Code, 2>
      ranking_strategies = {
          ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE,
          ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP};

  std::uniform_int_distribution<> dist(0, ranking_strategies.size() - 1);
  scoring_spec.set_rank_by(ranking_strategies[dist(*random)]);
  return scoring_spec;
}

ResultSpecProto::SnippetSpecProto GenerateRandomSnippetSpecProto(
    MonkeyTestRandomEngine* random, const ResultSpecProto& result_spec) {
  ResultSpecProto::SnippetSpecProto snippet_spec;

  std::uniform_int_distribution<> num_to_snippet_dist(
      0, result_spec.num_per_page() * 2);
  snippet_spec.set_num_to_snippet(num_to_snippet_dist(*random));

  std::uniform_int_distribution<> num_matches_per_property_dist(0, 10);
  snippet_spec.set_num_matches_per_property(
      num_matches_per_property_dist(*random));

  std::uniform_int_distribution<> dist(0, 4);
  int random_num = dist(*random);
  // 1/5 chance of getting one of 0 (disabled), 8, 32, 128, 512
  int max_window_utf32_length =
      random_num == 0 ? 0 : (1 << (2 * random_num + 1));
  snippet_spec.set_max_window_utf32_length(max_window_utf32_length);
  snippet_spec.set_get_embedding_match_info(GetRandomBoolean(random));
  return snippet_spec;
}

TypePropertyMask GenerateTypePropertyMask(
    MonkeyTestRandomEngine* random, const SchemaTypeConfigProto& type_config) {
  TypePropertyMask type_property_mask;
  type_property_mask.set_schema_type(type_config.schema_type());
  for (const auto& properties : type_config.properties()) {
    // 25% chance of adding the current property to the mask.
    std::uniform_int_distribution<> dist(0, 3);
    if (dist(*random) == 0) {
      type_property_mask.add_paths(properties.property_name());
    }
  }
  return type_property_mask;
}

ResultSpecProto GenerateRandomResultSpecProto(MonkeyTestRandomEngine* random,
                                              const SchemaProto* schema,
                                              bool is_nested) {
  // TODO(b/491577935): support nested snippet and projection.
  std::uniform_int_distribution<> dist(0, 4);
  ResultSpecProto result_spec;
  // 1/5 chance of getting one of 1, 4, 16, 64, 256
  int num_per_page = 1 << (2 * dist(*random));
  result_spec.set_num_per_page(num_per_page);
  result_spec.set_num_to_score(std::numeric_limits<int32_t>::max());
  if (!is_nested) {
    *result_spec.mutable_snippet_spec() =
        GenerateRandomSnippetSpecProto(random, result_spec);
  }

  // 1/5 chance of enabling projection.
  if (!is_nested && dist(*random) == 0) {
    for (const SchemaTypeConfigProto& type_config : schema->types()) {
      // 25% chance of adding the current type to the projection.
      std::uniform_int_distribution<> dist(0, 3);
      if (dist(*random) == 0) {
        *result_spec.add_type_property_masks() =
            GenerateTypePropertyMask(random, type_config);
      }
    }
  }
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int>::max());
  return result_spec;
}

// A pair of JoinSpecProto and InMemoryIcingSearchEngine::JoinQuerySpec that
// should be equivalent.
struct MonkeyJoinSpecPair {
  JoinSpecProto join_spec;
  InMemoryIcingSearchEngine::JoinQuerySpec join_query_spec;
};
libtextclassifier3::StatusOr<MonkeyJoinSpecPair>
GenerateRnadomMonkeyJoinSpecPair(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    InMemoryIcingSearchEngine* in_memory_icing,
    const std::vector<std::string>& candidate_join_properties) {
  // 1.1: Generate a random MonkeyQueryPair for the child query.
  ICING_ASSIGN_OR_RETURN(
      MonkeyQueryPair child_query_pair,
      GenerateRandomMonkeyQueryPair(random, document_generator));

  // 1.2: Parent join property expression: currently it is always the qualified
  //      id expression.
  std::string parent_join_prop_expr(JoinProcessor::kQualifiedIdExpr);

  // 1.3: Child join property expression: randomly pick a join property.
  std::string child_join_prop_expr = "NonExistentJoinProperty";
  if (!candidate_join_properties.empty()) {
    std::uniform_int_distribution<> dist(
        0, static_cast<int>(candidate_join_properties.size()) - 1);
    child_join_prop_expr = candidate_join_properties[dist(*random)];
  }

  // 2. Convert to JoinSpecProto.
  JoinSpecProto join_spec;
  *join_spec.mutable_nested_spec()->mutable_search_spec() = std::move(
      child_query_pair.search_spec);  // Nested search spec is moved from the
                                      // child query pair's search spec.
  *join_spec.mutable_nested_spec()->mutable_scoring_spec() =
      GenerateRandomScoringSpec(random);
  *join_spec.mutable_nested_spec()->mutable_result_spec() =
      GenerateRandomResultSpecProto(random, in_memory_icing->GetSchema(),
                                    /*is_nested=*/true);
  join_spec.set_parent_property_expression(parent_join_prop_expr);
  join_spec.set_child_property_expression(child_join_prop_expr);

  // 3. Convert to InMemoryIcingSearchEngine::JoinQuerySpec.
  InMemoryIcingSearchEngine::JoinQuerySpec join_query_spec = {
      .prev_join_prop_expr = std::move(parent_join_prop_expr),
      .curr_join_prop_expr = std::move(child_join_prop_expr),
      .curr_query_node = std::move(child_query_pair.query_node)};

  return MonkeyJoinSpecPair{.join_spec = std::move(join_spec),
                            .join_query_spec = std::move(join_query_spec)};
}

template <typename T>
void SortResults(std::vector<T>& results) {
  struct DocumentExtractor {
    const DocumentProto& operator()(
        const SearchResultProto::ResultProto& r) const {
      return r.document();
    }
    const DocumentProto& operator()(
        const SearchResultProto::ResultProto* r) const {
      return r->document();
    }
  } get_document;

  std::sort(results.begin(), results.end(),
            [&get_document](const T& result1, const T& result2) {
              const DocumentProto& doc1 = get_document(result1);
              const DocumentProto& doc2 = get_document(result2);
              if (doc1.namespace_() != doc2.namespace_()) {
                return doc1.namespace_() < doc2.namespace_();
              }
              return doc1.uri() < doc2.uri();
            });
}

void CompareSearchResultProto(
    const SearchResultProto::ResultProto& actual_result,
    const SearchResultProto::ResultProto& exp_result,
    bool is_projection_enabled) {
  if (is_projection_enabled) {
    ASSERT_THAT(actual_result.document().namespace_(),
                Eq(exp_result.document().namespace_()));
    ASSERT_THAT(actual_result.document().uri(),
                Eq(exp_result.document().uri()));
  } else {
    ASSERT_THAT(actual_result.document(), EqualsProto(exp_result.document()));
  }

  // Compare joined results.
  ASSERT_THAT(actual_result.joined_results(),
              SizeIs(exp_result.joined_results().size()));
  std::vector<const SearchResultProto::ResultProto*> actual_joined_results;
  std::vector<const SearchResultProto::ResultProto*> exp_joined_results;
  actual_joined_results.reserve(actual_result.joined_results().size());
  exp_joined_results.reserve(exp_result.joined_results().size());
  for (int i = 0; i < actual_result.joined_results().size(); ++i) {
    actual_joined_results.push_back(&actual_result.joined_results(i));
    exp_joined_results.push_back(&exp_result.joined_results(i));
  }
  SortResults(actual_joined_results);
  SortResults(exp_joined_results);
  for (int i = 0; i < actual_joined_results.size(); ++i) {
    // TODO(b/491577935): add support for nested projection.
    CompareSearchResultProto(*actual_joined_results[i], *exp_joined_results[i],
                             /*is_projection_enabled=*/false);
  }
}

}  // namespace

IcingMonkeyTestRunner::IcingMonkeyTestRunner(
    IcingMonkeyTestRunnerConfiguration config)
    : clock_(std::make_unique<Clock>()),
      config_(std::move(config)),
      random_(config_.seed),
      in_memory_icing_(std::make_unique<InMemoryIcingSearchEngine>(
          &random_, config_.enable_join_delete_propagation)),
      schema_generator_(
          std::make_unique<MonkeySchemaGenerator>(&random_, &config_)) {
  ICING_LOG(INFO) << "Monkey test runner started with seed: " << config_.seed;
  icing_dir_ = GetTestTempDir() + "/icing/monkey";
}

void IcingMonkeyTestRunner::Run(uint32_t num) {
  ASSERT_TRUE(icing_ != nullptr)
      << "Icing search engine has not yet been created. Please call "
         "Initialize() first";

  for (uint32_t i = 0; i < num; ++i) {
    auto api_to_call =
        GetRandomWeightedElement(&random_, config_.monkey_api_schedules);
    ASSERT_NO_FATAL_FAILURE(api_to_call(this));
    ICING_LOG(INFO) << "Completed Run #" << i
                    << ". Documents in the in-memory icing: "
                    << in_memory_icing_->GetNumAliveDocuments();
  }
}

SetSchemaResultProto IcingMonkeyTestRunner::SetSchema(SchemaProto&& schema) {
  in_memory_icing_->SetSchema(std::move(schema));
  document_generator_ = std::make_unique<MonkeyDocumentGenerator>(
      &random_, in_memory_icing_->GetSchema(), &config_);
  return icing_->SetSchema(*in_memory_icing_->GetSchema(),
                           /*ignore_errors_and_delete_documents=*/true);
}

void IcingMonkeyTestRunner::Initialize() {
  // Check that the configuration is valid for join delete propagation.
  if (config_.enable_join_delete_propagation) {
    ASSERT_TRUE(config_.IsJoinEnabled());
  }

  if (config_.initialize_by_existing_data) {
    ICING_LOG(INFO) << "Initializing icing by existing data";

    ASSERT_NO_FATAL_FAILURE(CreateIcingSearchEngine());
    ASSERT_NO_FATAL_FAILURE(ReloadInMemoryIcing());
  } else {
    ICING_LOG(INFO) << "Initializing icing by empty data";

    filesystem_.DeleteDirectoryRecursively(icing_dir_.c_str());
    ASSERT_NO_FATAL_FAILURE(CreateIcingSearchEngine());
    SchemaProto schema = schema_generator_->GenerateSchema();
    ICING_LOG(DBG) << "Schema Generated: " << schema.DebugString();
    ASSERT_THAT(SetSchema(std::move(schema)).status(), ProtoIsOk());
  }
}

void IcingMonkeyTestRunner::DoUpdateSchema() {
  ICING_LOG(INFO) << "Monkey updating schema";

  MonkeySchemaGenerator::UpdateSchemaResult result =
      schema_generator_->UpdateSchema(*in_memory_icing_->GetSchema());
  if (result.is_invalid_schema) {
    SetSchemaResultProto set_schema_result =
        icing_->SetSchema(result.schema,
                          /*ignore_errors_and_delete_documents=*/true);
    ASSERT_THAT(set_schema_result.status(), Not(ProtoIsOk()));
    return;
  }

  ICING_LOG(DBG) << "Updating schema to: " << result.schema.DebugString();
  SetSchemaResultProto icing_set_schema_result =
      SetSchema(std::move(result.schema));
  ASSERT_THAT(icing_set_schema_result.status(), ProtoIsOk());
  ASSERT_THAT(icing_set_schema_result.deleted_schema_types(),
              UnorderedElementsAreArray(result.schema_types_deleted));
  ASSERT_THAT(icing_set_schema_result.incompatible_schema_types(),
              UnorderedElementsAreArray(result.schema_types_incompatible));
  ASSERT_THAT(
      icing_set_schema_result.index_incompatible_changed_schema_types(),
      UnorderedElementsAreArray(result.schema_types_index_incompatible));
  ASSERT_THAT(icing_set_schema_result.join_incompatible_changed_schema_types(),
              UnorderedElementsAreArray(result.schema_types_join_incompatible));

  ICING_ASSERT_OK_AND_ASSIGN(
      int num_deleted_documents,
      in_memory_icing_->RevalidateDocuments(result.schema_types_deleted,
                                            result.schema_types_incompatible));
  ASSERT_THAT(icing_set_schema_result.deleted_document_count(),
              Eq(num_deleted_documents));
  int64_t update_ts_ms = clock_->GetSystemTimeMilliseconds();
  for (const std::string& schema_type :
       result.schema_types_index_incompatible) {
    // Insert or update the timestamp of the last index incompatible update.
    schema_to_last_index_incompatible_update_[schema_type] = update_ts_ms;
  }
}

void IcingMonkeyTestRunner::DoGet() {
  InMemoryIcingSearchEngine::PickDocumentResult document =
      in_memory_icing_->RandomPickDocument(/*p_alive=*/0.70, /*p_all=*/0.28,
                                           /*p_other=*/0.02);
  ICING_LOG(INFO) << "Monkey getting namespace: " << document.name_space
                  << ", uri: " << document.uri;
  GetResultProto get_result =
      icing_->Get(document.name_space, document.uri,
                  GetResultSpecProto::default_instance());
  if (document.document.has_value()) {
    ASSERT_THAT(get_result.status(), ProtoIsOk())
        << "Cannot find the document that is supposed to exist.";
    ASSERT_THAT(get_result.document(), EqualsProto(document.document.value()))
        << "The document found does not match with the value in the in-memory "
           "icing.";
  } else {
    // Should expect that no document has been found.
    if (get_result.status().code() != StatusProto::NOT_FOUND) {
      if (get_result.status().code() == StatusProto::OK) {
        FAIL() << "Found a document that is not supposed to be found.";
      }
      FAIL() << "Icing search engine failure (code "
             << get_result.status().code()
             << "): " << get_result.status().message();
    }
  }
}

void IcingMonkeyTestRunner::DoGetAllNamespaces() {
  ICING_LOG(INFO) << "Monkey getting all namespaces";
  GetAllNamespacesResultProto get_result = icing_->GetAllNamespaces();
  ASSERT_THAT(get_result.status(), ProtoIsOk());
  ASSERT_THAT(get_result.namespaces(),
              UnorderedElementsAreArray(in_memory_icing_->GetAllNamespaces()));
}

void IcingMonkeyTestRunner::DoPut() {
  MonkeyTokenizedDocument doc = document_generator_->GenerateDocument();
  ICING_LOG(INFO) << "Monkey document generated, namespace: "
                  << doc.document.namespace_()
                  << ", uri: " << doc.document.uri();
  ICING_LOG(DBG) << doc.document.DebugString();

  // Put the document into the in-memory icing. If it fails due to unsatisfied
  // dependency of delete propagation, then expect the same result from the
  // Icing search engine.
  auto status = in_memory_icing_->Put(doc);
  if (status.ok()) {
    ASSERT_THAT(icing_->Put(doc.document).status(), ProtoIsOk());
  } else {
    ASSERT_THAT(icing_->Put(doc.document).status(),
                ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
  }
}

void IcingMonkeyTestRunner::DoDelete() {
  InMemoryIcingSearchEngine::PickDocumentResult document =
      in_memory_icing_->RandomPickDocument(/*p_alive=*/0.70, /*p_all=*/0.2,
                                           /*p_other=*/0.1);
  ICING_LOG(INFO) << "Monkey deleting namespace: " << document.name_space
                  << ", uri: " << document.uri;
  DeleteResultProto delete_result =
      icing_->Delete(document.name_space, document.uri);
  if (document.document.has_value()) {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::vector<DocumentMetadata> deleted_documents,
        in_memory_icing_->Delete(document.name_space, document.uri));
    ASSERT_THAT(delete_result.status(), ProtoIsOk())
        << "Cannot delete an existing document.";

    // Verify # of deleted documents.
    //
    // Note:
    // - If delete propagation is enabled, then the number of deleted documents
    //   may be larger than 1.
    // - If delete propagation is not enabled, then the number of deleted
    //   documents should be exactly 1.
    ASSERT_THAT(deleted_documents,
                SizeIs(delete_result.delete_stats().num_documents_deleted()));
  } else {
    // Should expect that no document has been deleted.
    if (delete_result.status().code() != StatusProto::NOT_FOUND) {
      if (delete_result.status().code() == StatusProto::OK) {
        FAIL() << "Deleted a non-existing document without an error.";
      }
      FAIL() << "Icing search engine failure (code "
             << delete_result.status().code()
             << "): " << delete_result.status().message();
    }
  }
}

void IcingMonkeyTestRunner::DoDeleteByNamespace() {
  std::string name_space = document_generator_->GetNamespace();
  ICING_LOG(INFO) << "Monkey deleting namespace: " << name_space;
  DeleteByNamespaceResultProto delete_result =
      icing_->DeleteByNamespace(name_space);
  ICING_ASSERT_OK_AND_ASSIGN(uint32_t num_docs_deleted,
                             in_memory_icing_->DeleteByNamespace(name_space));
  if (num_docs_deleted != 0) {
    ASSERT_THAT(delete_result.status(), ProtoIsOk())
        << "Cannot delete an existing namespace.";
    ASSERT_THAT(delete_result.delete_stats().num_documents_deleted(),
                Eq(num_docs_deleted));
  } else {
    // Should expect that no document has been deleted.
    if (delete_result.status().code() != StatusProto::NOT_FOUND) {
      if (delete_result.status().code() == StatusProto::OK) {
        FAIL() << "Deleted a non-existing namespace without an error.";
      }
      FAIL() << "Icing search engine failure (code "
             << delete_result.status().code()
             << "): " << delete_result.status().message();
    }
  }
}

void IcingMonkeyTestRunner::DoDeleteBySchemaType() {
  std::string schema_type = document_generator_->GetType().schema_type();
  ICING_LOG(INFO) << "Monkey deleting type: " << schema_type;
  DeleteBySchemaTypeResultProto delete_result =
      icing_->DeleteBySchemaType(schema_type);
  ICING_ASSERT_OK_AND_ASSIGN(uint32_t num_docs_deleted,
                             in_memory_icing_->DeleteBySchemaType(schema_type));
  if (num_docs_deleted != 0) {
    ASSERT_THAT(delete_result.status(), ProtoIsOk())
        << "Cannot delete an existing schema type.";
    ASSERT_THAT(delete_result.delete_stats().num_documents_deleted(),
                Eq(num_docs_deleted));
  } else {
    // Should expect that no document has been deleted.
    if (delete_result.status().code() != StatusProto::NOT_FOUND) {
      if (delete_result.status().code() == StatusProto::OK) {
        FAIL() << "Deleted a non-existing schema type without an error.";
      }
      FAIL() << "Icing search engine failure (code "
             << delete_result.status().code()
             << "): " << delete_result.status().message();
    }
  }
}

void IcingMonkeyTestRunner::DoDeleteByQuery() {
  int tree_depth =
      GetRandomWeightedElement(&random_, config_.possible_query_tree_depths);
  ICING_ASSERT_OK_AND_ASSIGN(
      MonkeyQueryPair query_pair,
      GenerateRandomMonkeyQueryPair(&random_, document_generator_.get(),
                                    tree_depth));
  SearchSpecProto search_spec = query_pair.search_spec;
  ICING_LOG(INFO) << "Monkey deleting by query: " << search_spec.query();
  DeleteByQueryResultProto delete_result = icing_->DeleteByQuery(search_spec);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<DocumentMetadata> deleted_documents,
      in_memory_icing_->DeleteByQuery(query_pair.query_node.get()));
  if (!deleted_documents.empty()) {
    ASSERT_THAT(delete_result.status(), ProtoIsOk())
        << "Cannot delete documents that matches with the query.";
    ASSERT_THAT(
        deleted_documents,
        SizeIs(delete_result.delete_by_query_stats().num_documents_deleted()));
  } else {
    // Should expect that no document has been deleted.
    if (delete_result.status().code() != StatusProto::NOT_FOUND) {
      if (delete_result.status().code() == StatusProto::OK) {
        FAIL() << "Deleted documents that should not match with the query "
                  "without an error.";
      }
      FAIL() << "Icing search engine failure (code "
             << delete_result.status().code()
             << "): " << delete_result.status().message();
    }
  }
  ICING_LOG(INFO)
      << delete_result.delete_by_query_stats().num_documents_deleted()
      << " documents deleted by query.";
}

void IcingMonkeyTestRunner::DoSearch() {
  InternalSearch(/*is_join_search=*/false);
}

void IcingMonkeyTestRunner::DoJoinSearch() {
  InternalSearch(/*is_join_search=*/true);
}

void IcingMonkeyTestRunner::InternalSearch(bool is_join_search) {
  int tree_depth =
      GetRandomWeightedElement(&random_, config_.possible_query_tree_depths);
  std::uniform_int_distribution<> num_children_dist(
      0, config_.possible_num_children_per_nary_node.size() - 1);
  int num_children =
      config_.possible_num_children_per_nary_node[num_children_dist(random_)];
  // Top level search spec.
  ICING_ASSERT_OK_AND_ASSIGN(
      MonkeyQueryPair query_pair,
      GenerateRandomMonkeyQueryPair(&random_, document_generator_.get(),
                                    tree_depth, num_children));
  std::unique_ptr<SearchSpecProto> search_spec =
      std::make_unique<SearchSpecProto>(query_pair.search_spec);
  std::unique_ptr<ScoringSpecProto> scoring_spec =
      std::make_unique<ScoringSpecProto>(GenerateRandomScoringSpec(&random_));
  auto result_spec = std::make_unique<ResultSpecProto>(
      GenerateRandomResultSpecProto(&random_, in_memory_icing_->GetSchema(),
                                    /*is_nested=*/false));
  ResultSpecProto::SnippetSpecProto snippet_spec = result_spec->snippet_spec();
  bool is_projection_enabled = !result_spec->type_property_masks().empty();

  bool is_snippetable_query =
      snippet_spec.num_matches_per_property() > 0 && !is_projection_enabled &&
      !query_pair.present_operators.is_embedding_query &&
      !query_pair.present_operators.is_has_property_query &&
      !query_pair.present_operators.is_property_defined_query &&
      !query_pair.present_operators.is_numeric_query &&
      !query_pair.present_operators.is_negation_query;

  ICING_LOG(INFO) << "Monkey searching by query: " << search_spec->query()
                  << ", term_match_type: " << search_spec->term_match_type()
                  << ", nprobe: " << search_spec->embedding_query_nprobe();
  ICING_VLOG(1) << "search_spec:\n" << search_spec->DebugString();
  ICING_VLOG(1) << "scoring_spec:\n" << scoring_spec->DebugString();
  ICING_VLOG(1) << "result_spec:\n" << result_spec->DebugString();

  // Nested queries. The vector will remain empty if is_join_search is false.
  std::vector<InMemoryIcingSearchEngine::JoinQuerySpec> nested_queries;

  // Currently we only support 1 level of join.
  // If we support multiple levels of joins, then change num_nested_levels to a
  // random number greater than 0.
  int num_nested_levels = is_join_search ? 1 : 0;
  std::vector<std::string> candidate_join_properties =
      in_memory_icing_->GetAllJoinProperties();
  SearchSpecProto* current_search_spec = search_spec.get();
  for (int i = 0; i < num_nested_levels; ++i) {
    // Generate a monkey join spec pair and set to the current level of search
    // spec.
    ICING_ASSERT_OK_AND_ASSIGN(
        MonkeyJoinSpecPair monkey_join_query_pair,
        GenerateRnadomMonkeyJoinSpecPair(&random_, document_generator_.get(),
                                         in_memory_icing_.get(),
                                         candidate_join_properties));

    ICING_LOG(INFO)
        << "Monkey nested searching (level " << i + 1
        << " with child_property_expression: "
        << monkey_join_query_pair.join_spec.child_property_expression()
        << ") by query: "
        << monkey_join_query_pair.join_spec.nested_spec().search_spec().query()
        << ", term_match_type: "
        << monkey_join_query_pair.join_spec.nested_spec()
               .search_spec()
               .term_match_type();
    ICING_VLOG(1) << "search_spec:\n"
                  << monkey_join_query_pair.join_spec.nested_spec()
                         .search_spec()
                         .DebugString();
    ICING_VLOG(1) << "scoring_spec:\n"
                  << monkey_join_query_pair.join_spec.nested_spec()
                         .scoring_spec()
                         .DebugString();
    ICING_VLOG(1) << "result_spec:\n"
                  << monkey_join_query_pair.join_spec.nested_spec()
                         .result_spec()
                         .DebugString();

    nested_queries.push_back(std::move(monkey_join_query_pair.join_query_spec));

    *current_search_spec->mutable_join_spec() =
        std::move(monkey_join_query_pair.join_spec);
    current_search_spec = current_search_spec->mutable_join_spec()
                              ->mutable_nested_spec()
                              ->mutable_search_spec();
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      SearchResultProto exp_search_result,
      in_memory_icing_->Search(query_pair.query_node.get(), nested_queries,
                               *scoring_spec, result_spec->num_per_page()));

  SearchResultProto search_result =
      icing_->Search(*search_spec, *scoring_spec, *result_spec);
  ASSERT_THAT(search_result.status(), ProtoIsOk());

  // Delete all of the specs used in the search. GetNextPage should have no
  // problem because it shouldn't be keeping any references to them.
  search_spec.reset();
  scoring_spec.reset();
  result_spec.reset();

  int num_to_snippet = snippet_spec.num_to_snippet();
  int64_t query_ts_ms = clock_->GetSystemTimeMilliseconds();

  CheckResults(search_result, exp_search_result, query_ts_ms, num_to_snippet,
               snippet_spec.num_matches_per_property(), is_snippetable_query,
               is_projection_enabled);

  bool is_interleaved_get_next_page = GetRandomBoolean(&random_);

  if (is_interleaved_get_next_page) {
    ICING_LOG(INFO) << "Monkey interleaving search";
    // If there are more results, then add the next page token to the map.
    if (search_result.next_page_token() != kInvalidNextPageToken) {
      ICING_LOG(INFO) << "Monkey adding real next page token: "
                      << search_result.next_page_token()
                      << " corresponding to in-memory next page token: "
                      << exp_search_result.next_page_token();
      auto [itr, inserted] = in_memory_token_to_icing_token_.insert(
          {exp_search_result.next_page_token(),
           {
               search_result.next_page_token(),
               query_ts_ms,
               num_to_snippet,
               snippet_spec.num_matches_per_property(),
               is_snippetable_query,
               is_projection_enabled,
           }});
      if (!inserted) {
        FAIL() << "Duplicate in-memory Icing next page token: "
               << exp_search_result.next_page_token();
      }
    }
  } else {
    ICING_LOG(INFO) << "Monkey getting all results in this search";
    // Keep getting the next page until there are no more pages.
    while (search_result.next_page_token() != kInvalidNextPageToken) {
      search_result = icing_->GetNextPage(search_result.next_page_token());
      ASSERT_THAT(search_result.status(), ProtoIsOk());
      ICING_ASSERT_OK_AND_ASSIGN(
          SearchResultProto exp_search_result,
          in_memory_icing_->GetNextPage(exp_search_result.next_page_token()));
      CheckResults(search_result, exp_search_result, query_ts_ms,
                   num_to_snippet, snippet_spec.num_matches_per_property(),
                   is_snippetable_query, is_projection_enabled);
    }
  }
}

void IcingMonkeyTestRunner::DoGetNextPage() {
  if (in_memory_token_to_icing_token_.empty()) {
    ICING_LOG(INFO) << "Monkey has no tokens to get next page";
    return;
  }
  ICING_LOG(INFO) << "Monkey getting next page";

  int random_index =
      GetRandomInt(&random_, 0, in_memory_token_to_icing_token_.size() - 1);
  auto itr = std::next(in_memory_token_to_icing_token_.begin(), random_index);
  uint64_t in_memory_next_page_token = itr->first;
  QueryState& query_state = itr->second;
  uint64_t icing_next_page_token = query_state.next_page_token;
  ICING_LOG(INFO) << "Monkey getting next page using real next page token: "
                  << icing_next_page_token;
  SearchResultProto search_result = icing_->GetNextPage(icing_next_page_token);
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  if (search_result.has_page_token_not_found()) {
    // The page token was not found, so we are done getting results for this
    // token.
    ICING_LOG(INFO) << "Real Icing token: " << icing_next_page_token
                    << " was invalidated";
    in_memory_token_to_icing_token_.erase(in_memory_next_page_token);
    return;
  }

  ICING_LOG(INFO)
      << "Monkey getting next page using in-memory next page token: "
      << in_memory_next_page_token;
  ICING_ASSERT_OK_AND_ASSIGN(
      SearchResultProto expected_search_result,
      in_memory_icing_->GetNextPage(in_memory_next_page_token));

  CheckResults(search_result, expected_search_result, query_state);

  // If there are no more results, then remove the next page token from the map.
  if (search_result.next_page_token() == kInvalidNextPageToken) {
    in_memory_token_to_icing_token_.erase(in_memory_next_page_token);
  }
}

void IcingMonkeyTestRunner::DoGetDebugInfo() {
  ICING_LOG(INFO) << "Monkey getting debug info";
  int verbosity_code = GetRandomInt(&random_, DebugInfoVerbosity::Code_MIN,
                                    DebugInfoVerbosity::Code_MAX);
  DebugInfoVerbosity::Code verbosity =
      static_cast<DebugInfoVerbosity::Code>(verbosity_code);
  DebugInfoResultProto get_debug_info_result = icing_->GetDebugInfo(verbosity);
  ASSERT_THAT(get_debug_info_result.status(), ProtoIsOk());
}

void IcingMonkeyTestRunner::DoPersistToDisk() {
  PersistType::Code persist_type = static_cast<PersistType::Code>(
      GetRandomInt(&random_, 1, PersistType::Code_MAX));
  ICING_LOG(INFO) << "Monkey persisting to disk type: " << persist_type;
  ASSERT_THAT(icing_->PersistToDisk(persist_type).status(), ProtoIsOk());
}

void IcingMonkeyTestRunner::ReloadFromDisk() {
  ICING_LOG(INFO) << "Monkey reloading from disk";

  // The IcingSearchEngine destructor does not automatically persist data to
  // disk in the monkey test environment. We introduce a 50% probability of
  // invoking PersistToDisk() (with a randomly selected persist type) and a 50%
  // probability of skipping it to simulate an unclean shutdown (e.g. crash or
  // power loss).
  if (GetRandomBoolean(&random_)) {
    ASSERT_NO_FATAL_FAILURE(DoPersistToDisk());
  }

  // Destruct the icing search engine by resetting the unique pointer.
  icing_.reset();
  ASSERT_NO_FATAL_FAILURE(CreateIcingSearchEngine());
}

void IcingMonkeyTestRunner::DoOptimize() {
  ICING_LOG(INFO) << "Monkey doing optimization";
  ASSERT_THAT(icing_->Optimize().status(), ProtoIsOk());
}

void IcingMonkeyTestRunner::DoMaintainAnnIndex() {
  ICING_LOG(INFO) << "Monkey maintaining ANN index";
  ASSERT_THAT(
      icing_->MaintainAnnIndex(config_.maintain_ann_index_options).status(),
      ProtoIsOk());
}

void IcingMonkeyTestRunner::CreateIcingSearchEngine() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_index_merge_size(config_.index_merge_size);
  icing_options.set_base_dir(icing_dir_);
  // 0.9 is the value always used in AppSearch.
  icing_options.set_optimize_rebuild_index_threshold(0.9);
  // The method will be called every time when we ReloadFromDisk(), so randomly
  // flip this flag to test document store's compatibility.
  icing_options.set_document_store_namespace_id_fingerprint(
      GetRandomBoolean(&random_));
  icing_options.set_compression_threshold_bytes(
      GetRandomInt(&random_, /*min=*/0, /*max=*/10000));

  // Randomly choose the number of shards.
  uint32_t num_shards = config_.possible_num_shards[GetRandomInt(
      &random_, /*min=*/0, /*max=*/config_.possible_num_shards.size() - 1)];
  icing_options.set_embedding_index_num_shards(num_shards);
  icing_options.set_enable_skip_set_schema_type_equality_check(
      GetRandomBoolean(&random_));
  icing_options.set_enable_optimize_improvements(true);
  icing_options.set_enable_manual_persist_to_disk(true);
  icing_options.set_enable_repeated_field_joins(true);
  icing_options.set_enable_non_existent_qualified_id_join(true);
  icing_options.set_enable_background_task_scheduler(true);
  icing_options.set_enable_delete_propagation_from(
      config_.enable_join_delete_propagation);
  icing_options.set_enable_fine_grained_index_rebuild(
      GetRandomBoolean(&random_));

  icing_options.set_enable_schema_definition_deduping(true);
  icing_options.set_build_property_existence_metadata_hits(true);
  icing_options.set_schema_store_release_cached_proto_after_use(
      GetRandomBoolean(&random_));
  icing_options.set_remove_schema_store_move_assignment(
      GetRandomBoolean(&random_));
  icing_ = std::make_unique<IcingSearchEngine>(icing_options);
  ASSERT_THAT(icing_->Initialize().status(), ProtoIsOk());
}

void IcingMonkeyTestRunner::ReloadInMemoryIcing() {
  // Reload schema
  GetSchemaResultProto get_schema_result = icing_->GetSchema();
  ASSERT_THAT(get_schema_result.status(), ProtoIsOk());
  in_memory_icing_->SetSchema(get_schema_result.schema());
  ASSERT_THAT(*in_memory_icing_->GetSchema(),
              EqualsProto(get_schema_result.schema()));
  document_generator_ = std::make_unique<MonkeyDocumentGenerator>(
      &random_, in_memory_icing_->GetSchema(), &config_);

  // Reload documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");
  ResultSpecProto result_spec;
  result_spec.set_num_to_score(std::numeric_limits<int32_t>::max());
  result_spec.set_num_per_page(100);
  int num_results = 0;
  int max_uri = 0;
  SearchResultProto search_result = icing_->Search(
      search_spec, ScoringSpecProto::default_instance(), result_spec);
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  while (true) {
    num_results += search_result.results_size();
    for (const SearchResultProto::ResultProto& doc : search_result.results()) {
      in_memory_icing_->Put(MonkeyTokenizedDocument::Reload(doc.document()));
      max_uri = std::max(
          max_uri, std::stoi(doc.document().uri().substr(
                       MonkeyDocumentGenerator::kDocumentUriPrefix.size())));
    }
    if (search_result.next_page_token() == kInvalidNextPageToken) {
      break;
    }
    search_result = icing_->GetNextPage(search_result.next_page_token());
    ASSERT_THAT(search_result.status(), ProtoIsOk());
  }
  ICING_LOG(INFO) << "Reloaded " << num_results << " documents";

  // Reload generators
  schema_generator_->ReloadPreviousStatus(*in_memory_icing_->GetSchema());
  document_generator_->ReloadPreviousStatus(max_uri);
}

void IcingMonkeyTestRunner::CheckResults(
    const SearchResultProto& actual_search_result,
    const SearchResultProto& expected_search_result, int64_t query_timestamp_ms,
    int& num_to_snippet, int num_matches_per_property,
    bool& is_snippetable_query, bool is_projection_enabled) {
  std::vector<SearchResultProto::ResultProto> actual_results;
  actual_results.reserve(actual_search_result.results().size());
  int num_snippeted = 0;
  for (const SearchResultProto::ResultProto& result :
       actual_search_result.results()) {
    actual_results.push_back(result);
    auto itr = schema_to_last_index_incompatible_update_.find(
        result.document().schema());
    // If the query timestamp is before the last index incompatible update, then
    // we disable checking snippets for this page and subsequent pages.
    // TODO(b/542593471): Remove once the incorrect snippet due to pagination
    // state and icing internal id changes is fixed.
    if (itr != schema_to_last_index_incompatible_update_.end() &&
        query_timestamp_ms <= itr->second) {
      ICING_LOG(INFO)
          << "Disabling snippet checking for query; query was made at "
             "timestamp: "
          << query_timestamp_ms
          << " and has result with schema: " << result.document().schema()
          << " which had last index incompatible update at timestamp: "
          << itr->second;
      is_snippetable_query = false;
    }
    if (!result.snippet().entries().empty()) {
      ++num_snippeted;
      for (const SnippetProto::EntryProto& entry : result.snippet().entries()) {
        ASSERT_THAT(entry.snippet_matches(),
                    SizeIs(Le(num_matches_per_property)));
      }
    }
  }

  std::vector<SearchResultProto::ResultProto> exp_results(
      std::make_move_iterator(expected_search_result.results().begin()),
      std::make_move_iterator(expected_search_result.results().end()));

  if (is_snippetable_query) {
    ASSERT_THAT(num_snippeted,
                Eq(std::min<uint32_t>(exp_results.size(), num_to_snippet)));
  }
  // Update num_to_snippet for the next call. Only num_to_snippet results need
  // to be snippeted, so if we have more than num_to_snippet results, we need to
  // skip checking if the remaining results are snippeted.
  num_to_snippet = std::max<int>(num_to_snippet - num_snippeted, 0);

  SortResults(exp_results);
  SortResults(actual_results);
  ASSERT_THAT(actual_results, SizeIs(exp_results.size()));
  for (int i = 0; i < actual_results.size(); ++i) {
    ASSERT_NO_FATAL_FAILURE(CompareSearchResultProto(
        actual_results[i], exp_results[i], is_projection_enabled));
  }
  ICING_LOG(INFO) << exp_results.size() << " documents found by query.";
}

void IcingMonkeyTestRunner::CheckResults(
    const SearchResultProto& actual_search_result,
    const SearchResultProto& expected_search_result, QueryState& query_state) {
  ICING_LOG(INFO) << "Checking results for query with real icing token: "
                  << query_state.next_page_token;
  CheckResults(actual_search_result, expected_search_result,
               query_state.query_timestamp_ms, query_state.num_to_snippet,
               query_state.num_matches_per_property,
               query_state.is_snippetable_query,
               query_state.is_projection_enabled);
}

}  // namespace lib
}  // namespace icing
