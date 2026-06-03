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
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-numeric-query-node.h"
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
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
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

void GetRandomPropertyRestricts(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    std::unordered_set<std::string>& property_restricts) {
  const SchemaTypeConfigProto& type_config = document_generator->GetType();
  if (type_config.properties_size() > 0) {
    std::uniform_int_distribution<> prop_dist(
        0, type_config.properties_size() - 1);
    property_restricts.insert(
        type_config.properties(prop_dist(*random)).property_name());
  }
}

// A pair of SearchSpecProto and MonkeyAbstractQueryNode that should be
// equivalent.
struct MonkeyQueryPair {
  SearchSpecProto search_spec;
  std::unique_ptr<MonkeyAbstractQueryNode> query_node;
  bool is_numeric = false;
};

std::unique_ptr<MonkeyTermQueryNode> GenerateRandomTermNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec) {
  // 50% chance of getting a property restrict.
  std::unordered_set<std::string> property_restricts;
  if (GetRandomBoolean(random)) {
    GetRandomPropertyRestricts(random, document_generator, property_restricts);
  }

  // Get a random token from the language set as a single term query.
  std::string term = std::string(document_generator->GetToken());
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  if (GetRandomBoolean(random)) {
    term_match_type = TermMatchType::PREFIX;
    // Randomly drop a suffix of query to test prefix query.
    std::uniform_int_distribution<> size_dist(1, term.size());
    term.resize(size_dist(*random));
  }
  // TODO(b/491571627) - Decide on how to support queries with different match
  // types.
  search_spec.set_term_match_type(term_match_type);

  auto query_node = std::make_unique<MonkeyTermQueryNode>(
      term, /*is_prefix=*/false, /*is_verbatim=*/false, term_match_type,
      /*document_namespaces=*/std::vector<std::string>(),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()),
      /*property_restricts=*/
      std::move(property_restricts));
  return query_node;
}

std::unique_ptr<MonkeySemanticQueryNode> GenerateRandomSemanticNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec) {
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

  SearchSpecProto::EmbeddingQueryMetricType::Code metric_type =
      SearchSpecProto::EmbeddingQueryMetricType::COSINE;
  PropertyProto::VectorProto vector =
      document_generator->GetRandomVector(/*allow_quantized_value=*/true);

  search_spec.set_embedding_query_metric_type(metric_type);

  *search_spec.add_embedding_query_vectors() = vector;

  // 0 means ANN is not enabled. >0 means ANN is enabled and still includes
  // the linear search part. The in-memory Icing search engine cannot truly
  // verify the behavior of the ANN index algorithmically, as IVF-based ANN
  // is an approximate non-deterministic mapping. As a result, we set a wildly
  // large nprobe to let ANN degenerate into linear search, granting exact
  // matching for verification.
  int nprobe = GetRandomBoolean(random) ? 0 : 100000000;
  search_spec.set_embedding_query_nprobe(nprobe);

  // TODO(b/491571627) - Add support for multiple embedding query vectors.
  auto query_node = std::make_unique<MonkeySemanticQueryNode>(
      /*vector_index=*/0, low, high, metric_type, nprobe, std::move(vector),
      /*property_restricts=*/std::move(property_restricts),
      /*document_namespaces=*/std::vector<std::string>(),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  return query_node;
}

std::unique_ptr<MonkeyAbstractLeafQueryNode> GenerateRandomNumericNode(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    SearchSpecProto& search_spec) {
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
      /*document_namespaces=*/std::vector<std::string>(),
      /*document_schema_types=*/
      std::vector<std::string>(search_spec.schema_type_filters().begin(),
                               search_spec.schema_type_filters().end()));
  search_spec.add_enabled_features(std::string(kNumericSearchFeature));
  return query_node;
}

std::vector<std::function<std::unique_ptr<MonkeyAbstractLeafQueryNode>(
    MonkeyTestRandomEngine*, MonkeyDocumentGenerator*, SearchSpecProto&)>>
    leaf_node_generators = {GenerateRandomTermNode, GenerateRandomSemanticNode,
                            GenerateRandomNumericNode};

// Generates a random query tree with the given depth.
// As a part of generating the query tree, the some fields in the
// SearchSpecProto will also be written and read to (depending on the type of
// query generated).
libtextclassifier3::StatusOr<std::unique_ptr<MonkeyAbstractQueryNode>>
GenerateRandomQueryTree(MonkeyTestRandomEngine* random,
                        MonkeyDocumentGenerator* document_generator,
                        SearchSpecProto& search_spec, int depth) {
  if (depth <= 0) {
    return absl_ports::InvalidArgumentError("Depth must be positive.");
  }
  // Generate a random leaf node.
  if (depth == 1) {
    int leaf_node_generator_index =
        GetRandomInt(random, 0, leaf_node_generators.size() - 1);
    return leaf_node_generators[leaf_node_generator_index](
        random, document_generator, search_spec);
  } else {
    // TODO(b/491571627): Handle cases where depth > 1 i.e. we have nodes with
    // children.
    return absl_ports::UnimplementedError(
        "Depth > 1 not implemented yet.");  // Not implemented yet.
  }
}

libtextclassifier3::StatusOr<MonkeyQueryPair> GenerateRandomMonkeyQueryPair(
    MonkeyTestRandomEngine* random, MonkeyDocumentGenerator* document_generator,
    int depth = 1) {
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

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<MonkeyAbstractQueryNode> query_node,
      GenerateRandomQueryTree(random, document_generator, search_spec, depth));

  search_spec.set_query(query_node->GenerateQueryString());
  bool is_numeric =
      dynamic_cast<MonkeyNumericQueryNode*>(query_node.get()) != nullptr;
  return MonkeyQueryPair{.search_spec = std::move(search_spec),
                         .query_node = std::move(query_node),
                         .is_numeric = is_numeric};
}

ScoringSpecProto GenerateRandomScoringSpec(MonkeyTestRandomEngine* random) {
  ScoringSpecProto scoring_spec;

  constexpr std::array<ScoringSpecProto::RankingStrategy::Code, 3>
      ranking_strategies = {
          ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE,
          ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP,
          ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE};

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
                                              const SchemaProto* schema) {
  std::uniform_int_distribution<> dist(0, 4);
  ResultSpecProto result_spec;
  // 1/5 chance of getting one of 1, 4, 16, 64, 256
  int num_per_page = 1 << (2 * dist(*random));
  result_spec.set_num_per_page(num_per_page);
  result_spec.set_num_to_score(std::numeric_limits<int32_t>::max());
  *result_spec.mutable_snippet_spec() =
      GenerateRandomSnippetSpecProto(random, result_spec);

  // 1/5 chance of enabling projection.
  if (dist(*random) == 0) {
    for (const SchemaTypeConfigProto& type_config : schema->types()) {
      // 25% chance of adding the current type to the projection.
      std::uniform_int_distribution<> dist(0, 3);
      if (dist(*random) == 0) {
        *result_spec.add_type_property_masks() =
            GenerateTypePropertyMask(random, type_config);
      }
    }
  }
  return result_spec;
}

void SortDocuments(std::vector<DocumentProto>& documents) {
  std::sort(documents.begin(), documents.end(),
            [](const DocumentProto& doc1, const DocumentProto& doc2) {
              if (doc1.namespace_() != doc2.namespace_()) {
                return doc1.namespace_() < doc2.namespace_();
              }
              return doc1.uri() < doc2.uri();
            });
}

}  // namespace

IcingMonkeyTestRunner::IcingMonkeyTestRunner(
    IcingMonkeyTestRunnerConfiguration config)
    : config_(std::move(config)),
      random_(config_.seed),
      in_memory_icing_(std::make_unique<InMemoryIcingSearchEngine>(&random_)),
      schema_generator_(
          std::make_unique<MonkeySchemaGenerator>(&random_, &config_)) {
  ICING_LOG(INFO) << "Monkey test runner started with seed: " << config_.seed;
  icing_dir_ = GetTestTempDir() + "/icing/monkey";
}

void IcingMonkeyTestRunner::Run(uint32_t num) {
  ASSERT_TRUE(icing_ != nullptr)
      << "Icing search engine has not yet been created. Please call "
         "Initialize() first";

  uint32_t frequency_sum = 0;
  for (const auto& schedule : config_.monkey_api_schedules) {
    frequency_sum += schedule.second;
  }
  std::uniform_int_distribution<> dist(0, frequency_sum - 1);
  for (uint32_t i = 0; i < num; ++i) {
    int p = dist(random_);
    for (const auto& schedule : config_.monkey_api_schedules) {
      if (p < schedule.second) {
        ASSERT_NO_FATAL_FAILURE(schedule.first(this));
        break;
      }
      p -= schedule.second;
    }
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

  // Update in-memory icing
  for (const std::string& deleted_type : result.schema_types_deleted) {
    ICING_ASSERT_OK(in_memory_icing_->DeleteBySchemaType(deleted_type));
  }
  for (const std::string& incompatible_type :
       result.schema_types_incompatible) {
    ICING_ASSERT_OK(in_memory_icing_->DeleteBySchemaType(incompatible_type));
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
  in_memory_icing_->Put(doc);
  ASSERT_THAT(icing_->Put(doc.document).status(), ProtoIsOk());
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
    ICING_ASSERT_OK(
        in_memory_icing_->Delete(document.name_space, document.uri));
    ASSERT_THAT(delete_result.status(), ProtoIsOk())
        << "Cannot delete an existing document.";
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
  ICING_ASSERT_OK_AND_ASSIGN(
      MonkeyQueryPair query_pair,
      GenerateRandomMonkeyQueryPair(&random_, document_generator_.get()));
  SearchSpecProto search_spec = query_pair.search_spec;
  ICING_LOG(INFO) << "Monkey deleting by query: " << search_spec.query();
  DeleteByQueryResultProto delete_result = icing_->DeleteByQuery(search_spec);
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t num_docs_deleted,
      in_memory_icing_->DeleteByQuery(query_pair.query_node.get()));
  if (num_docs_deleted != 0) {
    ASSERT_THAT(delete_result.status(), ProtoIsOk())
        << "Cannot delete documents that matches with the query.";
    ASSERT_THAT(delete_result.delete_by_query_stats().num_documents_deleted(),
                Eq(num_docs_deleted));
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
  ICING_ASSERT_OK_AND_ASSIGN(
      MonkeyQueryPair query_pair,
      GenerateRandomMonkeyQueryPair(&random_, document_generator_.get()));
  std::unique_ptr<SearchSpecProto> search_spec =
      std::make_unique<SearchSpecProto>(query_pair.search_spec);
  std::unique_ptr<ScoringSpecProto> scoring_spec =
      std::make_unique<ScoringSpecProto>(GenerateRandomScoringSpec(&random_));
  std::unique_ptr<ResultSpecProto> result_spec =
      std::make_unique<ResultSpecProto>(GenerateRandomResultSpecProto(
          &random_, in_memory_icing_->GetSchema()));
  const ResultSpecProto::SnippetSpecProto snippet_spec =
      result_spec->snippet_spec();
  bool is_projection_enabled = !result_spec->type_property_masks().empty();
  bool is_embedding_query = !search_spec->embedding_query_vectors().empty();

  ICING_LOG(INFO) << "Monkey searching by query: " << search_spec->query()
                  << ", term_match_type: " << search_spec->term_match_type()
                  << ", nprobe: " << search_spec->embedding_query_nprobe();
  ICING_VLOG(1) << "search_spec:\n" << search_spec->DebugString();
  ICING_VLOG(1) << "scoring_spec:\n" << scoring_spec->DebugString();
  ICING_VLOG(1) << "result_spec:\n" << result_spec->DebugString();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<DocumentProto> exp_documents,
      in_memory_icing_->Search(query_pair.query_node.get()));

  SearchResultProto search_result =
      icing_->Search(*search_spec, *scoring_spec, *result_spec);
  ASSERT_THAT(search_result.status(), ProtoIsOk());

  // Delete all of the specs used in the search. GetNextPage should have no
  // problem because it shouldn't be keeping any references to them.
  search_spec.reset();
  scoring_spec.reset();
  result_spec.reset();

  std::vector<DocumentProto> actual_documents;
  int num_snippeted = 0;
  while (true) {
    for (const SearchResultProto::ResultProto& doc : search_result.results()) {
      actual_documents.push_back(doc.document());
      if (!doc.snippet().entries().empty()) {
        ++num_snippeted;
        for (const SnippetProto::EntryProto& entry : doc.snippet().entries()) {
          ASSERT_THAT(entry.snippet_matches(),
                      SizeIs(Le(snippet_spec.num_matches_per_property())));
        }
      }
    }
    if (search_result.next_page_token() == kInvalidNextPageToken) {
      break;
    }
    search_result = icing_->GetNextPage(search_result.next_page_token());
    ASSERT_THAT(search_result.status(), ProtoIsOk());
  }
  if (snippet_spec.num_matches_per_property() > 0 && !is_projection_enabled &&
      !is_embedding_query && !query_pair.is_numeric) {
    ASSERT_THAT(num_snippeted,
                Eq(std::min<uint32_t>(exp_documents.size(),
                                      snippet_spec.num_to_snippet())));
  }
  SortDocuments(exp_documents);
  SortDocuments(actual_documents);
  ASSERT_THAT(actual_documents, SizeIs(exp_documents.size()));
  for (int i = 0; i < exp_documents.size(); ++i) {
    if (is_projection_enabled) {
      ASSERT_THAT(actual_documents[i].namespace_(),
                  Eq(exp_documents[i].namespace_()));
      ASSERT_THAT(actual_documents[i].uri(), Eq(exp_documents[i].uri()));
      continue;
    }
    ASSERT_THAT(actual_documents[i], EqualsProto(exp_documents[i]));
  }
  ICING_LOG(INFO) << exp_documents.size() << " documents found by query.";
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
  icing_options.set_enable_schema_definition_deduping(true);
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

}  // namespace lib
}  // namespace icing
