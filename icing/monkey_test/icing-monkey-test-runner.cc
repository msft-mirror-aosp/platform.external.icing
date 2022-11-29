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
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-generators.h"
#include "icing/portable/equals-proto.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::Le;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAreArray;

inline constexpr int kNumTypes = 30;
const std::vector<int> kPossibleNumProperties = {0,
                                                 1,
                                                 2,
                                                 4,
                                                 8,
                                                 16,
                                                 kTotalNumSections / 2,
                                                 kTotalNumSections,
                                                 kTotalNumSections + 1,
                                                 kTotalNumSections * 2};
inline constexpr int kNumNamespaces = 100;
inline constexpr int kNumURIs = 1000;

// Merge per 131072 hits
const int kIndexMergeSize = 1024 * 1024;

// An array of pairs of monkey test APIs with frequencies.
// If f_sum is the sum of all the frequencies, an operation with frequency f
// means for every f_sum iterations, the operation is expected to run f times.
const std::vector<
    std::pair<std::function<void(IcingMonkeyTestRunner*)>, uint32_t>>
    kMonkeyAPISchedules = {{&IcingMonkeyTestRunner::DoPut, 500},
                           {&IcingMonkeyTestRunner::DoSearch, 200},
                           {&IcingMonkeyTestRunner::DoGet, 70},
                           {&IcingMonkeyTestRunner::DoGetAllNamespaces, 50},
                           {&IcingMonkeyTestRunner::DoDelete, 50},
                           {&IcingMonkeyTestRunner::DoDeleteByNamespace, 50},
                           {&IcingMonkeyTestRunner::DoDeleteBySchemaType, 50},
                           {&IcingMonkeyTestRunner::DoDeleteByQuery, 20},
                           {&IcingMonkeyTestRunner::DoOptimize, 5},
                           {&IcingMonkeyTestRunner::ReloadFromDisk, 5}};

SchemaProto GenerateRandomSchema(MonkeyTestRandomEngine* random) {
  MonkeySchemaGenerator schema_generator(random);
  return schema_generator.GenerateSchema(kNumTypes, kPossibleNumProperties);
}

SearchSpecProto GenerateRandomSearchSpecProto(
    MonkeyTestRandomEngine* random,
    MonkeyDocumentGenerator* document_generator) {
  // Get a random token from the language set as a single term query.
  std::string query(document_generator->GetToken());
  std::uniform_int_distribution<> dist(0, 1);
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  if (dist(*random) == 1) {
    term_match_type = TermMatchType::PREFIX;
    // Randomly drop a suffix of query to test prefix query.
    std::uniform_int_distribution<> size_dist(1, query.size());
    query.resize(size_dist(*random));
  }
  // 50% chance of getting a section restriction.
  if (dist(*random) == 1) {
    const SchemaTypeConfigProto& type_config = document_generator->GetType();
    if (type_config.properties_size() > 0) {
      std::uniform_int_distribution<> prop_dist(
          0, type_config.properties_size() - 1);
      query = absl_ports::StrCat(
          type_config.properties(prop_dist(*random)).property_name(), ":",
          query);
    }
  }
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_query(query);
  return search_spec;
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
  return snippet_spec;
}

ResultSpecProto GenerateRandomResultSpecProto(MonkeyTestRandomEngine* random) {
  std::uniform_int_distribution<> dist(0, 4);
  ResultSpecProto result_spec;
  // 1/5 chance of getting one of 1, 4, 16, 64, 256
  int num_per_page = 1 << (2 * dist(*random));
  result_spec.set_num_per_page(num_per_page);
  *result_spec.mutable_snippet_spec() =
      GenerateRandomSnippetSpecProto(random, result_spec);
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

IcingMonkeyTestRunner::IcingMonkeyTestRunner(uint32_t seed)
    : random_(seed), in_memory_icing_() {
  ICING_LOG(INFO) << "Monkey test runner started with seed: " << seed;

  SchemaProto schema = GenerateRandomSchema(&random_);
  ICING_LOG(DBG) << "Schema Generated: " << schema.DebugString();

  in_memory_icing_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_, std::move(schema));

  document_generator_ = std::make_unique<MonkeyDocumentGenerator>(
      &random_, in_memory_icing_->GetSchema(), kNumNamespaces, kNumURIs);

  std::string dir = GetTestTempDir() + "/icing/monkey";
  filesystem_.DeleteDirectoryRecursively(dir.c_str());
  icing_dir_ = std::make_unique<DestructibleDirectory>(&filesystem_, dir);
}

void IcingMonkeyTestRunner::Run(uint32_t num) {
  ASSERT_TRUE(icing_ != nullptr)
      << "Icing search engine has not yet been created. Please call "
         "CreateIcingSearchEngineWithSchema() first";

  uint32_t frequency_sum = 0;
  for (const auto& schedule : kMonkeyAPISchedules) {
    frequency_sum += schedule.second;
  }
  std::uniform_int_distribution<> dist(0, frequency_sum - 1);
  for (; num; --num) {
    int p = dist(random_);
    for (const auto& schedule : kMonkeyAPISchedules) {
      if (p < schedule.second) {
        ASSERT_NO_FATAL_FAILURE(schedule.first(this));
        break;
      }
      p -= schedule.second;
    }
    ICING_LOG(INFO) << "Documents in the in-memory icing: "
                    << in_memory_icing_->GetNumAliveDocuments();
  }
}

void IcingMonkeyTestRunner::CreateIcingSearchEngineWithSchema() {
  ASSERT_NO_FATAL_FAILURE(CreateIcingSearchEngine());
  ASSERT_THAT(icing_->SetSchema(*in_memory_icing_->GetSchema()).status(),
              ProtoIsOk());
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
  in_memory_icing_->Delete(document.name_space, document.uri);
  DeleteResultProto delete_result =
      icing_->Delete(document.name_space, document.uri);
  if (document.document.has_value()) {
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
  SearchSpecProto search_spec =
      GenerateRandomSearchSpecProto(&random_, document_generator_.get());
  ICING_LOG(INFO) << "Monkey deleting by query: " << search_spec.query();
  DeleteByQueryResultProto delete_result = icing_->DeleteByQuery(search_spec);
  ICING_ASSERT_OK_AND_ASSIGN(uint32_t num_docs_deleted,
                             in_memory_icing_->DeleteByQuery(search_spec));
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
  SearchSpecProto search_spec =
      GenerateRandomSearchSpecProto(&random_, document_generator_.get());
  ScoringSpecProto scoring_spec = GenerateRandomScoringSpec(&random_);
  ResultSpecProto result_spec = GenerateRandomResultSpecProto(&random_);
  const ResultSpecProto::SnippetSpecProto& snippet_spec =
      result_spec.snippet_spec();

  ICING_LOG(INFO) << "Monkey searching by query: " << search_spec.query()
                  << ", term_match_type: " << search_spec.term_match_type();
  ICING_VLOG(1) << "search_spec:\n" << search_spec.DebugString();
  ICING_VLOG(1) << "scoring_spec:\n" << scoring_spec.DebugString();
  ICING_VLOG(1) << "result_spec:\n" << result_spec.DebugString();

  std::vector<DocumentProto> exp_documents =
      in_memory_icing_->Search(search_spec);

  SearchResultProto search_result =
      icing_->Search(search_spec, scoring_spec, result_spec);
  ASSERT_THAT(search_result.status(), ProtoIsOk());

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
  if (snippet_spec.num_matches_per_property() > 0) {
    ASSERT_THAT(num_snippeted,
                Eq(std::min<uint32_t>(exp_documents.size(),
                                      snippet_spec.num_to_snippet())));
  }
  SortDocuments(exp_documents);
  SortDocuments(actual_documents);
  ASSERT_THAT(actual_documents, SizeIs(exp_documents.size()));
  for (int i = 0; i < exp_documents.size(); ++i) {
    ASSERT_THAT(actual_documents[i], EqualsProto(exp_documents[i]));
  }
  ICING_LOG(INFO) << exp_documents.size() << " documents found by query.";
}

void IcingMonkeyTestRunner::ReloadFromDisk() {
  ICING_LOG(INFO) << "Monkey reloading from disk";
  // Destruct the icing search engine by resetting the unique pointer.
  icing_.reset();
  ASSERT_NO_FATAL_FAILURE(CreateIcingSearchEngine());
}

void IcingMonkeyTestRunner::DoOptimize() {
  ICING_LOG(INFO) << "Monkey doing optimization";
  ASSERT_THAT(icing_->Optimize().status(), ProtoIsOk());
}

void IcingMonkeyTestRunner::CreateIcingSearchEngine() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_index_merge_size(kIndexMergeSize);
  icing_options.set_base_dir(icing_dir_->dir());
  icing_ = std::make_unique<IcingSearchEngine>(icing_options);
  ASSERT_THAT(icing_->Initialize().status(), ProtoIsOk());
}

}  // namespace lib
}  // namespace icing
