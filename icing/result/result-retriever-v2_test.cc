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

#include "icing/result/result-retriever-v2.h"

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/mock-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/result/page-result.h"
#include "icing/result/result-state-v2.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::DoDefault;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::IsEmpty;
using ::testing::Pointee;
using ::testing::Return;
using ::testing::SizeIs;
using NamespaceIdMap = std::unordered_map<NamespaceId, int>;

constexpr PropertyConfigProto::Cardinality::Code CARDINALITY_OPTIONAL =
    PropertyConfigProto::Cardinality::OPTIONAL;

constexpr StringIndexingConfig::TokenizerType::Code TOKENIZER_PLAIN =
    StringIndexingConfig::TokenizerType::PLAIN;

constexpr TermMatchType::Code MATCH_EXACT = TermMatchType::EXACT_ONLY;
constexpr TermMatchType::Code MATCH_PREFIX = TermMatchType::PREFIX;

// Mock the behavior of GroupResultLimiter::ShouldBeRemoved.
class MockGroupResultLimiter : public GroupResultLimiterV2 {
 public:
  MockGroupResultLimiter() : GroupResultLimiterV2() {
    ON_CALL(*this, ShouldBeRemoved).WillByDefault(Return(false));
  }

  MOCK_METHOD(bool, ShouldBeRemoved,
              (const ScoredDocumentHit&, const NamespaceIdMap&,
               const DocumentStore&, std::vector<int>&),
              (const, override));
};

class ResultRetrieverV2Test : public ::testing::Test {
 protected:
  ResultRetrieverV2Test() : test_dir_(GetTestTempDir() + "/icing") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
  }

  void SetUp() override {
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpICUDataFile(
              GetTestFilePath("icing/icu.dat")));
    }
    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/10000));

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder()
                         .SetType("Email")
                         .AddProperty(PropertyConfigBuilder()
                                          .SetName("name")
                                          .SetDataTypeString(MATCH_PREFIX,
                                                             TOKENIZER_PLAIN)
                                          .SetCardinality(CARDINALITY_OPTIONAL))
                         .AddProperty(PropertyConfigBuilder()
                                          .SetName("body")
                                          .SetDataTypeString(MATCH_EXACT,
                                                             TOKENIZER_PLAIN)
                                          .SetCardinality(CARDINALITY_OPTIONAL))
                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName("sender")
                                 .SetDataTypeDocument(
                                     "Person", /*index_nested_properties=*/true)
                                 .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Person")
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("name")
                            .SetDataTypeString(MATCH_PREFIX, TOKENIZER_PLAIN)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("emailAddress")
                            .SetDataTypeString(MATCH_PREFIX, TOKENIZER_PLAIN)
                            .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

    num_total_hits_ = 0;
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  SectionId GetSectionId(const std::string& type, const std::string& property) {
    auto type_id_or = schema_store_->GetSchemaTypeId(type);
    if (!type_id_or.ok()) {
      return kInvalidSectionId;
    }
    SchemaTypeId type_id = type_id_or.ValueOrDie();
    for (SectionId section_id = 0; section_id <= kMaxSectionId; ++section_id) {
      auto metadata_or = schema_store_->GetSectionMetadata(type_id, section_id);
      if (!metadata_or.ok()) {
        break;
      }
      const SectionMetadata* metadata = metadata_or.ValueOrDie();
      if (metadata->path == property) {
        return metadata->id;
      }
    }
    return kInvalidSectionId;
  }

  const Filesystem filesystem_;
  const std::string test_dir_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<Normalizer> normalizer_;
  std::atomic<int> num_total_hits_;
  FakeClock fake_clock_;
};

// TODO(sungyc): Refactor helper functions below (builder classes or common test
//               utility).

DocumentProto CreateDocument(int id) {
  return DocumentBuilder()
      .SetKey("icing", "Email/" + std::to_string(id))
      .SetSchema("Email")
      .AddStringProperty("name", "subject foo " + std::to_string(id))
      .AddStringProperty("body", "body bar " + std::to_string(id))
      .SetCreationTimestampMs(1574365086666 + id)
      .Build();
}

SectionIdMask CreateSectionIdMask(const std::vector<SectionId>& section_ids) {
  SectionIdMask mask = 0;
  for (SectionId section_id : section_ids) {
    mask |= (UINT64_C(1) << section_id);
  }
  return mask;
}

SearchSpecProto CreateSearchSpec(TermMatchType::Code match_type) {
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(match_type);
  return search_spec;
}

ScoringSpecProto CreateScoringSpec(bool is_descending_order) {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_order_by(is_descending_order ? ScoringSpecProto::Order::DESC
                                                : ScoringSpecProto::Order::ASC);
  return scoring_spec;
}

ResultSpecProto CreateResultSpec(int num_per_page) {
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
}

TEST_F(ResultRetrieverV2Test, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      ResultRetrieverV2::Create(/*doc_store=*/nullptr, schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  EXPECT_THAT(
      ResultRetrieverV2::Create(doc_store.get(), /*schema_store=*/nullptr,
                                language_segmenter_.get(), normalizer_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                        /*language_segmenter=*/nullptr,
                                        normalizer_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                        language_segmenter_.get(),
                                        /*normalizer=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(ResultRetrieverV2Test, ShouldRetrieveSimpleResults) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             doc_store->Put(CreateDocument(/*id=*/4)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5,
                             doc_store->Put(CreateDocument(/*id=*/5)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/19},
      {document_id2, hit_section_id_mask, /*score=*/12},
      {document_id3, hit_section_id_mask, /*score=*/8},
      {document_id4, hit_section_id_mask, /*score=*/3},
      {document_id5, hit_section_id_mask, /*score=*/1}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  result1.set_score(19);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);
  result2.set_score(12);
  SearchResultProto::ResultProto result3;
  *result3.mutable_document() = CreateDocument(/*id=*/3);
  result3.set_score(8);
  SearchResultProto::ResultProto result4;
  *result4.mutable_document() = CreateDocument(/*id=*/4);
  result4.set_score(3);
  SearchResultProto::ResultProto result5;
  *result5.mutable_document() = CreateDocument(/*id=*/5);
  result5.set_score(1);

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/2), *doc_store);

  // First page, 2 results
  auto [page_result1, has_more_results1] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result1.results,
              ElementsAre(EqualsProto(result1), EqualsProto(result2)));
  // num_results_with_snippets is 0 when there is no snippet.
  EXPECT_THAT(page_result1.num_results_with_snippets, Eq(0));
  // Requested page size is same as num_per_page.
  EXPECT_THAT(page_result1.requested_page_size, Eq(2));
  // Has more results.
  EXPECT_TRUE(has_more_results1);

  // Second page, 2 results
  auto [page_result2, has_more_results2] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result2.results,
              ElementsAre(EqualsProto(result3), EqualsProto(result4)));
  // num_results_with_snippets is 0 when there is no snippet.
  EXPECT_THAT(page_result2.num_results_with_snippets, Eq(0));
  // Requested page size is same as num_per_page.
  EXPECT_THAT(page_result2.requested_page_size, Eq(2));
  // Has more results.
  EXPECT_TRUE(has_more_results2);

  // Third page, 1 result
  auto [page_result3, has_more_results3] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result3.results, ElementsAre(EqualsProto(result5)));
  // num_results_with_snippets is 0 when there is no snippet.
  EXPECT_THAT(page_result3.num_results_with_snippets, Eq(0));
  // Requested page size is same as num_per_page.
  EXPECT_THAT(page_result3.requested_page_size, Eq(2));
  // No more results.
  EXPECT_FALSE(has_more_results3);
}

TEST_F(ResultRetrieverV2Test, ShouldIgnoreNonInternalErrors) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  DocumentId invalid_document_id = -1;
  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/12},
      {document_id2, hit_section_id_mask, /*score=*/4},
      {invalid_document_id, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get(),
                                std::make_unique<MockGroupResultLimiter>()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  result1.set_score(12);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);
  result2.set_score(4);

  ResultStateV2 result_state1(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/3), *doc_store);
  PageResult page_result1 =
      result_retriever->RetrieveNextPage(result_state1).first;
  EXPECT_THAT(page_result1.results,
              ElementsAre(EqualsProto(result1), EqualsProto(result2)));

  DocumentId non_existing_document_id = 4;
  scored_document_hits = {
      {non_existing_document_id, hit_section_id_mask, /*score=*/15},
      {document_id1, hit_section_id_mask, /*score=*/12},
      {document_id2, hit_section_id_mask, /*score=*/4}};
  ResultStateV2 result_state2(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/3), *doc_store);
  PageResult page_result2 =
      result_retriever->RetrieveNextPage(result_state2).first;
  EXPECT_THAT(page_result2.results,
              ElementsAre(EqualsProto(result1), EqualsProto(result2)));
}

TEST_F(ResultRetrieverV2Test, ShouldIgnoreInternalErrors) {
  MockFilesystem mock_filesystem;
  EXPECT_CALL(mock_filesystem,
              PRead(A<int>(), A<void*>(), A<size_t>(), A<off_t>()))
      .WillOnce(Return(false))
      .WillRepeatedly(DoDefault());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&mock_filesystem, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0}};

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get(),
                                std::make_unique<MockGroupResultLimiter>()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  result1.set_score(0);

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/2), *doc_store);
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  // We mocked mock_filesystem to return an internal error when retrieving doc2,
  // so doc2 should be skipped and doc1 should still be returned.
  EXPECT_THAT(page_result.results, ElementsAre(EqualsProto(result1)));
}

TEST_F(ResultRetrieverV2Test, ShouldUpdateResultState) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             doc_store->Put(CreateDocument(/*id=*/4)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5,
                             doc_store->Put(CreateDocument(/*id=*/5)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0},
      {document_id4, hit_section_id_mask, /*score=*/0},
      {document_id5, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/2), *doc_store);

  // First page, 2 results
  PageResult page_result1 =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result1.results, SizeIs(2));
  {
    absl_ports::shared_lock l(&result_state.mutex);

    // num_returned = size of first page
    EXPECT_THAT(result_state.num_returned, Eq(2));
    // Should remove the 2 returned docs from scored_document_hits and only
    // contain the remaining 3.
    EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(SizeIs(3)));
  }

  // Second page, 2 results
  PageResult page_result2 =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result2.results, SizeIs(2));
  {
    absl_ports::shared_lock l(&result_state.mutex);

    // num_returned = size of first and second pages
    EXPECT_THAT(result_state.num_returned, Eq(4));
    // Should remove the 2 returned docs from scored_document_hits and only
    // contain the remaining 1.
    EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(SizeIs(1)));
  }

  // Third page, 1 result
  PageResult page_result3 =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result3.results, SizeIs(1));
  {
    absl_ports::shared_lock l(&result_state.mutex);

    // num_returned = size of first, second and third pages
    EXPECT_THAT(result_state.num_returned, Eq(5));
    // Should remove the 1 returned doc from scored_document_hits and become
    // empty.
    EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(IsEmpty()));
  }
}

TEST_F(ResultRetrieverV2Test, ShouldUpdateNumTotalHits) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  std::vector<ScoredDocumentHit> scored_document_hits1 = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0}};
  std::shared_ptr<ResultStateV2> result_state1 =
      std::make_shared<ResultStateV2>(
          std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
              std::move(scored_document_hits1),
              /*is_descending=*/true),
          /*query_terms=*/SectionRestrictQueryTermsMap{},
          CreateSearchSpec(TermMatchType::EXACT_ONLY),
          CreateScoringSpec(/*is_descending_order=*/true),
          CreateResultSpec(/*num_per_page=*/1), *doc_store);
  {
    absl_ports::unique_lock l(&result_state1->mutex);

    result_state1->RegisterNumTotalHits(&num_total_hits_);
    ASSERT_THAT(num_total_hits_, Eq(2));
  }

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             doc_store->Put(CreateDocument(/*id=*/4)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5,
                             doc_store->Put(CreateDocument(/*id=*/5)));
  std::vector<ScoredDocumentHit> scored_document_hits2 = {
      {document_id3, hit_section_id_mask, /*score=*/0},
      {document_id4, hit_section_id_mask, /*score=*/0},
      {document_id5, hit_section_id_mask, /*score=*/0}};
  std::shared_ptr<ResultStateV2> result_state2 =
      std::make_shared<ResultStateV2>(
          std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
              std::move(scored_document_hits2),
              /*is_descending=*/true),
          /*query_terms=*/SectionRestrictQueryTermsMap{},
          CreateSearchSpec(TermMatchType::EXACT_ONLY),
          CreateScoringSpec(/*is_descending_order=*/true),
          CreateResultSpec(/*num_per_page=*/2), *doc_store);
  {
    absl_ports::unique_lock l(&result_state2->mutex);

    result_state2->RegisterNumTotalHits(&num_total_hits_);
    ASSERT_THAT(num_total_hits_, Eq(5));
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Should get 1 doc in the first page of result_state1, and num_total_hits
  // should be decremented by 1.
  PageResult page_result1 =
      result_retriever->RetrieveNextPage(*result_state1).first;
  ASSERT_THAT(page_result1.results, SizeIs(1));
  EXPECT_THAT(num_total_hits_, Eq(4));

  // Should get 2 docs in the first page of result_state2, and num_total_hits
  // should be decremented by 2.
  PageResult page_result2 =
      result_retriever->RetrieveNextPage(*result_state2).first;
  ASSERT_THAT(page_result2.results, SizeIs(2));
  EXPECT_THAT(num_total_hits_, Eq(2));

  // Should get 1 doc in the second page of result_state2 (although num_per_page
  // is 2, there is only 1 doc left), and num_total_hits should be decremented
  // by 1.
  PageResult page_result3 =
      result_retriever->RetrieveNextPage(*result_state2).first;
  ASSERT_THAT(page_result3.results, SizeIs(1));
  EXPECT_THAT(num_total_hits_, Eq(1));

  // Destruct result_state1. There is 1 doc left, so num_total_hits should be
  // decremented by 1 when destructing it.
  result_state1.reset();
  EXPECT_THAT(num_total_hits_, Eq(0));

  // Destruct result_state2. There is 0 doc left, so num_total_hits should be
  // unchanged when destructing it.
  result_state1.reset();
  EXPECT_THAT(num_total_hits_, Eq(0));
}

TEST_F(ResultRetrieverV2Test, ShouldLimitNumTotalBytesPerPage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/5},
      {document_id2, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  result1.set_score(5);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);
  result2.set_score(0);

  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  result_spec.set_num_total_bytes_per_page_threshold(result1.ByteSizeLong());
  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec, *doc_store);

  // First page. Only result1 should be returned, since its byte size meets
  // num_total_bytes_per_page_threshold and ResultRetriever should terminate
  // early even though # of results is still below num_per_page.
  auto [page_result1, has_more_results1] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result1.results, ElementsAre(EqualsProto(result1)));
  // Has more results.
  EXPECT_TRUE(has_more_results1);

  // Second page, result2.
  auto [page_result2, has_more_results2] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result2.results, ElementsAre(EqualsProto(result2)));
  // No more results.
  EXPECT_FALSE(has_more_results2);
}

TEST_F(ResultRetrieverV2Test,
       ShouldReturnSingleLargeResultAboveNumTotalBytesPerPageThreshold) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/5},
      {document_id2, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  result1.set_score(5);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);
  result2.set_score(0);

  int threshold = 1;
  ASSERT_THAT(result1.ByteSizeLong(), Gt(threshold));

  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  result_spec.set_num_total_bytes_per_page_threshold(threshold);
  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec, *doc_store);

  // First page. Should return single result1 even though its byte size exceeds
  // num_total_bytes_per_page_threshold.
  auto [page_result1, has_more_results1] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result1.results, ElementsAre(EqualsProto(result1)));
  // Has more results.
  EXPECT_TRUE(has_more_results1);

  // Second page, result2.
  auto [page_result2, has_more_results2] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result2.results, ElementsAre(EqualsProto(result2)));
  // No more results.
  EXPECT_FALSE(has_more_results2);
}

TEST_F(ResultRetrieverV2Test,
       ShouldRetrieveNextResultWhenBelowNumTotalBytesPerPageThreshold) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  std::unique_ptr<DocumentStore> doc_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/5},
      {document_id2, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(doc_store.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  result1.set_score(5);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);
  result2.set_score(0);

  int threshold = result1.ByteSizeLong() + 1;
  ASSERT_THAT(result1.ByteSizeLong() + result2.ByteSizeLong(), Gt(threshold));

  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  result_spec.set_num_total_bytes_per_page_threshold(threshold);
  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec, *doc_store);

  // After retrieving result1, total bytes are still below the threshold and #
  // of results is still below num_per_page, so ResultRetriever should continue
  // the retrieval process and thus include result2 into this page, even though
  // finally total bytes of result1 + result2 exceed the threshold.
  auto [page_result, has_more_results] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result.results,
              ElementsAre(EqualsProto(result1), EqualsProto(result2)));
  // No more results.
  EXPECT_FALSE(has_more_results);
}

}  // namespace

}  // namespace lib
}  // namespace icing
