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

#include <limits>
#include <memory>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/result/page-result.h"
#include "icing/result/result-retriever-v2.h"
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
#include "icing/testing/snippet-helpers.h"
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
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::SizeIs;

class ResultRetrieverV2SnippetTest : public testing::Test {
 protected:
  ResultRetrieverV2SnippetTest() : test_dir_(GetTestTempDir() + "/icing") {
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
                                          .SetDataTypeString(TERM_MATCH_PREFIX,
                                                             TOKENIZER_PLAIN)
                                          .SetCardinality(CARDINALITY_OPTIONAL))
                         .AddProperty(PropertyConfigBuilder()
                                          .SetName("body")
                                          .SetDataTypeString(TERM_MATCH_EXACT,
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
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("name")
                                     .SetDataTypeString(TERM_MATCH_PREFIX,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("emailAddress")
                                     .SetDataTypeString(TERM_MATCH_PREFIX,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                              schema_store_.get()));
    document_store_ = std::move(create_result.document_store);
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
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
};

// TODO(sungyc): Refactor helper functions below (builder classes or common test
//               utility).

ResultSpecProto::SnippetSpecProto CreateSnippetSpec() {
  ResultSpecProto::SnippetSpecProto snippet_spec;
  snippet_spec.set_num_to_snippet(std::numeric_limits<int>::max());
  snippet_spec.set_num_matches_per_property(std::numeric_limits<int>::max());
  snippet_spec.set_max_window_utf32_length(1024);
  return snippet_spec;
}

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

TEST_F(ResultRetrieverV2SnippetTest,
       DefaultSnippetSpecShouldDisableSnippeting) {
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(CreateDocument(/*id=*/3)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/3), *document_store_);
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.results.at(0).snippet(),
              EqualsProto(SnippetProto::default_instance()));
  EXPECT_THAT(page_result.results.at(1).snippet(),
              EqualsProto(SnippetProto::default_instance()));
  EXPECT_THAT(page_result.results.at(2).snippet(),
              EqualsProto(SnippetProto::default_instance()));
  EXPECT_THAT(page_result.num_results_with_snippets, Eq(0));
}

TEST_F(ResultRetrieverV2SnippetTest, SimpleSnippeted) {
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(CreateDocument(/*id=*/3)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Create ResultSpec with custom snippet spec.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  *result_spec.mutable_snippet_spec() = CreateSnippetSpec();

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/false),
      /*query_terms=*/{{"", {"foo", "bar"}}},
      CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/false), result_spec,
      *document_store_);

  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.num_results_with_snippets, Eq(3));

  const DocumentProto& result_document_one =
      page_result.results.at(0).document();
  const SnippetProto& result_snippet_one = page_result.results.at(0).snippet();
  EXPECT_THAT(result_document_one, EqualsProto(CreateDocument(/*id=*/1)));
  EXPECT_THAT(result_snippet_one.entries(), SizeIs(2));
  EXPECT_THAT(result_snippet_one.entries(0).property_name(), Eq("body"));
  std::string_view content = GetString(
      &result_document_one, result_snippet_one.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_one.entries(0)),
              ElementsAre("body bar 1"));
  EXPECT_THAT(GetMatches(content, result_snippet_one.entries(0)),
              ElementsAre("bar"));
  EXPECT_THAT(result_snippet_one.entries(1).property_name(), Eq("name"));
  content = GetString(&result_document_one,
                      result_snippet_one.entries(1).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_one.entries(1)),
              ElementsAre("subject foo 1"));
  EXPECT_THAT(GetMatches(content, result_snippet_one.entries(1)),
              ElementsAre("foo"));

  const DocumentProto& result_document_two =
      page_result.results.at(1).document();
  const SnippetProto& result_snippet_two = page_result.results.at(1).snippet();
  EXPECT_THAT(result_document_two, EqualsProto(CreateDocument(/*id=*/2)));
  EXPECT_THAT(result_snippet_two.entries(), SizeIs(2));
  EXPECT_THAT(result_snippet_two.entries(0).property_name(), Eq("body"));
  content = GetString(&result_document_two,
                      result_snippet_two.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_two.entries(0)),
              ElementsAre("body bar 2"));
  EXPECT_THAT(GetMatches(content, result_snippet_two.entries(0)),
              ElementsAre("bar"));
  EXPECT_THAT(result_snippet_two.entries(1).property_name(), Eq("name"));
  content = GetString(&result_document_two,
                      result_snippet_two.entries(1).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_two.entries(1)),
              ElementsAre("subject foo 2"));
  EXPECT_THAT(GetMatches(content, result_snippet_two.entries(1)),
              ElementsAre("foo"));

  const DocumentProto& result_document_three =
      page_result.results.at(2).document();
  const SnippetProto& result_snippet_three =
      page_result.results.at(2).snippet();
  EXPECT_THAT(result_document_three, EqualsProto(CreateDocument(/*id=*/3)));
  EXPECT_THAT(result_snippet_three.entries(), SizeIs(2));
  EXPECT_THAT(result_snippet_three.entries(0).property_name(), Eq("body"));
  content = GetString(&result_document_three,
                      result_snippet_three.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_three.entries(0)),
              ElementsAre("body bar 3"));
  EXPECT_THAT(GetMatches(content, result_snippet_three.entries(0)),
              ElementsAre("bar"));
  EXPECT_THAT(result_snippet_three.entries(1).property_name(), Eq("name"));
  content = GetString(&result_document_three,
                      result_snippet_three.entries(1).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_three.entries(1)),
              ElementsAre("subject foo 3"));
  EXPECT_THAT(GetMatches(content, result_snippet_three.entries(1)),
              ElementsAre("foo"));
}

TEST_F(ResultRetrieverV2SnippetTest, OnlyOneDocumentSnippeted) {
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(CreateDocument(/*id=*/3)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Create ResultSpec with custom snippet spec.
  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(1);
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  *result_spec.mutable_snippet_spec() = std::move(snippet_spec);

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/false),
      /*query_terms=*/{{"", {"foo", "bar"}}},
      CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/false), result_spec,
      *document_store_);

  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.num_results_with_snippets, Eq(1));

  const DocumentProto& result_document = page_result.results.at(0).document();
  const SnippetProto& result_snippet = page_result.results.at(0).snippet();
  EXPECT_THAT(result_document, EqualsProto(CreateDocument(/*id=*/1)));
  EXPECT_THAT(result_snippet.entries(), SizeIs(2));
  EXPECT_THAT(result_snippet.entries(0).property_name(), Eq("body"));
  std::string_view content =
      GetString(&result_document, result_snippet.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet.entries(0)),
              ElementsAre("body bar 1"));
  EXPECT_THAT(GetMatches(content, result_snippet.entries(0)),
              ElementsAre("bar"));
  EXPECT_THAT(result_snippet.entries(1).property_name(), Eq("name"));
  content =
      GetString(&result_document, result_snippet.entries(1).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet.entries(1)),
              ElementsAre("subject foo 1"));
  EXPECT_THAT(GetMatches(content, result_snippet.entries(1)),
              ElementsAre("foo"));

  EXPECT_THAT(page_result.results.at(1).document(),
              EqualsProto(CreateDocument(/*id=*/2)));
  EXPECT_THAT(page_result.results.at(1).snippet(),
              EqualsProto(SnippetProto::default_instance()));

  EXPECT_THAT(page_result.results.at(2).document(),
              EqualsProto(CreateDocument(/*id=*/3)));
  EXPECT_THAT(page_result.results.at(2).snippet(),
              EqualsProto(SnippetProto::default_instance()));
}

TEST_F(ResultRetrieverV2SnippetTest, ShouldSnippetAllResults) {
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(CreateDocument(/*id=*/3)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Create ResultSpec with custom snippet spec.
  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(5);
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  *result_spec.mutable_snippet_spec() = std::move(snippet_spec);

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/false),
      /*query_terms=*/{{"", {"foo", "bar"}}},
      CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/false), result_spec,
      *document_store_);

  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  // num_to_snippet = 5, num_previously_returned_in = 0,
  // We can return 5 - 0 = 5 snippets at most. We're able to return all 3
  // snippets here.
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.results.at(0).snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(page_result.results.at(1).snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(page_result.results.at(2).snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(page_result.num_results_with_snippets, Eq(3));
}

TEST_F(ResultRetrieverV2SnippetTest, ShouldSnippetSomeResults) {
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(CreateDocument(/*id=*/3)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Create ResultSpec with custom snippet spec.
  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(5);
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  *result_spec.mutable_snippet_spec() = std::move(snippet_spec);

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/false),
      /*query_terms=*/{{"", {"foo", "bar"}}},
      CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/false), result_spec,
      *document_store_);
  {
    absl_ports::unique_lock l(&result_state.mutex);

    // Set (previously) num_returned = 3 docs
    result_state.num_returned = 3;
  }

  // num_to_snippet = 5, (previously) num_returned = 3,
  // We can return 5 - 3 = 2 snippets.
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.results.at(0).snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(page_result.results.at(1).snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(page_result.results.at(2).snippet().entries(), IsEmpty());
  EXPECT_THAT(page_result.num_results_with_snippets, Eq(2));
}

TEST_F(ResultRetrieverV2SnippetTest, ShouldNotSnippetAnyResults) {
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(CreateDocument(/*id=*/3)));

  std::vector<SectionId> hit_section_ids = {GetSectionId("Email", "name"),
                                            GetSectionId("Email", "body")};
  SectionIdMask hit_section_id_mask = CreateSectionIdMask(hit_section_ids);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, hit_section_id_mask, /*score=*/0},
      {document_id2, hit_section_id_mask, /*score=*/0},
      {document_id3, hit_section_id_mask, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Create ResultSpec with custom snippet spec.
  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(5);
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  *result_spec.mutable_snippet_spec() = std::move(snippet_spec);

  ResultStateV2 result_state(
      std::make_unique<PriorityQueueScoredDocumentHitsRanker>(
          std::move(scored_document_hits), /*is_descending=*/false),
      /*query_terms=*/{{"", {"foo", "bar"}}},
      CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/false), result_spec,
      *document_store_);
  {
    absl_ports::unique_lock l(&result_state.mutex);

    // Set (previously) num_returned = 6 docs
    result_state.num_returned = 6;
  }

  // num_to_snippet = 5, (previously) num_returned = 6,
  // We can't return any snippets for this page.
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.results.at(0).snippet().entries(), IsEmpty());
  EXPECT_THAT(page_result.results.at(1).snippet().entries(), IsEmpty());
  EXPECT_THAT(page_result.results.at(2).snippet().entries(), IsEmpty());
  EXPECT_THAT(page_result.num_results_with_snippets, Eq(0));
}

}  // namespace

}  // namespace lib
}  // namespace icing
