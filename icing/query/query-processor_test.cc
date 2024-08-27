// Copyright (C) 2019 Google LLC
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

#include "icing/query/query-processor.h"

#include <cstdint>
#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/platform.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-features.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class QueryProcessorTest
    : public ::testing::TestWithParam<SearchSpecProto::SearchType::Code> {
 protected:
  QueryProcessorTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        store_dir_(test_dir_ + "/store"),
        schema_store_dir_(test_dir_ + "/schema_store"),
        index_dir_(test_dir_ + "/index") {}

  void SetUp() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(index_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      // If we've specified using the reverse-JNI method for segmentation (i.e.
      // not ICU), then we won't have the ICU data file included to set up.
      // Technically, we could choose to use reverse-JNI for segmentation AND
      // include an ICU data file, but that seems unlikely and our current BUILD
      // setup doesn't do this.
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpICUDataFile(
              GetTestFilePath("icing/icu.dat")));
    }
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                              schema_store_.get()));
    document_store_ = std::move(create_result.document_store);

    Index::Options options(index_dir_,
                           /*index_merge_size=*/1024 * 1024);
    ICING_ASSERT_OK_AND_ASSIGN(
        index_, Index::Create(options, &filesystem_, &icing_filesystem_));
    // TODO(b/249829533): switch to use persistent numeric index.
    numeric_index_ = std::make_unique<DummyNumericIndex<int64_t>>();

    language_segmenter_factory::SegmenterOptions segmenter_options(
        ULOC_US, jni_cache_.get());
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(segmenter_options));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/1000));

    ICING_ASSERT_OK_AND_ASSIGN(
        query_processor_,
        QueryProcessor::Create(index_.get(), numeric_index_.get(),
                               language_segmenter_.get(), normalizer_.get(),
                               document_store_.get(), schema_store_.get()));
  }

  libtextclassifier3::Status AddTokenToIndex(
      DocumentId document_id, SectionId section_id,
      TermMatchType::Code term_match_type, const std::string& token) {
    Index::Editor editor = index_->Edit(document_id, section_id,
                                        term_match_type, /*namespace_id=*/0);
    auto status = editor.BufferTerm(token.c_str());
    return status.ok() ? editor.IndexAllBufferedTerms() : status;
  }

  libtextclassifier3::Status AddToNumericIndex(DocumentId document_id,
                                               const std::string& property,
                                               SectionId section_id,
                                               int64_t value) {
    std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
        numeric_index_->Edit(property, document_id, section_id);
    ICING_RETURN_IF_ERROR(editor->BufferKey(value));
    return editor->IndexAllBufferedKeys();
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }
  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string store_dir_;
  const std::string schema_store_dir_;

 private:
  IcingFilesystem icing_filesystem_;
  const std::string index_dir_;

 protected:
  std::unique_ptr<Index> index_;
  std::unique_ptr<NumericIndex<int64_t>> numeric_index_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<Normalizer> normalizer_;
  FakeClock fake_clock_;
  std::unique_ptr<const JniCache> jni_cache_ = GetTestJniCache();
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<QueryProcessor> query_processor_;
};

TEST_P(QueryProcessorTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      QueryProcessor::Create(/*index=*/nullptr, numeric_index_.get(),
                             language_segmenter_.get(), normalizer_.get(),
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(index_.get(), /*numeric_index_=*/nullptr,
                             language_segmenter_.get(), normalizer_.get(),
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(index_.get(), numeric_index_.get(),
                             /*language_segmenter=*/nullptr, normalizer_.get(),
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(
          index_.get(), numeric_index_.get(), language_segmenter_.get(),
          /*normalizer=*/nullptr, document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(index_.get(), numeric_index_.get(),
                             language_segmenter_.get(), normalizer_.get(),
                             /*document_store=*/nullptr, schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(QueryProcessor::Create(index_.get(), numeric_index_.get(),
                                     language_segmenter_.get(),
                                     normalizer_.get(), document_store_.get(),
                                     /*schema_store=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_P(QueryProcessorTest, EmptyGroupMatchAllDocuments) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // We don't need to insert anything in the index since the empty query will
  // match all DocumentIds from the DocumentStore
  SearchSpecProto search_spec;
  search_spec.set_query("()");
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocumentIds(results.root_iterator.get()),
              ElementsAre(document_id2, document_id1));
  EXPECT_THAT(results.query_terms, IsEmpty());
  EXPECT_THAT(results.query_term_iterators, IsEmpty());
}

TEST_P(QueryProcessorTest, EmptyQueryMatchAllDocuments) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // We don't need to insert anything in the index since the empty query will
  // match all DocumentIds from the DocumentStore
  SearchSpecProto search_spec;
  search_spec.set_query("");
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocumentIds(results.root_iterator.get()),
              ElementsAre(document_id2, document_id1));
  EXPECT_THAT(results.query_terms, IsEmpty());
  EXPECT_THAT(results.query_term_iterators, IsEmpty());
}

TEST_P(QueryProcessorTest, QueryTermNormalized) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hElLo WORLD");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
    EXPECT_EQ(matched_terms_stats.at(1).term, "world");
    EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("hello", "world"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, OneTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("he");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "he");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, OneTermPrefixMatchWithMaxSectionID) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = kMaxSectionId;
  SectionIdMask section_id_mask = UINT64_C(1) << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{};
  term_frequencies[kMaxSectionId] = 1;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("he");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "he");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, OneTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, AndSameTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hello hello");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);
  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
  }

  ASSERT_FALSE(results.root_iterator->Advance().ok());

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, AndTwoTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hello world");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
    EXPECT_EQ(matched_terms_stats.at(1).term, "world");
    EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("hello", "world"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, AndSameTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("he he");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "he");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
  }

  ASSERT_FALSE(results.root_iterator->Advance().ok());

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, AndTwoTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("he wo");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
    EXPECT_EQ(matched_terms_stats.at(0).term, "he");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
    EXPECT_EQ(matched_terms_stats.at(1).term, "wo");
    EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he", "wo"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, AndTwoTermPrefixAndExactMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(AddTokenToIndex(document_id, section_id,
                              TermMatchType::EXACT_ONLY, "hello"),
              IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hello wo");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
    EXPECT_EQ(matched_terms_stats.at(1).term, "wo");
    EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "wo"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, OrTwoTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hello OR world");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id2);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);
  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "world");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
  }

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id1);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);
  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("hello", "world"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, OrTwoTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("he OR wo");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id2);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "wo");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
  }

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id1);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "he");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he", "wo"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, OrTwoTermPrefixAndExactMatch) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};

  EXPECT_THAT(AddTokenToIndex(document_id1, section_id,
                              TermMatchType::EXACT_ONLY, "hello"),
              IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, TermMatchType::PREFIX, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("hello OR wo");
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id2);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "wo");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));
  }

  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id1);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(1));  // 1 term
    EXPECT_EQ(matched_terms_stats.at(0).term, "hello");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(term_frequencies));

    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "wo"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, CombinedAndOrTerms) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  std::array<Hit::TermFrequency, kTotalNumSections> term_frequencies{1};
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());
  index_->Merge();

  // Document 2 has content "animal kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  {
    // OR gets precedence over AND, this is parsed as ((puppy OR kitten) AND
    // dog)
    SearchSpecProto search_spec;
    search_spec.set_query("puppy OR kitten dog");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // Only Document 1 matches since it has puppy AND dog
    ASSERT_THAT(results.root_iterator->Advance(), IsOk());
    EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(),
              document_id1);
    EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
              section_id_mask);

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      std::vector<TermMatchInfo> matched_terms_stats;
      results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
      ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
      EXPECT_EQ(matched_terms_stats.at(0).term, "puppy");
      EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                  ElementsAreArray(term_frequencies));
      EXPECT_EQ(matched_terms_stats.at(1).term, "dog");
      EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                  ElementsAreArray(term_frequencies));

      EXPECT_THAT(results.query_terms, SizeIs(1));
      EXPECT_THAT(results.query_terms[""],
                  UnorderedElementsAre("puppy", "kitten", "dog"));
      EXPECT_THAT(results.query_term_iterators, SizeIs(3));
    }
  }

  {
    // OR gets precedence over AND, this is parsed as (animal AND (puppy OR
    // kitten))
    SearchSpecProto search_spec;
    search_spec.set_query("animal puppy OR kitten");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // Both Document 1 and 2 match since Document 1 has animal AND puppy, and
    // Document 2 has animal AND kitten
    // Descending order of valid DocumentIds
    ASSERT_THAT(results.root_iterator->Advance(), IsOk());
    EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(),
              document_id2);
    EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
              section_id_mask);

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      std::vector<TermMatchInfo> matched_terms_stats;
      results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
      ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
      EXPECT_EQ(matched_terms_stats.at(0).term, "animal");
      EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                  ElementsAreArray(term_frequencies));
      EXPECT_EQ(matched_terms_stats.at(1).term, "kitten");
      EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                  ElementsAreArray(term_frequencies));
    }

    ASSERT_THAT(results.root_iterator->Advance(), IsOk());
    EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(),
              document_id1);
    EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
              section_id_mask);

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      std::vector<TermMatchInfo> matched_terms_stats;
      results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
      ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
      EXPECT_EQ(matched_terms_stats.at(0).term, "animal");
      EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                  ElementsAreArray(term_frequencies));
      EXPECT_EQ(matched_terms_stats.at(1).term, "puppy");
      EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                  ElementsAreArray(term_frequencies));

      EXPECT_THAT(results.query_terms, SizeIs(1));
      EXPECT_THAT(results.query_terms[""],
                  UnorderedElementsAre("animal", "puppy", "kitten"));
      EXPECT_THAT(results.query_term_iterators, SizeIs(3));
    }
  }

  {
    // OR gets precedence over AND, this is parsed as (kitten AND ((foo OR
    // bar) OR cat))
    SearchSpecProto search_spec;
    search_spec.set_query("kitten foo OR bar OR cat");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // Only Document 2 matches since it has both kitten and cat
    ASSERT_THAT(results.root_iterator->Advance(), IsOk());
    EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(),
              document_id2);
    EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
              section_id_mask);

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      std::vector<TermMatchInfo> matched_terms_stats;
      results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
      ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
      EXPECT_EQ(matched_terms_stats.at(0).term, "kitten");
      EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                  ElementsAreArray(term_frequencies));
      EXPECT_EQ(matched_terms_stats.at(1).term, "cat");
      EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
      EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                  ElementsAreArray(term_frequencies));

      EXPECT_THAT(results.query_terms, SizeIs(1));
      EXPECT_THAT(results.query_terms[""],
                  UnorderedElementsAre("kitten", "foo", "bar", "cat"));
      EXPECT_THAT(results.query_term_iterators, SizeIs(4));
    }
  }
}

TEST_P(QueryProcessorTest, OneGroup) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  // Without grouping, this would be parsed as ((puppy OR kitten) AND foo) and
  // no documents would match. But with grouping, Document 1 matches puppy
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR (kitten foo)");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo(document_id1);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("puppy", "kitten", "foo"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(3));
  }
}

TEST_P(QueryProcessorTest, TwoGroups) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  // Without grouping, this would be parsed as (puppy AND (dog OR kitten) AND
  // cat) and wouldn't match any documents. But with grouping, Document 1
  // matches (puppy AND dog) and Document 2 matches (kitten and cat).
  SearchSpecProto search_spec;
  search_spec.set_query("(puppy dog) OR (kitten cat)");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo1(document_id1);
  expectedDocHitInfo1.UpdateSection(/*section_id=*/0);
  DocHitInfo expectedDocHitInfo2(document_id2);
  expectedDocHitInfo2.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo2, expectedDocHitInfo1));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("puppy", "dog", "kitten", "cat"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(4));
  }
}

TEST_P(QueryProcessorTest, ManyLevelNestedGrouping) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  // Without grouping, this would be parsed as ((puppy OR kitten) AND foo) and
  // no documents would match. But with grouping, Document 1 matches puppy
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR ((((kitten foo))))");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo(document_id1);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("puppy", "kitten", "foo"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(3));
  }
}

TEST_P(QueryProcessorTest, OneLevelNestedGrouping) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  // Document 1 will match puppy and Document 2 matches (kitten AND (cat))
  SearchSpecProto search_spec;
  // TODO(b/208654892) decide how we want to handle queries of the form foo(...)
  search_spec.set_query("puppy OR (kitten (cat))");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo1(document_id1);
  expectedDocHitInfo1.UpdateSection(/*section_id=*/0);
  DocHitInfo expectedDocHitInfo2(document_id2);
  expectedDocHitInfo2.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo2, expectedDocHitInfo1));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("puppy", "kitten", "cat"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(3));
  }
}

TEST_P(QueryProcessorTest, ExcludeTerm) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("-hello");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE));

  // We don't know have the section mask to indicate what section "world"
  // came. It doesn't matter which section it was in since the query doesn't
  // care.  It just wanted documents that didn't have "hello"
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, kSectionIdMaskNone)));
  EXPECT_THAT(results.query_terms, IsEmpty());
  EXPECT_THAT(results.query_term_iterators, IsEmpty());
}

TEST_P(QueryProcessorTest, ExcludeNonexistentTerm) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("-foo");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, kSectionIdMaskNone),
                          DocHitInfo(document_id1, kSectionIdMaskNone)));
  EXPECT_THAT(results.query_terms, IsEmpty());
  EXPECT_THAT(results.query_term_iterators, IsEmpty());
}

TEST_P(QueryProcessorTest, ExcludeAnd) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-dog -cat");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // The query is interpreted as "exclude all documents that have animal,
    // and exclude all documents that have cat". Since both documents contain
    // animal, there are no results.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      EXPECT_THAT(results.query_terms, IsEmpty());
      EXPECT_THAT(results.query_term_iterators, IsEmpty());
    }
  }

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-animal cat");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // The query is interpreted as "exclude all documents that have animal,
    // and include all documents that have cat". Since both documents contain
    // animal, there are no results.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      EXPECT_THAT(results.query_terms, SizeIs(1));
      EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("cat"));
      EXPECT_THAT(results.query_term_iterators, SizeIs(1));
    }
  }
}

TEST_P(QueryProcessorTest, ExcludeOr) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-animal OR -cat");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // We don't have a section mask indicating which sections in this document
    // matched the query since it's not based on section-term matching. It's
    // more based on the fact that the query excluded all the other documents.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id1, kSectionIdMaskNone)));

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      EXPECT_THAT(results.query_terms, IsEmpty());
      EXPECT_THAT(results.query_term_iterators, IsEmpty());
    }
  }

  {
    SearchSpecProto search_spec;
    search_spec.set_query("animal OR -cat");
    search_spec.set_term_match_type(term_match_type);
    search_spec.set_search_type(GetParam());

    ICING_ASSERT_OK_AND_ASSIGN(
        QueryResults results,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

    // Descending order of valid DocumentIds
    DocHitInfo expectedDocHitInfo1(document_id1);
    expectedDocHitInfo1.UpdateSection(/*section_id=*/0);
    DocHitInfo expectedDocHitInfo2(document_id2);
    expectedDocHitInfo2.UpdateSection(/*section_id=*/0);
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(expectedDocHitInfo2, expectedDocHitInfo1));

    // TODO(b/208654892) Support Query Terms with advanced query
    if (GetParam() !=
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
      EXPECT_THAT(results.query_terms, SizeIs(1));
      EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
    }
  }
}

TEST_P(QueryProcessorTest, WithoutTermFrequency) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal puppy dog", which is added to the main
  // index.
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());
  ASSERT_THAT(index_->Merge(), IsOk());

  // Document 2 has content "animal kitten cat", which is added to the lite
  // index.
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  // OR gets precedence over AND, this is parsed as (animal AND (puppy OR
  // kitten))
  SearchSpecProto search_spec;
  search_spec.set_query("animal puppy OR kitten");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE));
  // Since need_hit_term_frequency is false, the expected term frequencies
  // should all be 0.
  Hit::TermFrequencyArray exp_term_frequencies{0};

  // Descending order of valid DocumentIds
  // The first Document to match (Document 2) matches on 'animal' AND 'kitten'
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id2);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
    EXPECT_EQ(matched_terms_stats.at(0).term, "animal");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(exp_term_frequencies));
    EXPECT_EQ(matched_terms_stats.at(1).term, "kitten");
    EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                ElementsAreArray(exp_term_frequencies));
  }

  // The second Document to match (Document 1) matches on 'animal' AND 'puppy'
  ASSERT_THAT(results.root_iterator->Advance(), IsOk());
  EXPECT_EQ(results.root_iterator->doc_hit_info().document_id(), document_id1);
  EXPECT_EQ(results.root_iterator->doc_hit_info().hit_section_ids_mask(),
            section_id_mask);

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    std::vector<TermMatchInfo> matched_terms_stats;
    results.root_iterator->PopulateMatchedTermsStats(&matched_terms_stats);
    ASSERT_THAT(matched_terms_stats, SizeIs(2));  // 2 terms
    EXPECT_EQ(matched_terms_stats.at(0).term, "animal");
    EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
                ElementsAreArray(exp_term_frequencies));
    EXPECT_EQ(matched_terms_stats.at(1).term, "puppy");
    EXPECT_EQ(matched_terms_stats.at(1).section_ids_mask, section_id_mask);
    EXPECT_THAT(matched_terms_stats.at(1).term_frequencies,
                ElementsAreArray(exp_term_frequencies));

    // This should be empty because ranking_strategy != RELEVANCE_SCORE
    EXPECT_THAT(results.query_term_iterators, IsEmpty());
  }
}

TEST_P(QueryProcessorTest, DeletedFilter) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  EXPECT_THAT(document_store_->Delete("namespace", "1"), IsOk());

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo(document_id2);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));
  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, NamespaceFilter) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace2", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.add_namespace_filters("namespace1");
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo(document_id1);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, SchemaTypeFilter) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email"))
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.add_schema_type_filters("email");
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo(document_id1);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, PropertyFilterForOneDocument) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  // First and only indexed property, so it gets a section_id of 0
  int subject_section_id = 0;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document has content "animal"
  ASSERT_THAT(AddTokenToIndex(document_id, subject_section_id, term_match_type,
                              "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("subject:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Descending order of valid DocumentIds
  DocHitInfo expectedDocHitInfo(document_id);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["subject"], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, PropertyFilterAcrossSchemaTypes) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("email")
                       // Section "a" would get sectionId 0
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("a")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("foo")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder().SetType("message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  int email_foo_section_id = 1;
  int message_foo_section_id = 0;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("foo:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Ordered by descending DocumentId, so message comes first since it was
  // inserted last
  DocHitInfo expectedDocHitInfo1(message_document_id);
  expectedDocHitInfo1.UpdateSection(/*section_id=*/0);
  DocHitInfo expectedDocHitInfo2(email_document_id);
  expectedDocHitInfo2.UpdateSection(/*section_id=*/1);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo1, expectedDocHitInfo2));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, PropertyFilterWithinSchemaType) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder().SetType("message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  int email_foo_section_id = 0;
  int message_foo_section_id = 0;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo:animal");
  search_spec.add_schema_type_filters("email");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Shouldn't include the message document since we're only looking at email
  // types
  DocHitInfo expectedDocHitInfo(email_document_id);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));
  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, NestedPropertyFilter) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("email")
                  // Add an unindexed property so we generate section
                  // metadata on it
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("foo")
                                   .SetDataTypeDocument(
                                       "Foo", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Foo")
                  // Add an unindexed property so we generate section
                  // metadata on it
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("bar")
                                   .SetDataTypeDocument(
                                       "Bar", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Bar")
                       // Add an unindexed property so we generate section
                       // metadata on it
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("baz")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, /*section_id=*/0,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo.bar.baz:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  DocHitInfo expectedDocHitInfo1(email_document_id);
  expectedDocHitInfo1.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo1));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["foo.bar.baz"],
                UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, PropertyFilterRespectsDifferentSectionIds) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder().SetType("message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("bar")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  int email_foo_section_id = 0;
  int message_foo_section_id = 0;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal", but put in in the same section id
  // as the indexed email section id, the same id as indexed property "foo" in
  // the message type
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  DocHitInfo expectedDocHitInfo(email_document_id);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, NonexistentPropertyFilterReturnsEmptyResults) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, /*section_id=*/0,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("nonexistent:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["nonexistent"],
                UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, UnindexedPropertyFilterReturnsEmptyResults) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("email")
                       // Add an unindexed property so we generate section
                       // metadata on it
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("foo")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, /*section_id=*/0,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(1));
  }
}

TEST_P(QueryProcessorTest, PropertyFilterTermAndUnrestrictedTerm) {
  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder().SetType("message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  int email_foo_section_id = 0;
  int message_foo_section_id = 0;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Poplate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "cat"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("cat OR foo:animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));

  // Ordered by descending DocumentId, so message comes first since it was
  // inserted last
  DocHitInfo expectedDocHitInfo1(message_document_id);
  expectedDocHitInfo1.UpdateSection(/*section_id=*/0);
  DocHitInfo expectedDocHitInfo2(email_document_id);
  expectedDocHitInfo2.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo1, expectedDocHitInfo2));

  // TODO(b/208654892) Support Query Terms with advanced query
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    EXPECT_THAT(results.query_terms, SizeIs(2));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("cat"));
    EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
    EXPECT_THAT(results.query_term_iterators, SizeIs(2));
  }
}

TEST_P(QueryProcessorTest, DocumentBeforeTtlNotFilteredOut) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // Arbitrary value, just has to be less than the document's creation
  // timestamp + ttl
  FakeClock fake_clock;
  fake_clock.SetSystemTimeMilliseconds(50);

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema("email")
                               .SetCreationTimestampMs(10)
                               .SetTtlMs(100)
                               .Build()));

  // Populate the index
  int section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> local_query_processor,
      QueryProcessor::Create(index_.get(), numeric_index_.get(),
                             language_segmenter_.get(), normalizer_.get(),
                             document_store_.get(), schema_store_.get()));

  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      local_query_processor->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::NONE));

  DocHitInfo expectedDocHitInfo(document_id);
  expectedDocHitInfo.UpdateSection(/*section_id=*/0);
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(expectedDocHitInfo));
}

TEST_P(QueryProcessorTest, DocumentPastTtlFilteredOut) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  // Arbitrary value, just has to be greater than the document's creation
  // timestamp + ttl
  FakeClock fake_clock;
  fake_clock.SetSystemTimeMilliseconds(200);

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema("email")
                               .SetCreationTimestampMs(50)
                               .SetTtlMs(100)
                               .Build()));

  // Populate the index
  int section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> local_query_processor,
      QueryProcessor::Create(index_.get(), numeric_index_.get(),
                             language_segmenter_.get(), normalizer_.get(),
                             document_store_.get(), schema_store_.get()));

  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);
  search_spec.set_search_type(GetParam());

  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      local_query_processor->ParseSearch(
          search_spec, ScoringSpecProto::RankingStrategy::NONE));

  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
}

TEST_P(QueryProcessorTest, NumericFilter) {
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    GTEST_SKIP() << "Numeric filter is only supported in advanced query.";
  }

  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("transaction")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("price")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("cost")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  // SectionIds are assigned alphabetically
  SectionId cost_section_id = 0;
  SectionId price_section_id = 1;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_one_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema("transaction")
                               .AddInt64Property("price", 10)
                               .Build()));
  ICING_ASSERT_OK(
      AddToNumericIndex(document_one_id, "price", price_section_id, 10));

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_two_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "2")
                               .SetSchema("transaction")
                               .AddInt64Property("price", 25)
                               .Build()));
  ICING_ASSERT_OK(
      AddToNumericIndex(document_two_id, "price", price_section_id, 25));

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_three_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "3")
                               .SetSchema("transaction")
                               .AddInt64Property("cost", 2)
                               .Build()));
  ICING_ASSERT_OK(
      AddToNumericIndex(document_three_id, "cost", cost_section_id, 2));

  SearchSpecProto search_spec;
  search_spec.set_query("price < 20");
  search_spec.set_search_type(GetParam());
  search_spec.add_enabled_features(std::string(kNumericSearchFeature));
  ICING_ASSERT_OK_AND_ASSIGN(
      QueryResults results,
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE));
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(EqualsDocHitInfo(
                  document_one_id, std::vector<SectionId>{price_section_id})));

  search_spec.set_query("price == 25");
  ICING_ASSERT_OK_AND_ASSIGN(
      results, query_processor_->ParseSearch(
                   search_spec, ScoringSpecProto::RankingStrategy::NONE));
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(EqualsDocHitInfo(
                  document_two_id, std::vector<SectionId>{price_section_id})));

  search_spec.set_query("cost > 2");
  ICING_ASSERT_OK_AND_ASSIGN(
      results, query_processor_->ParseSearch(
                   search_spec, ScoringSpecProto::RankingStrategy::NONE));
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());

  search_spec.set_query("cost >= 2");
  ICING_ASSERT_OK_AND_ASSIGN(
      results, query_processor_->ParseSearch(
                   search_spec, ScoringSpecProto::RankingStrategy::NONE));
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(EqualsDocHitInfo(
                  document_three_id, std::vector<SectionId>{cost_section_id})));

  search_spec.set_query("price <= 25");
  ICING_ASSERT_OK_AND_ASSIGN(
      results, query_processor_->ParseSearch(
                   search_spec, ScoringSpecProto::RankingStrategy::NONE));
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(EqualsDocHitInfo(document_two_id,
                                   std::vector<SectionId>{price_section_id}),
                  EqualsDocHitInfo(document_one_id,
                                   std::vector<SectionId>{price_section_id})));
}

TEST_P(QueryProcessorTest, NumericFilterWithoutEnablingFeatureFails) {
  if (GetParam() !=
      SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY) {
    GTEST_SKIP() << "Numeric filter is only supported in advanced query.";
  }

  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("transaction")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("price")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  SectionId price_section_id = 0;
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_one_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema("transaction")
                               .AddInt64Property("price", 10)
                               .Build()));
  ICING_ASSERT_OK(
      AddToNumericIndex(document_one_id, "price", price_section_id, 10));

  SearchSpecProto search_spec;
  search_spec.set_query("price < 20");
  search_spec.set_search_type(GetParam());

  libtextclassifier3::StatusOr<QueryResults> result_or =
      query_processor_->ParseSearch(search_spec,
                                    ScoringSpecProto::RankingStrategy::NONE);
  EXPECT_THAT(result_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

INSTANTIATE_TEST_SUITE_P(
    QueryProcessorTest, QueryProcessorTest,
    testing::Values(
        SearchSpecProto::SearchType::ICING_RAW_QUERY,
        SearchSpecProto::SearchType::EXPERIMENTAL_ICING_ADVANCED_QUERY));

}  // namespace

}  // namespace lib
}  // namespace icing
