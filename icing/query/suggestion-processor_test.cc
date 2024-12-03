// Copyright (C) 2021 Google LLC
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

#include "icing/query/suggestion-processor.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/index.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/term-metadata.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/platform.h"
#include "icing/query/query-features.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "icing/util/icu-data-file-helper.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::IsEmpty;
using ::testing::Test;
using ::testing::UnorderedElementsAre;

std::vector<std::string> RetrieveSuggestionsText(
    const std::vector<TermMetadata>& terms) {
  std::vector<std::string> suggestions;
  suggestions.reserve(terms.size());
  for (const TermMetadata& term : terms) {
    suggestions.push_back(term.content);
  }
  return suggestions;
}

class SuggestionProcessorTest : public Test {
 protected:
  SuggestionProcessorTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        store_dir_(test_dir_ + "/store"),
        schema_store_dir_(test_dir_ + "/schema_store"),
        index_dir_(test_dir_ + "/index"),
        numeric_index_dir_(test_dir_ + "/numeric_index"),
        embedding_index_dir_(test_dir_ + "/embedding_index") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
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
          icu_data_file_helper::SetUpIcuDataFile(
              GetTestFilePath("icing/icu.dat")));
    }

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                              schema_store_.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);

    Index::Options options(index_dir_,
                           /*index_merge_size=*/1024 * 1024,
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/1024 * 8);
    ICING_ASSERT_OK_AND_ASSIGN(
        index_, Index::Create(options, &filesystem_, &icing_filesystem_));
    // TODO(b/249829533): switch to use persistent numeric index.
    ICING_ASSERT_OK_AND_ASSIGN(
        numeric_index_,
        DummyNumericIndex<int64_t>::Create(filesystem_, numeric_index_dir_));
    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &fake_clock_,
                               feature_flags_.get()));

    language_segmenter_factory::SegmenterOptions segmenter_options(
        ULOC_US, jni_cache_.get());
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(segmenter_options));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/1000));

    ICING_ASSERT_OK_AND_ASSIGN(
        suggestion_processor_,
        SuggestionProcessor::Create(
            index_.get(), numeric_index_.get(), embedding_index_.get(),
            language_segmenter_.get(), normalizer_.get(), document_store_.get(),
            schema_store_.get(), &fake_clock_, feature_flags_.get()));
  }

  libtextclassifier3::Status AddTokenToIndex(
      DocumentId document_id, SectionId section_id,
      TermMatchType::Code term_match_type, const std::string& token) {
    Index::Editor editor = index_->Edit(document_id, section_id,
                                        /*namespace_id=*/0);
    auto status = editor.BufferTerm(token, term_match_type);
    return status.ok() ? editor.IndexAllBufferedTerms() : status;
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string store_dir_;
  const std::string schema_store_dir_;

 private:
  IcingFilesystem icing_filesystem_;
  const std::string index_dir_;
  const std::string numeric_index_dir_;
  const std::string embedding_index_dir_;

 protected:
  std::unique_ptr<Index> index_;
  std::unique_ptr<NumericIndex<int64_t>> numeric_index_;
  std::unique_ptr<EmbeddingIndex> embedding_index_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<Normalizer> normalizer_;
  FakeClock fake_clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<const JniCache> jni_cache_ = GetTestJniCache();
  std::unique_ptr<SuggestionProcessor> suggestion_processor_;
};

constexpr SectionId kSectionId2 = 2;

TEST_F(SuggestionProcessorTest, MultipleTermsTest_And) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId1 = put_result1.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "bar"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "fool"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("bar f");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms), UnorderedElementsAre("bar foo"));
}

TEST_F(SuggestionProcessorTest, MultipleTermsTest_AndNary) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId1 = put_result1.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "bar"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "cat"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "fool"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("bar cat f");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms),
              UnorderedElementsAre("bar cat foo"));
}

TEST_F(SuggestionProcessorTest, MultipleTermsTest_Or) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId1 = put_result1.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "fo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "bar"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "cat"),
              IsOk());

  // Search for "(bar OR cat) AND f" both document1 "bar fo" and document2 "cat
  // foo" could match.
  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("bar OR cat f");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms),
              UnorderedElementsAre("bar OR cat fo", "bar OR cat foo"));
}

TEST_F(SuggestionProcessorTest, MultipleTermsTest_OrNary) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "3")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId2 = put_result2.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "fo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "bar"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "cat"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId2, kSectionId2,
                              TermMatchType::EXACT_ONLY, "fool"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId2, kSectionId2,
                              TermMatchType::EXACT_ONLY, "lot"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  // Search for "((bar OR cat) OR lot) AND f"
  suggestion_spec.set_prefix("bar OR cat OR lot f");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  // "fo" in document1, "foo" in document2 and "fool" in document3 could match.
  EXPECT_THAT(
      RetrieveSuggestionsText(terms),
      UnorderedElementsAre("bar OR cat OR lot fo", "bar OR cat OR lot foo",
                           "bar OR cat OR lot fool"));
}

TEST_F(SuggestionProcessorTest, MultipleTermsTest_NormalizedTerm) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId1 = put_result1.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "bar"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "fool"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(documentId1, kSectionId2,
                              TermMatchType::EXACT_ONLY, "bar"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  // Search for "bar AND FO"
  suggestion_spec.set_prefix("bar FO");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  // The term is normalized.
  EXPECT_THAT(RetrieveSuggestionsText(terms),
              UnorderedElementsAre("bar foo", "bar fool"));

  // Search for "bar AND ḞÖ"
  suggestion_spec.set_prefix("bar ḞÖ");
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, suggestion_processor_->QuerySuggestions(
                 suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  // The term is normalized.
  EXPECT_THAT(RetrieveSuggestionsText(terms),
              UnorderedElementsAre("bar foo", "bar fool"));
}

TEST_F(SuggestionProcessorTest, NonExistentPrefixTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("nonExistTerm");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(terms, IsEmpty());
}

TEST_F(SuggestionProcessorTest, PrefixTrailingSpaceTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("f    ");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(terms, IsEmpty());
}

TEST_F(SuggestionProcessorTest, NormalizePrefixTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.

  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("F");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms), UnorderedElementsAre("foo"));

  suggestion_spec.set_prefix("fO");
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, suggestion_processor_->QuerySuggestions(
                 suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms), UnorderedElementsAre("foo"));

  suggestion_spec.set_prefix("Fo");
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, suggestion_processor_->QuerySuggestions(
                 suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms), UnorderedElementsAre("foo"));

  suggestion_spec.set_prefix("FO");
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, suggestion_processor_->QuerySuggestions(
                 suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(RetrieveSuggestionsText(terms), UnorderedElementsAre("foo"));
}

TEST_F(SuggestionProcessorTest, ParenthesesOperatorPrefixTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.

  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;
  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("{f}");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(terms, IsEmpty());

  suggestion_spec.set_prefix("[f]");
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, suggestion_processor_->QuerySuggestions(
                 suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(terms, IsEmpty());

  suggestion_spec.set_prefix("(f)");
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, suggestion_processor_->QuerySuggestions(
                 suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(terms, IsEmpty());
}

TEST_F(SuggestionProcessorTest, OtherSpecialPrefixTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("f:");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  EXPECT_THAT(suggestion_processor_->QuerySuggestions(
                  suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // TODO(b/208654892): Update handling for hyphens to only consider it a hyphen
  // within a TEXT token (rather than a MINUS token) when surrounded on both
  // sides by TEXT rather than just preceded by TEXT.
  suggestion_spec.set_prefix("f-");
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<TermMetadata> terms,
      suggestion_processor_->QuerySuggestions(
          suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(terms, IsEmpty());

  suggestion_spec.set_prefix("f OR");
  EXPECT_THAT(suggestion_processor_->QuerySuggestions(
                  suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  suggestion_spec.set_prefix(
      "bar OR semanticSearch(getEmbeddingParameter(0), 0.5, 1)");
  suggestion_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  suggestion_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  PropertyProto::VectorProto* vector =
      suggestion_spec.add_embedding_query_vectors();
  vector->set_model_signature("model_signature");
  vector->add_values(0.1);
  EXPECT_THAT(suggestion_processor_->QuerySuggestions(
                  suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SuggestionProcessorTest, SemanticSearchPrefixTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "foo"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  // Suggesting without adding embedding query vectors will cause a failure.
  suggestion_spec.set_prefix(
      "semanticSearch(getEmbeddingParameter(0), 0.5, 1) OR foo");
  suggestion_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  EXPECT_THAT(suggestion_processor_->QuerySuggestions(
                  suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));

  // Adding embedding query vectors will allow us to successfully suggest.
  suggestion_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  PropertyProto::VectorProto* vector =
      suggestion_spec.add_embedding_query_vectors();
  vector->set_model_signature("model_signature");
  vector->add_values(0.1);
  EXPECT_THAT(suggestion_processor_->QuerySuggestions(
                  suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::OK));
}

TEST_F(SuggestionProcessorTest, InvalidPrefixTest) {
  // Create the schema and document store
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();
  ASSERT_THAT(schema_store_->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false,
                  /*allow_circular_schema_definitions=*/false),
              IsOk());

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result0,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  DocumentId documentId0 = put_result0.new_document_id;

  ASSERT_THAT(AddTokenToIndex(documentId0, kSectionId2,
                              TermMatchType::EXACT_ONLY, "original"),
              IsOk());

  SuggestionSpecProto suggestion_spec;
  suggestion_spec.set_prefix("OR OR - :");
  suggestion_spec.set_num_to_return(10);
  suggestion_spec.mutable_scoring_spec()->set_scoring_match_type(
      TermMatchType::PREFIX);

  EXPECT_THAT(suggestion_processor_->QuerySuggestions(
                  suggestion_spec, fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing
