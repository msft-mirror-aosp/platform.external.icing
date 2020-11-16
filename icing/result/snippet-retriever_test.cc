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

#include "icing/result/snippet-retriever.h"

#include <cstdint>
#include <limits>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/mock-filesystem.h"
#include "icing/helpers/icu/icu-data-file-helper.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/query/query-terms.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section-manager.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/snippet-helpers.h"
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

using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::SizeIs;

class SnippetRetrieverTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing";
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());

    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        icu_data_file_helper::SetUpICUDataFile(
            GetTestFilePath("icing/icu.dat")));
    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    // Setup the schema
    ICING_ASSERT_OK_AND_ASSIGN(schema_store_,
                               SchemaStore::Create(&filesystem_, test_dir_));
    SchemaProto schema;
    SchemaTypeConfigProto* type_config = schema.add_types();
    type_config->set_schema_type("email");
    PropertyConfigProto* prop_config = type_config->add_properties();
    prop_config->set_property_name("subject");
    prop_config->set_data_type(PropertyConfigProto::DataType::STRING);
    prop_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    prop_config->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    prop_config->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);
    prop_config = type_config->add_properties();
    prop_config->set_property_name("body");
    prop_config->set_data_type(PropertyConfigProto::DataType::STRING);
    prop_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    prop_config->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    prop_config->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);
    ICING_ASSERT_OK(schema_store_->SetSchema(schema));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/10000));
    ICING_ASSERT_OK_AND_ASSIGN(
        snippet_retriever_,
        SnippetRetriever::Create(schema_store_.get(), language_segmenter_.get(),
                                 normalizer_.get()));

    // Set limits to max - effectively no limit. Enable matching and request a
    // window of 64 bytes.
    snippet_spec_.set_num_to_snippet(std::numeric_limits<int32_t>::max());
    snippet_spec_.set_num_matches_per_property(
        std::numeric_limits<int32_t>::max());
    snippet_spec_.set_max_window_bytes(64);
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  Filesystem filesystem_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<SnippetRetriever> snippet_retriever_;
  std::unique_ptr<Normalizer> normalizer_;
  ResultSpecProto::SnippetSpecProto snippet_spec_;
  std::string test_dir_;
};

TEST_F(SnippetRetrieverTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      SnippetRetriever::Create(/*schema_store=*/nullptr,
                               language_segmenter_.get(), normalizer_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(SnippetRetriever::Create(schema_store_.get(),
                                       /*language_segmenter=*/nullptr,
                                       normalizer_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      SnippetRetriever::Create(schema_store_.get(), language_segmenter_.get(),
                               /*normalizer=*/nullptr),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowSizeSmallerThanMatch) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window starts at the beginning of "three" and ends in the middle of
  // "three". len=4, orig_window= "thre"
  snippet_spec_.set_max_window_bytes(4);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq(""));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowStartsInWhitespace) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window starts at the space between "one" and "two". Window ends in the
  // middle of "four".
  // len=14, orig_window=" two three fou"
  snippet_spec_.set_max_window_bytes(14);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("two three"));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowStartsMidToken) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window starts in the middle of "one" and ends at the end of "four".
  // len=16, orig_window="e two three four"
  snippet_spec_.set_max_window_bytes(16);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("two three four"));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowEndsInPunctuation) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window ends in the middle of all the punctuation and window starts at 0.
  // len=20, orig_window="one two three four.."
  snippet_spec_.set_max_window_bytes(20);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("one two three four.."));
}

TEST_F(SnippetRetrieverTest,
       SnippetingWindowMaxWindowEndsInMiddleOfMultiBytePunctuation) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body",
                             "Is everything upside down in Australia¿ Crikey!")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"in"}}};

  // Window ends in the middle of all the punctuation and window starts at 0.
  // len=26, orig_window="pside down in Australia\xC2"
  snippet_spec_.set_max_window_bytes(24);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("down in Australia"));
}

TEST_F(SnippetRetrieverTest,
       SnippetingWindowMaxWindowEndsInMultiBytePunctuation) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body",
                             "Is everything upside down in Australia¿ Crikey!")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"in"}}};

  // Window ends in the middle of all the punctuation and window starts at 0.
  // len=26, orig_window="upside down in Australia\xC2\xBF"
  snippet_spec_.set_max_window_bytes(26);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("upside down in Australia¿"));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowStartsBeforeValueStart) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window starts before 0.
  // len=22, orig_window="one two three four..."
  snippet_spec_.set_max_window_bytes(22);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("one two three four..."));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowEndsInWhitespace) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window ends before "five" but after all the punctuation
  // len=26, orig_window="one two three four.... "
  snippet_spec_.set_max_window_bytes(26);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("one two three four...."));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowEndsMidToken) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Window ends in the middle of "five"
  // len=32, orig_window="one two three four.... fiv"
  snippet_spec_.set_max_window_bytes(32);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("one two three four...."));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowSizeEqualToValueSize) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Max window size equals the size of the value.
  // len=34, orig_window="one two three four.... five"
  snippet_spec_.set_max_window_bytes(34);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("one two three four.... five"));
}

TEST_F(SnippetRetrieverTest, SnippetingWindowMaxWindowSizeLargerThanValueSize) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "counting")
          .AddStringProperty("body", "one two three four.... five")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"three"}}};

  // Max window size exceeds the size of the value.
  // len=36, orig_window="one two three four.... five"
  snippet_spec_.set_max_window_bytes(36);
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", /*snippet_index=*/0),
              Eq("one two three four.... five"));
}

TEST_F(SnippetRetrieverTest, PrefixSnippeting) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body", "Only a fool would match this content.")
          .Build();
  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"f"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::PREFIX, snippet_spec_, document,
      section_mask);

  // Check the snippets. 'f' should match prefix-enabled property 'subject', but
  // not exact-only property 'body'
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "subject", 0), Eq("subject foo"));
  EXPECT_THAT(GetMatch(document, snippet, "subject", 0), Eq("foo"));
}

TEST_F(SnippetRetrieverTest, ExactSnippeting) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body", "Only a fool would match this content.")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"f"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);

  // Check the snippets
  EXPECT_THAT(snippet.entries(), IsEmpty());
}

TEST_F(SnippetRetrieverTest, SimpleSnippetingNoWindowing) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body", "Only a fool would match this content.")
          .Build();

  snippet_spec_.set_max_window_bytes(0);

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"foo"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);

  // Check the snippets
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "subject", 0), IsEmpty());
  EXPECT_THAT(GetMatch(document, snippet, "subject", 0), Eq("foo"));
}

TEST_F(SnippetRetrieverTest, SnippetingMultipleMatches) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body",
                             "Concerning the subject of foo, we need to begin "
                             "considering our options regarding body bar.")
          .Build();
  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"foo", "bar"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::PREFIX, snippet_spec_, document,
      section_mask);

  // Check the snippets
  EXPECT_THAT(snippet.entries(), SizeIs(2));
  EXPECT_THAT(GetWindow(document, snippet, "subject", 0), Eq("subject foo"));
  EXPECT_THAT(GetMatch(document, snippet, "subject", 0), Eq("foo"));
  EXPECT_THAT(
      GetWindow(document, snippet, "body", 0),
      Eq("Concerning the subject of foo, we need to begin considering"));
  EXPECT_THAT(GetMatch(document, snippet, "body", 0), Eq("foo"));
  EXPECT_THAT(GetWindow(document, snippet, "body", 1),
              Eq("our options regarding body bar."));
  EXPECT_THAT(GetMatch(document, snippet, "body", 1), Eq("bar"));
}

TEST_F(SnippetRetrieverTest, SnippetingMultipleMatchesSectionRestrict) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body",
                             "Concerning the subject of foo, we need to begin "
                             "considering our options regarding body bar.")
          .Build();
  // Section 1 "subject" is not in the section_mask, so no snippet information
  // from that section should be returned by the SnippetRetriever.
  SectionIdMask section_mask = 0b00000001;
  SectionRestrictQueryTermsMap query_terms{{"", {"foo", "bar"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::PREFIX, snippet_spec_, document,
      section_mask);

  // Check the snippets
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(
      GetWindow(document, snippet, "body", 0),
      Eq("Concerning the subject of foo, we need to begin considering"));
  EXPECT_THAT(GetMatch(document, snippet, "body", 0), Eq("foo"));
  EXPECT_THAT(GetWindow(document, snippet, "body", 1),
              Eq("our options regarding body bar."));
  EXPECT_THAT(GetMatch(document, snippet, "body", 1), Eq("bar"));
}

TEST_F(SnippetRetrieverTest, SnippetingMultipleMatchesSectionRestrictedTerm) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body",
                             "Concerning the subject of foo, we need to begin "
                             "considering our options regarding body bar.")
          .Build();
  SectionIdMask section_mask = 0b00000011;
  // "subject" should match in both sections, but "foo" is restricted to "body"
  // so it should only match in the 'body' section and not the 'subject'
  // section.
  SectionRestrictQueryTermsMap query_terms{{"", {"subject"}},
                                           {"body", {"foo"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::PREFIX, snippet_spec_, document,
      section_mask);

  // Check the snippets
  EXPECT_THAT(snippet.entries(), SizeIs(2));
  // 'subject' section should only have the one match for "subject".
  EXPECT_THAT(GetWindow(document, snippet, "subject", 0), Eq("subject foo"));
  EXPECT_THAT(GetMatch(document, snippet, "subject", 0), Eq("subject"));
  EXPECT_THAT(GetWindow(document, snippet, "subject", 1), IsEmpty());
  EXPECT_THAT(GetMatch(document, snippet, "subject", 1), IsEmpty());

  // 'body' section should have matches for "subject" and "foo".
  EXPECT_THAT(GetWindow(document, snippet, "body", 0),
              Eq("Concerning the subject of foo, we need to begin"));
  EXPECT_THAT(GetMatch(document, snippet, "body", 0), Eq("subject"));
  EXPECT_THAT(
      GetWindow(document, snippet, "body", 1),
      Eq("Concerning the subject of foo, we need to begin considering"));
  EXPECT_THAT(GetMatch(document, snippet, "body", 1), Eq("foo"));
}

TEST_F(SnippetRetrieverTest, SnippetingMultipleMatchesOneMatchPerProperty) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body",
                             "Concerning the subject of foo, we need to begin "
                             "considering our options regarding body bar.")
          .Build();

  snippet_spec_.set_num_matches_per_property(1);

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"foo", "bar"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::PREFIX, snippet_spec_, document,
      section_mask);

  // Check the snippets
  EXPECT_THAT(snippet.entries(), SizeIs(2));
  EXPECT_THAT(GetWindow(document, snippet, "subject", 0), Eq("subject foo"));
  EXPECT_THAT(GetMatch(document, snippet, "subject", 0), Eq("foo"));
  EXPECT_THAT(
      GetWindow(document, snippet, "body", 0),
      Eq("Concerning the subject of foo, we need to begin considering"));
  EXPECT_THAT(GetMatch(document, snippet, "body", 0), Eq("foo"));
  EXPECT_THAT(GetWindow(document, snippet, "body", 1), IsEmpty());
  EXPECT_THAT(GetMatch(document, snippet, "body", 1), IsEmpty());
}

TEST_F(SnippetRetrieverTest, PrefixSnippetingNormalization) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "MDI team")
          .AddStringProperty("body", "Some members are in Zürich.")
          .Build();
  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"md"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::PREFIX, snippet_spec_, document,
      section_mask);

  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "subject", 0), Eq("MDI team"));
  EXPECT_THAT(GetMatch(document, snippet, "subject", 0), Eq("MDI"));
}

TEST_F(SnippetRetrieverTest, ExactSnippetingNormalization) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "MDI team")
          .AddStringProperty("body", "Some members are in Zürich.")
          .Build();

  SectionIdMask section_mask = 0b00000011;
  SectionRestrictQueryTermsMap query_terms{{"", {"zurich"}}};
  SnippetProto snippet = snippet_retriever_->RetrieveSnippet(
      query_terms, TermMatchType::EXACT_ONLY, snippet_spec_, document,
      section_mask);

  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(GetWindow(document, snippet, "body", 0),
              Eq("Some members are in Zürich."));
  EXPECT_THAT(GetMatch(document, snippet, "body", 0), Eq("Zürich"));
}

}  // namespace

}  // namespace lib
}  // namespace icing
