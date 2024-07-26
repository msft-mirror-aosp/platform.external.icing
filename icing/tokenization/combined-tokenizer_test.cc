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

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/index.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/platform.h"
#include "icing/proto/schema.pb.h"
#include "icing/query/query-processor.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "icing/util/status-macros.h"
#include "unicode/uloc.h"
#include "icing/query/query-results.h"
#include "icing/query/query-terms.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

// This test exists to ensure that the different tokenizers treat different
// segments of text in the same manner.
class CombinedTokenizerTest : public ::testing::Test {
 protected:
  CombinedTokenizerTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        store_dir_(test_dir_ + "/store"),
        schema_store_dir_(test_dir_ + "/schema_store"),
        index_dir_(test_dir_ + "/index"),
        numeric_index_dir_(test_dir_ + "/numeric_index"),
        embedding_index_dir_(test_dir_ + "/embedding_index") {}

  void SetUp() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(index_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpICUDataFile(
              GetTestFilePath("icing/icu.dat")));
    }
    jni_cache_ = GetTestJniCache();

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, store_dir_, &fake_clock_, schema_store_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*namespace_id_fingerprint=*/false, /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/false,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDeflateCompressionLevel,
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
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_));

    language_segmenter_factory::SegmenterOptions segmenter_options(
        ULOC_US, jni_cache_.get());
    ICING_ASSERT_OK_AND_ASSIGN(
        lang_segmenter_,
        language_segmenter_factory::Create(std::move(segmenter_options)));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/1000));
    ICING_ASSERT_OK_AND_ASSIGN(
        query_processor_,
        QueryProcessor::Create(index_.get(), numeric_index_.get(),
                               embedding_index_.get(), lang_segmenter_.get(),
                               normalizer_.get(), document_store_.get(),
                               schema_store_.get(), &fake_clock_));
  }

  libtextclassifier3::StatusOr<std::vector<std::string>> GetQueryTerms(
      std::string_view query) {
    SearchSpecProto search_spec;
    search_spec.set_query(std::string(query));
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    ICING_ASSIGN_OR_RETURN(
        QueryResults parsed_query,
        query_processor_->ParseSearch(
            search_spec, ScoringSpecProto::RankingStrategy::NONE,
            /*current_time_ms=*/0, /*search_stats=*/nullptr));

    std::vector<std::string> query_terms;
    const SectionRestrictQueryTermsMap& query_terms_map =
        parsed_query.query_terms;
    for (const auto& [section_id, terms] : query_terms_map) {
      std::copy(terms.begin(), terms.end(), std::back_inserter(query_terms));
    }
    return query_terms;
  }

  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string store_dir_;
  const std::string schema_store_dir_;

  IcingFilesystem icing_filesystem_;
  const std::string index_dir_;
  const std::string numeric_index_dir_;
  const std::string embedding_index_dir_;

  std::unique_ptr<const JniCache> jni_cache_;
  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<QueryProcessor> query_processor_;

  std::unique_ptr<Index> index_;
  std::unique_ptr<NumericIndex<int64_t>> numeric_index_;
  std::unique_ptr<EmbeddingIndex> embedding_index_;
  std::unique_ptr<Normalizer> normalizer_;
  FakeClock fake_clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
};

std::vector<std::string> GetTokenTerms(const std::vector<Token>& tokens) {
  std::vector<std::string> terms;
  terms.reserve(tokens.size());
  for (const Token& token : tokens) {
    if (token.type == Token::Type::REGULAR) {
      terms.push_back(std::string(token.text));
    }
  }
  return terms;
}

}  // namespace

TEST_F(CombinedTokenizerTest, SpecialCharacters) {
  const std::string_view kText = "😊 Hello! Goodbye?";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Tokenizer> indexing_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, lang_segmenter_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                             indexing_tokenizer->TokenizeAll(kText));
  std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
  EXPECT_THAT(indexing_terms, ElementsAre("😊", "Hello", "Goodbye"));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                             GetQueryTerms(kText));
  // NOTE: The query parser will also normalize query terms
  EXPECT_THAT(query_terms, UnorderedElementsAre("😊", "hello", "goodbye"));
}

TEST_F(CombinedTokenizerTest, Parentheses) {
  const std::string_view kText = "((paren1)(paren2) (last paren))";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Tokenizer> indexing_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, lang_segmenter_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                             indexing_tokenizer->TokenizeAll(kText));
  std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
  EXPECT_THAT(indexing_terms, ElementsAre("paren1", "paren2", "last", "paren"));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                             GetQueryTerms(kText));
  EXPECT_THAT(query_terms,
              UnorderedElementsAre("paren1", "paren2", "last", "paren"));
}

TEST_F(CombinedTokenizerTest, Negation) {
  const std::string_view kText = "-foo -bar -baz";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Tokenizer> indexing_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, lang_segmenter_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                             indexing_tokenizer->TokenizeAll(kText));
  std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
  EXPECT_THAT(indexing_terms, ElementsAre("foo", "bar", "baz"));

  const std::string_view kQueryText = "\\-foo \\-bar \\-baz";
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                             GetQueryTerms(kQueryText));
  EXPECT_THAT(query_terms,
              UnorderedElementsAre("foo", "bar", "baz"));
}

// TODO(b/254874614): Handle colon word breaks in ICU 72+
TEST_F(CombinedTokenizerTest, Colons) {
  const std::string_view kText = ":foo: :bar baz:";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Tokenizer> indexing_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, lang_segmenter_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                             indexing_tokenizer->TokenizeAll(kText));
  std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
  EXPECT_THAT(indexing_terms, ElementsAre("foo", "bar", "baz"));

  const std::string_view kQueryText = "\\:foo\\: \\:bar baz\\:";
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                             GetQueryTerms(kQueryText));
  EXPECT_THAT(query_terms, UnorderedElementsAre("foo", "bar", "baz"));
}

// TODO(b/254874614): Handle colon word breaks in ICU 72+
TEST_F(CombinedTokenizerTest, ColonsPropertyRestricts) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Tokenizer> indexing_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, lang_segmenter_.get()));

  if (GetIcuTokenizationVersion() >= 72) {
    // In ICU 72+ and above, ':' are no longer considered word connectors.
    constexpr std::string_view kText = "foo:bar";
    ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                               indexing_tokenizer->TokenizeAll(kText));
    std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
    EXPECT_THAT(indexing_terms, ElementsAre("foo", "bar"));

    const std::string_view kQueryText = "foo\\:bar";
    ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                              GetQueryTerms(kQueryText));
    EXPECT_THAT(query_terms, UnorderedElementsAre("foo", "bar"));

    constexpr std::string_view kText2 = "foo:bar:baz";
    ICING_ASSERT_OK_AND_ASSIGN(indexing_tokens,
                               indexing_tokenizer->TokenizeAll(kText2));
    indexing_terms = GetTokenTerms(indexing_tokens);
    EXPECT_THAT(indexing_terms, ElementsAre("foo", "bar", "baz"));

    const std::string_view kQueryText2 = "foo\\:bar\\:baz";
    ICING_ASSERT_OK_AND_ASSIGN(query_terms,
                              GetQueryTerms(kQueryText2));
    EXPECT_THAT(query_terms, UnorderedElementsAre("foo", "bar", "baz"));
  } else {
    constexpr std::string_view kText = "foo:bar";
    constexpr std::string_view kQueryText = "foo\\:bar";
    ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                               indexing_tokenizer->TokenizeAll(kText));
    std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
    EXPECT_THAT(indexing_terms, ElementsAre("foo:bar"));

    ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                               GetQueryTerms(kQueryText));
    EXPECT_THAT(query_terms, UnorderedElementsAre("foo:bar"));

    constexpr std::string_view kText2 = "foo:bar:baz";
    constexpr std::string_view kQueryText2 = "foo\\:bar\\:baz";
    ICING_ASSERT_OK_AND_ASSIGN(indexing_tokens,
                               indexing_tokenizer->TokenizeAll(kText2));
    indexing_terms = GetTokenTerms(indexing_tokens);
    EXPECT_THAT(indexing_terms, ElementsAre("foo:bar:baz"));

    ICING_ASSERT_OK_AND_ASSIGN(query_terms, GetQueryTerms(kQueryText2));
    EXPECT_THAT(query_terms, UnorderedElementsAre("foo:bar:baz"));
  }
}

TEST_F(CombinedTokenizerTest, Punctuation) {
  const std::string_view kText = "Who? What!? Why & How";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Tokenizer> indexing_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, lang_segmenter_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Token> indexing_tokens,
                             indexing_tokenizer->TokenizeAll(kText));
  std::vector<std::string> indexing_terms = GetTokenTerms(indexing_tokens);
  EXPECT_THAT(indexing_terms, ElementsAre("Who", "What", "Why", "How"));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string> query_terms,
                             GetQueryTerms(kText));
  // NOTE: The query parser will also normalize query terms
  EXPECT_THAT(query_terms, UnorderedElementsAre("who", "what", "why", "how"));
}

}  // namespace lib
}  // namespace icing
