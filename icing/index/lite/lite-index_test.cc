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

#include "icing/index/lite/lite-index.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/lite/doc-hit-info-iterator-term-lite.h"
#include "icing/index/term-id-codec.h"
#include "icing/legacy/index/icing-mock-filesystem.h"
#include "icing/schema/section.h"
#include "icing/store/suggestion-result-checker.h"
#include "icing/testing/always-false-suggestion-result-checker-impl.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::SizeIs;

class LiteIndexTest : public testing::Test {
 protected:
  void SetUp() override {
    index_dir_ = GetTestTempDir() + "/test_dir";
    ASSERT_TRUE(filesystem_.CreateDirectoryRecursively(index_dir_.c_str()));

    std::string lite_index_file_name = index_dir_ + "/test_file.lite-idx.index";
    LiteIndex::Options options(lite_index_file_name,
                               /*hit_buffer_want_merge_bytes=*/1024 * 1024);
    ICING_ASSERT_OK_AND_ASSIGN(lite_index_,
                               LiteIndex::Create(options, &icing_filesystem_));

    ICING_ASSERT_OK_AND_ASSIGN(
        term_id_codec_,
        TermIdCodec::Create(
            IcingDynamicTrie::max_value_index(IcingDynamicTrie::Options()),
            IcingDynamicTrie::max_value_index(options.lexicon_options)));
  }

  void TearDown() override {
    ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(index_dir_.c_str()));
  }

  std::string index_dir_;
  Filesystem filesystem_;
  IcingFilesystem icing_filesystem_;
  std::unique_ptr<LiteIndex> lite_index_;
  std::unique_ptr<TermIdCodec> term_id_codec_;
};

constexpr NamespaceId kNamespace0 = 0;

TEST_F(LiteIndexTest, LiteIndexAppendHits) {
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t tvi,
      lite_index_->InsertTerm("foo", TermMatchType::PREFIX, kNamespace0));
  ICING_ASSERT_OK_AND_ASSIGN(uint32_t foo_term_id,
                             term_id_codec_->EncodeTvi(tvi, TviType::LITE));
  Hit doc_hit0(/*section_id=*/0, /*document_id=*/0, Hit::kDefaultTermFrequency,
               /*is_in_prefix_section=*/false);
  Hit doc_hit1(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
               /*is_in_prefix_section=*/false);
  ICING_ASSERT_OK(lite_index_->AddHit(foo_term_id, doc_hit0));
  ICING_ASSERT_OK(lite_index_->AddHit(foo_term_id, doc_hit1));

  std::vector<DocHitInfo> hits1;
  lite_index_->AppendHits(
      foo_term_id, kSectionIdMaskAll,
      /*only_from_prefix_sections=*/false,
      SuggestionScoringSpecProto::SuggestionRankingStrategy::DOCUMENT_COUNT,
      /*namespace_checker=*/nullptr, &hits1);
  EXPECT_THAT(hits1, SizeIs(1));
  EXPECT_THAT(hits1.back().document_id(), Eq(0));
  // Check that the hits are coming from section 0 and section 1.
  EXPECT_THAT(hits1.back().hit_section_ids_mask(), Eq(0b11));

  std::vector<DocHitInfo> hits2;
  AlwaysFalseSuggestionResultCheckerImpl always_false_suggestion_result_checker;
  lite_index_->AppendHits(
      foo_term_id, kSectionIdMaskAll,
      /*only_from_prefix_sections=*/false,
      SuggestionScoringSpecProto::SuggestionRankingStrategy::DOCUMENT_COUNT,
      &always_false_suggestion_result_checker, &hits2);
  // Check that no hits are returned because they get skipped by the namespace
  // checker.
  EXPECT_THAT(hits2, IsEmpty());
}

TEST_F(LiteIndexTest, LiteIndexIterator) {
  const std::string term = "foo";
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t tvi,
      lite_index_->InsertTerm(term, TermMatchType::PREFIX, kNamespace0));
  ICING_ASSERT_OK_AND_ASSIGN(uint32_t foo_term_id,
                             term_id_codec_->EncodeTvi(tvi, TviType::LITE));
  Hit doc_hit0(/*section_id=*/0, /*document_id=*/0, 3,
               /*is_in_prefix_section=*/false);
  Hit doc_hit1(/*section_id=*/1, /*document_id=*/0, 5,
               /*is_in_prefix_section=*/false);
  Hit::TermFrequencyArray doc0_term_frequencies{3, 5};
  Hit doc_hit2(/*section_id=*/1, /*document_id=*/1, 7,
               /*is_in_prefix_section=*/false);
  Hit doc_hit3(/*section_id=*/2, /*document_id=*/1, 11,
               /*is_in_prefix_section=*/false);
  Hit::TermFrequencyArray doc1_term_frequencies{0, 7, 11};
  ICING_ASSERT_OK(lite_index_->AddHit(foo_term_id, doc_hit0));
  ICING_ASSERT_OK(lite_index_->AddHit(foo_term_id, doc_hit1));
  ICING_ASSERT_OK(lite_index_->AddHit(foo_term_id, doc_hit2));
  ICING_ASSERT_OK(lite_index_->AddHit(foo_term_id, doc_hit3));

  std::unique_ptr<DocHitInfoIteratorTermLiteExact> iter =
      std::make_unique<DocHitInfoIteratorTermLiteExact>(
          term_id_codec_.get(), lite_index_.get(), term, kSectionIdMaskAll,
          /*need_hit_term_frequency=*/true);

  ASSERT_THAT(iter->Advance(), IsOk());
  EXPECT_THAT(iter->doc_hit_info().document_id(), Eq(1));
  EXPECT_THAT(iter->doc_hit_info().hit_section_ids_mask(), Eq(0b110));
  std::vector<TermMatchInfo> matched_terms_stats;
  iter->PopulateMatchedTermsStats(&matched_terms_stats);
  ASSERT_THAT(matched_terms_stats, SizeIs(1));
  EXPECT_EQ(matched_terms_stats.at(0).term, term);
  EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, 0b110);
  EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
              ElementsAreArray(doc1_term_frequencies));

  ASSERT_THAT(iter->Advance(), IsOk());
  EXPECT_THAT(iter->doc_hit_info().document_id(), Eq(0));
  EXPECT_THAT(iter->doc_hit_info().hit_section_ids_mask(), Eq(0b11));
  matched_terms_stats.clear();
  iter->PopulateMatchedTermsStats(&matched_terms_stats);
  ASSERT_THAT(matched_terms_stats, SizeIs(1));
  EXPECT_EQ(matched_terms_stats.at(0).term, term);
  EXPECT_EQ(matched_terms_stats.at(0).section_ids_mask, 0b11);
  EXPECT_THAT(matched_terms_stats.at(0).term_frequencies,
              ElementsAreArray(doc0_term_frequencies));
}

}  // namespace
}  // namespace lib
}  // namespace icing
