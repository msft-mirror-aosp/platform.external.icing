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

#include "icing/index/index.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/legacy/index/icing-mock-filesystem.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/random-string.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::IsEmpty;
using ::testing::IsTrue;
using ::testing::Ne;
using ::testing::NiceMock;
using ::testing::Not;
using ::testing::SizeIs;
using ::testing::Test;
using ::testing::UnorderedElementsAre;

class IndexTest : public Test {
 protected:
  void SetUp() override {
    index_dir_ = GetTestTempDir() + "/index_test/";
    Index::Options options(index_dir_, /*index_merge_size=*/1024 * 1024);
    ICING_ASSERT_OK_AND_ASSIGN(index_, Index::Create(options, &filesystem_));
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(index_dir_.c_str());
  }

  std::unique_ptr<Index> index_;
  std::string index_dir_;
  IcingFilesystem filesystem_;
};

constexpr DocumentId kDocumentId0 = 0;
constexpr DocumentId kDocumentId1 = 1;
constexpr DocumentId kDocumentId2 = 2;
constexpr SectionId kSectionId2 = 2;
constexpr SectionId kSectionId3 = 3;

std::vector<DocHitInfo> GetHits(std::unique_ptr<DocHitInfoIterator> iterator) {
  std::vector<DocHitInfo> infos;
  while (iterator->Advance().ok()) {
    infos.push_back(iterator->doc_hit_info());
  }
  return infos;
}

MATCHER_P2(EqualsDocHitInfo, document_id, sections, "") {
  const DocHitInfo& actual = arg;
  SectionIdMask section_mask = kSectionIdMaskNone;
  for (SectionId section : sections) {
    section_mask |= 1U << section;
  }
  *result_listener << "actual is {document_id=" << actual.document_id()
                   << ", section_mask=" << actual.hit_section_ids_mask()
                   << "}, but expected was {document_id=" << document_id
                   << ", section_mask=" << section_mask << "}.";
  return actual.document_id() == document_id &&
         actual.hit_section_ids_mask() == section_mask;
}

MATCHER_P2(EqualsTermMetadata, content, hit_count, "") {
  const TermMetadata& actual = arg;
  *result_listener << "actual is {content=" << actual.content
                   << ", hit_count=" << actual.hit_count
                   << "}, but expected was {content=" << content
                   << ", hit_count=" << hit_count << "}.";
  return actual.content == content && actual.hit_count == hit_count;
}

TEST_F(IndexTest, CreationWithNullPointerShouldFail) {
  Index::Options options(index_dir_, /*index_merge_size=*/1024 * 1024);
  EXPECT_THAT(Index::Create(options, /*filesystem=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(IndexTest, EmptyIndex) {
  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->Advance(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->Advance(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kInvalidDocumentId));
}

TEST_F(IndexTest, AdvancePastEnd) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("bar", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->Advance(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->doc_hit_info(),
              EqualsDocHitInfo(kInvalidDocumentId, std::vector<SectionId>()));

  ICING_ASSERT_OK_AND_ASSIGN(
      itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->Advance(), IsOk());
  EXPECT_THAT(itr->Advance(),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  EXPECT_THAT(itr->doc_hit_info(),
              EqualsDocHitInfo(kInvalidDocumentId, std::vector<SectionId>()));
}

TEST_F(IndexTest, SingleHitSingleTermIndex) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, SingleHitMultiTermIndex) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  EXPECT_THAT(edit.AddHit("bar"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, NoHitMultiTermIndex) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  EXPECT_THAT(edit.AddHit("bar"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("baz", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->Advance(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, MultiHitMultiTermIndex) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());

  edit = index_->Edit(kDocumentId1, kSectionId2, TermMatchType::EXACT_ONLY,
                      /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("bar"), IsOk());

  edit = index_->Edit(kDocumentId2, kSectionId3, TermMatchType::EXACT_ONLY,
                      /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(
      GetHits(std::move(itr)),
      ElementsAre(
          EqualsDocHitInfo(kDocumentId2, std::vector<SectionId>{kSectionId3}),
          EqualsDocHitInfo(kDocumentId0, std::vector<SectionId>{kSectionId2})));
  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId2));
}

TEST_F(IndexTest, MultiHitSectionRestrict) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());

  edit = index_->Edit(kDocumentId1, kSectionId3, TermMatchType::EXACT_ONLY,
                      /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());

  // Assert
  SectionIdMask desired_section = 1U << kSectionId2;
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", desired_section, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId1));
}

TEST_F(IndexTest, SingleHitDedupeIndex) {
  Crc32 empty_crc = index_->ComputeChecksum();
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  Crc32 first_hit_crc = index_->ComputeChecksum();
  EXPECT_THAT(first_hit_crc.Get(), Ne(empty_crc.Get()));
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  Crc32 second_hit_crc = index_->ComputeChecksum();
  EXPECT_THAT(second_hit_crc.Get(), Eq(first_hit_crc.Get()));

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, PrefixHit) {
  // Act
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("fool"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::PREFIX));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, MultiPrefixHit) {
  // Act
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("fool"), IsOk());

  edit = index_->Edit(kDocumentId1, kSectionId3, TermMatchType::EXACT_ONLY,
                      /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::PREFIX));
  EXPECT_THAT(
      GetHits(std::move(itr)),
      ElementsAre(
          EqualsDocHitInfo(kDocumentId1, std::vector<SectionId>{kSectionId3}),
          EqualsDocHitInfo(kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId1));
}

TEST_F(IndexTest, NoExactHitInPrefixQuery) {
  // Act
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("fool"), IsOk());

  edit = index_->Edit(kDocumentId1, kSectionId3, TermMatchType::PREFIX,
                      /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::PREFIX));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId1, std::vector<SectionId>{kSectionId3})));
  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId1));
}

TEST_F(IndexTest, PrefixHitDedupe) {
  // Act
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());
  ASSERT_THAT(edit.AddHit("fool"), IsOk());

  // Assert
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::PREFIX));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));
  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, PrefixToString) {
  SectionIdMask id_mask = (1U << kSectionId2) | (1U << kSectionId3);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", id_mask, TermMatchType::PREFIX));
  EXPECT_THAT(itr->ToString(), Eq("0000000000001100:foo*"));

  ICING_ASSERT_OK_AND_ASSIGN(itr, index_->GetIterator("foo", kSectionIdMaskAll,
                                                      TermMatchType::PREFIX));
  EXPECT_THAT(itr->ToString(), Eq("1111111111111111:foo*"));

  ICING_ASSERT_OK_AND_ASSIGN(itr, index_->GetIterator("foo", kSectionIdMaskNone,
                                                      TermMatchType::PREFIX));
  EXPECT_THAT(itr->ToString(), Eq("0000000000000000:foo*"));
}

TEST_F(IndexTest, ExactToString) {
  SectionIdMask id_mask = (1U << kSectionId2) | (1U << kSectionId3);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("foo", id_mask, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->ToString(), Eq("0000000000001100:foo"));

  ICING_ASSERT_OK_AND_ASSIGN(
      itr,
      index_->GetIterator("foo", kSectionIdMaskAll, TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->ToString(), Eq("1111111111111111:foo"));

  ICING_ASSERT_OK_AND_ASSIGN(itr,
                             index_->GetIterator("foo", kSectionIdMaskNone,
                                                 TermMatchType::EXACT_ONLY));
  EXPECT_THAT(itr->ToString(), Eq("0000000000000000:foo"));
}

TEST_F(IndexTest, NonAsciiTerms) {
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("こんにちは"), IsOk());
  ASSERT_THAT(edit.AddHit("あなた"), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("こんに", kSectionIdMaskAll, TermMatchType::PREFIX));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  ICING_ASSERT_OK_AND_ASSIGN(itr,
                             index_->GetIterator("あなた", kSectionIdMaskAll,
                                                 TermMatchType::EXACT_ONLY));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));
}

TEST_F(IndexTest, FullIndex) {
  // Make a smaller index so that it's easier to fill up.
  Index::Options options(index_dir_, /*index_merge_size=*/1024);
  ICING_ASSERT_OK_AND_ASSIGN(index_, Index::Create(options, &filesystem_));
  std::default_random_engine random;
  libtextclassifier3::Status status = libtextclassifier3::Status::OK;
  constexpr int kTokenSize = 5;
  DocumentId document_id = 0;
  std::vector<std::string> query_terms;
  while (status.ok()) {
    for (int i = 0; i < 100; ++i) {
      Index::Editor edit =
          index_->Edit(document_id, kSectionId2, TermMatchType::EXACT_ONLY,
                       /*namespace_id=*/0);
      std::string term = RandomString(kAlNumAlphabet, kTokenSize, &random);
      status = edit.AddHit(term.c_str());
      if (i % 50 == 0) {
        // Remember one out of every fifty terms to query for later.
        query_terms.push_back(std::move(term));
      }
      if (!status.ok()) {
        break;
      }
    }
    ++document_id;
  }

  // Assert
  // Adding more hits should fail.
  Index::Editor edit =
      index_->Edit(document_id + 1, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  EXPECT_THAT(edit.AddHit("bar"),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  EXPECT_THAT(edit.AddHit("baz"),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));

  for (const std::string& term : query_terms) {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocHitInfoIterator> itr,
        index_->GetIterator(term.c_str(), kSectionIdMaskAll,
                            TermMatchType::EXACT_ONLY));
    // Each query term should contain at least one hit - there may have been
    // other hits for this term that were added.
    EXPECT_THAT(itr->Advance(), IsOk());
  }
  EXPECT_THAT(index_->last_added_document_id(), Eq(document_id - 1));
}

TEST_F(IndexTest, IndexCreateIOFailure) {
  // Create the index with mock filesystem. By default, Mock will return false,
  // so the first attempted file operation will fail.
  NiceMock<IcingMockFilesystem> mock_filesystem;
  Index::Options options(index_dir_, /*index_merge_size=*/1024 * 1024);
  EXPECT_THAT(Index::Create(options, &mock_filesystem),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(IndexTest, IndexCreateCorruptionFailure) {
  // Add some content to the index
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());
  ASSERT_THAT(edit.AddHit("bar"), IsOk());

  // Close the index.
  index_.reset();

  // Corrrupt the index file.
  std::string hit_buffer_filename = index_dir_ + "/idx/lite.hb";
  ScopedFd sfd(filesystem_.OpenForWrite(hit_buffer_filename.c_str()));
  ASSERT_THAT(sfd.is_valid(), IsTrue());

  constexpr std::string_view kCorruptBytes = "ffffffffffffffffffffff";
  // The first page of the hit_buffer is taken up by the header. Overwrite the
  // first page of content.
  constexpr int kHitBufferStartOffset = 4096;
  ASSERT_THAT(filesystem_.PWrite(sfd.get(), kHitBufferStartOffset,
                                 kCorruptBytes.data(), kCorruptBytes.length()),
              IsTrue());

  // Recreate the index.
  Index::Options options(index_dir_, /*index_merge_size=*/1024 * 1024);
  EXPECT_THAT(Index::Create(options, &filesystem_),
              StatusIs(libtextclassifier3::StatusCode::DATA_LOSS));
}

TEST_F(IndexTest, IndexPersistence) {
  // Add some content to the index
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());
  ASSERT_THAT(edit.AddHit("bar"), IsOk());
  EXPECT_THAT(index_->PersistToDisk(), IsOk());

  // Close the index.
  index_.reset();

  // Recreate the index.
  Index::Options options(index_dir_, /*index_merge_size=*/1024 * 1024);
  ICING_ASSERT_OK_AND_ASSIGN(index_, Index::Create(options, &filesystem_));

  // Check that the hits are present.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIterator> itr,
      index_->GetIterator("f", kSectionIdMaskAll, TermMatchType::PREFIX));
  EXPECT_THAT(GetHits(std::move(itr)),
              ElementsAre(EqualsDocHitInfo(
                  kDocumentId0, std::vector<SectionId>{kSectionId2})));

  EXPECT_THAT(index_->last_added_document_id(), Eq(kDocumentId0));
}

TEST_F(IndexTest, InvalidHitBufferSize) {
  Index::Options options(
      index_dir_, /*index_merge_size=*/std::numeric_limits<uint32_t>::max());
  EXPECT_THAT(Index::Create(options, &filesystem_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IndexTest, ComputeChecksumSameBetweenCalls) {
  // Add some content to the index.
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());

  Crc32 checksum = index_->ComputeChecksum();
  // Calling it again shouldn't change the checksum
  EXPECT_THAT(index_->ComputeChecksum(), Eq(checksum));
}

TEST_F(IndexTest, ComputeChecksumSameAcrossInstances) {
  // Add some content to the index.
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());

  Crc32 checksum = index_->ComputeChecksum();

  // Recreate the index, checksum should still be the same across instances
  index_.reset();
  Index::Options options(index_dir_, /*index_merge_size=*/1024 * 1024);
  ICING_ASSERT_OK_AND_ASSIGN(index_, Index::Create(options, &filesystem_));

  EXPECT_THAT(index_->ComputeChecksum(), Eq(checksum));
}

TEST_F(IndexTest, ComputeChecksumChangesOnModification) {
  // Add some content to the index.
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  ASSERT_THAT(edit.AddHit("foo"), IsOk());

  Crc32 checksum = index_->ComputeChecksum();

  // Modifying the index changes the checksum;
  EXPECT_THAT(edit.AddHit("bar"), IsOk());

  EXPECT_THAT(index_->ComputeChecksum(), Not(Eq(checksum)));
}

TEST_F(IndexTest, FindTermByPrefixShouldReturnEmpty) {
  Index::Editor edit = index_->Edit(kDocumentId0, kSectionId2,
                                    TermMatchType::PREFIX, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("fool"), IsOk());

  EXPECT_THAT(index_->FindTermsByPrefix(/*prefix=*/"foo", /*namespace_ids=*/{0},
                                        /*num_to_return=*/0),
              IsOkAndHolds(IsEmpty()));

  EXPECT_THAT(index_->FindTermsByPrefix(/*prefix=*/"foo", /*namespace_ids=*/{0},
                                        /*num_to_return=*/-1),
              IsOkAndHolds(IsEmpty()));
}

TEST_F(IndexTest, FindTermByPrefixShouldReturnCorrectResult) {
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  EXPECT_THAT(edit.AddHit("bar"), IsOk());

  // "b" should only match "bar" but not "foo".
  EXPECT_THAT(index_->FindTermsByPrefix(/*prefix=*/"b", /*namespace_ids=*/{0},
                                        /*num_to_return=*/10),
              IsOkAndHolds(UnorderedElementsAre(EqualsTermMetadata("bar", 1))));
}

TEST_F(IndexTest, FindTermByPrefixShouldRespectNumToReturn) {
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("fo"), IsOk());
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  EXPECT_THAT(edit.AddHit("fool"), IsOk());

  // We have 3 results but only 2 should be returned.
  EXPECT_THAT(index_->FindTermsByPrefix(/*prefix=*/"f", /*namespace_ids=*/{0},
                                        /*num_to_return=*/2),
              IsOkAndHolds(SizeIs(2)));
}

TEST_F(IndexTest, FindTermByPrefixShouldReturnTermsInOneNamespace) {
  Index::Editor edit1 =
      index_->Edit(kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/0);
  EXPECT_THAT(edit1.AddHit("fo"), IsOk());
  EXPECT_THAT(edit1.AddHit("foo"), IsOk());

  Index::Editor edit2 =
      index_->Edit(kDocumentId1, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/1);
  EXPECT_THAT(edit2.AddHit("fool"), IsOk());

  // namespace with id 0 has 2 results.
  EXPECT_THAT(index_->FindTermsByPrefix(/*prefix=*/"f", /*namespace_ids=*/{0},
                                        /*num_to_return=*/10),
              IsOkAndHolds(UnorderedElementsAre(EqualsTermMetadata("fo", 1),
                                                EqualsTermMetadata("foo", 1))));

  // namespace with id 1 has 1 result.
  EXPECT_THAT(
      index_->FindTermsByPrefix(/*prefix=*/"f", /*namespace_ids=*/{1},
                                /*num_to_return=*/10),
      IsOkAndHolds(UnorderedElementsAre(EqualsTermMetadata("fool", 1))));
}

TEST_F(IndexTest, FindTermByPrefixShouldReturnTermsInMultipleNamespaces) {
  Index::Editor edit1 =
      index_->Edit(kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/0);
  EXPECT_THAT(edit1.AddHit("fo"), IsOk());

  Index::Editor edit2 =
      index_->Edit(kDocumentId1, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/1);
  EXPECT_THAT(edit2.AddHit("foo"), IsOk());

  Index::Editor edit3 =
      index_->Edit(kDocumentId2, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/2);
  EXPECT_THAT(edit3.AddHit("fool"), IsOk());

  // Should return "foo" and "fool" which are in namespaces with ids 1 and 2.
  EXPECT_THAT(
      index_->FindTermsByPrefix(/*prefix=*/"f", /*namespace_ids=*/{1, 2},
                                /*num_to_return=*/10),
      IsOkAndHolds(UnorderedElementsAre(EqualsTermMetadata("foo", 1),
                                        EqualsTermMetadata("fool", 1))));
}

TEST_F(IndexTest, FindTermByPrefixShouldReturnTermsInAllNamespaces) {
  Index::Editor edit1 =
      index_->Edit(kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/0);
  EXPECT_THAT(edit1.AddHit("fo"), IsOk());

  Index::Editor edit2 =
      index_->Edit(kDocumentId1, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/1);
  EXPECT_THAT(edit2.AddHit("foo"), IsOk());

  Index::Editor edit3 =
      index_->Edit(kDocumentId2, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/2);
  EXPECT_THAT(edit3.AddHit("fool"), IsOk());

  // Should return "fo", "foo" and "fool" across all namespaces.
  EXPECT_THAT(index_->FindTermsByPrefix(/*prefix=*/"f", /*namespace_ids=*/{},
                                        /*num_to_return=*/10),
              IsOkAndHolds(UnorderedElementsAre(
                  EqualsTermMetadata("fo", 1), EqualsTermMetadata("foo", 1),
                  EqualsTermMetadata("fool", 1))));
}

TEST_F(IndexTest, FindTermByPrefixShouldReturnCorrectHitCount) {
  Index::Editor edit1 =
      index_->Edit(kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/0);
  EXPECT_THAT(edit1.AddHit("foo"), IsOk());
  EXPECT_THAT(edit1.AddHit("fool"), IsOk());

  Index::Editor edit2 =
      index_->Edit(kDocumentId1, kSectionId2, TermMatchType::EXACT_ONLY,
                   /*namespace_id=*/0);
  EXPECT_THAT(edit2.AddHit("fool"), IsOk());

  // 'foo' has 1 hit, 'fool' has 2 hits.
  EXPECT_THAT(
      index_->FindTermsByPrefix(/*prefix=*/"f", /*namespace_ids=*/{0},
                                /*num_to_return=*/10),
      IsOkAndHolds(UnorderedElementsAre(EqualsTermMetadata("foo", 1),
                                        EqualsTermMetadata("fool", 2))));
}

TEST_F(IndexTest, GetElementsSize) {
  // Check empty index.
  EXPECT_THAT(index_->GetElementsSize(), IsOkAndHolds(Eq(0)));

  // Add an element.
  Index::Editor edit = index_->Edit(
      kDocumentId0, kSectionId2, TermMatchType::EXACT_ONLY, /*namespace_id=*/0);
  EXPECT_THAT(edit.AddHit("foo"), IsOk());
  EXPECT_THAT(index_->GetElementsSize(), IsOkAndHolds(Gt(0)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
