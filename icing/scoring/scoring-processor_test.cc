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

#include "icing/scoring/scoring-processor.h"

#include <cstdint>

#include "utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::Test;

class ScoringProcessorTest : public Test {
 protected:
  ScoringProcessorTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        doc_store_dir_(test_dir_ + "/doc_store"),
        schema_store_dir_(test_dir_ + "/schema_store") {}

  void SetUp() override {
    // Creates file directories
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(schema_store_,
                               SchemaStore::Create(&filesystem_, test_dir_));

    ICING_ASSERT_OK_AND_ASSIGN(
        document_store_,
        DocumentStore::Create(&filesystem_, doc_store_dir_, &fake_clock_,
                              schema_store_.get()));

    // Creates a simple email schema
    SchemaProto test_email_schema;
    auto type_config = test_email_schema.add_types();
    type_config->set_schema_type("email");
    auto subject = type_config->add_properties();
    subject->set_property_name("subject");
    subject->set_data_type(PropertyConfigProto::DataType::STRING);
    subject->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    ICING_ASSERT_OK(schema_store_->SetSchema(test_email_schema));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  DocumentStore* document_store() { return document_store_.get(); }

 private:
  const std::string test_dir_;
  const std::string doc_store_dir_;
  const std::string schema_store_dir_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<SchemaStore> schema_store_;
};

constexpr int kDefaultScore = 0;
constexpr int64_t kDefaultCreationTimestampSecs = 1571100001;

DocumentProto CreateDocument(const std::string& name_space,
                             const std::string& uri, int score,
                             int64_t creation_timestamp_secs) {
  return DocumentBuilder()
      .SetKey(name_space, uri)
      .SetSchema("email")
      .SetScore(score)
      .SetCreationTimestampSecs(creation_timestamp_secs)
      .Build();
}

libtextclassifier3::StatusOr<
    std::pair<std::vector<DocHitInfo>, std::vector<ScoredDocumentHit>>>
CreateAndInsertsDocumentsWithScores(DocumentStore* document_store,
                                    const std::vector<int>& scores) {
  std::vector<DocHitInfo> doc_hit_infos;
  std::vector<ScoredDocumentHit> scored_document_hits;
  for (int i = 0; i < scores.size(); i++) {
    ICING_ASSIGN_OR_RETURN(DocumentId document_id,
                           document_store->Put(CreateDocument(
                               "icing", "email/" + std::to_string(i),
                               scores.at(i), kDefaultCreationTimestampSecs)));
    doc_hit_infos.emplace_back(document_id);
    scored_document_hits.emplace_back(document_id, kSectionIdMaskNone,
                                      scores.at(i));
  }
  return std::pair(doc_hit_infos, scored_document_hits);
}

TEST_F(ScoringProcessorTest, FailToCreateOnInvalidRankingStrategy) {
  ScoringSpecProto spec_proto;
  EXPECT_THAT(ScoringProcessor::Create(spec_proto, document_store()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScoringProcessorTest, ShouldCreateInstance) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  ICING_EXPECT_OK(ScoringProcessor::Create(spec_proto, document_store()));
}

TEST_F(ScoringProcessorTest, ShouldHandleEmptyDocHitIterator) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Creates an empty DocHitInfoIterator
  std::vector<DocHitInfo> doc_hit_infos = {};
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/5),
              IsEmpty());
}

TEST_F(ScoringProcessorTest, ShouldHandleNonPositiveNumToReturn) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Sets up documents
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id1,
      document_store()->Put(CreateDocument("icing", "email/1", /*score=*/1,
                                           kDefaultCreationTimestampSecs)));
  DocHitInfo doc_hit_info1(document_id1);

  // Creates a dummy DocHitInfoIterator
  std::vector<DocHitInfo> doc_hit_infos = {doc_hit_info1};
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/-1),
              IsEmpty());

  doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/0),
              IsEmpty());
}

TEST_F(ScoringProcessorTest, ShouldRespectNumToReturn) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Sets up documents
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {1, 2, 3}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 3 results
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/2),
              SizeIs(2));

  doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/4),
              SizeIs(3));
}

TEST_F(ScoringProcessorTest, ShouldRankByDocumentScoreDesc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Sets up documents, guaranteed relationship:
  // document1 < document2 < document3
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {1, 2, 3}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);
  std::vector<ScoredDocumentHit> scored_document_hits =
      std::move(doc_hit_result_pair.second);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 3 results
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/3),
              ElementsAre(EqualsScoredDocumentHit(scored_document_hits.at(2)),
                          EqualsScoredDocumentHit(scored_document_hits.at(1)),
                          EqualsScoredDocumentHit(scored_document_hits.at(0))));
}

TEST_F(ScoringProcessorTest, ShouldRankByDocumentScoreAsc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  spec_proto.set_order_by(ScoringSpecProto::Order::ASC);

  // Sets up documents, guaranteed relationship:
  // document1 < document2 < document3
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {1, 2, 3}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);
  std::vector<ScoredDocumentHit> scored_document_hits =
      std::move(doc_hit_result_pair.second);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 3 results
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/3),
              ElementsAre(EqualsScoredDocumentHit(scored_document_hits.at(0)),
                          EqualsScoredDocumentHit(scored_document_hits.at(1)),
                          EqualsScoredDocumentHit(scored_document_hits.at(2))));
}

TEST_F(ScoringProcessorTest, ShouldRankByCreationTimestampDesc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);

  // Sets up documents, guaranteed relationship:
  // document1 < document2 < document3
  DocumentProto document1 =
      CreateDocument("icing", "email/1", kDefaultScore,
                     /*creation_timestamp_secs=*/1571100001);
  DocumentProto document2 =
      CreateDocument("icing", "email/2", kDefaultScore,
                     /*creation_timestamp_secs=*/1571100002);
  DocumentProto document3 =
      CreateDocument("icing", "email/3", kDefaultScore,
                     /*creation_timestamp_secs=*/1571100003);
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store()->Put(document1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store()->Put(document2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store()->Put(document3));
  DocHitInfo doc_hit_info1(document_id1);
  DocHitInfo doc_hit_info2(document_id2);
  DocHitInfo doc_hit_info3(document_id3);
  ScoredDocumentHit scored_document_hit1(document_id1, kSectionIdMaskNone,
                                         document1.creation_timestamp_secs());
  ScoredDocumentHit scored_document_hit2(document_id2, kSectionIdMaskNone,
                                         document2.creation_timestamp_secs());
  ScoredDocumentHit scored_document_hit3(document_id3, kSectionIdMaskNone,
                                         document3.creation_timestamp_secs());

  // Creates a dummy DocHitInfoIterator with 3 results
  std::vector<DocHitInfo> doc_hit_infos = {doc_hit_info2, doc_hit_info3,
                                           doc_hit_info1};
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor which ranks in descending order
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/3),
              ElementsAre(EqualsScoredDocumentHit(scored_document_hit3),
                          EqualsScoredDocumentHit(scored_document_hit2),
                          EqualsScoredDocumentHit(scored_document_hit1)));
}

TEST_F(ScoringProcessorTest, ShouldRankByCreationTimestampAsc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  spec_proto.set_order_by(ScoringSpecProto::Order::ASC);

  // Sets up documents, guaranteed relationship:
  // document1 < document2 < document3
  DocumentProto document1 =
      CreateDocument("icing", "email/1", kDefaultScore,
                     /*creation_timestamp_secs=*/1571100001);
  DocumentProto document2 =
      CreateDocument("icing", "email/2", kDefaultScore,
                     /*creation_timestamp_secs=*/1571100002);
  DocumentProto document3 =
      CreateDocument("icing", "email/3", kDefaultScore,
                     /*creation_timestamp_secs=*/1571100003);
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store()->Put(document1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store()->Put(document2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store()->Put(document3));
  DocHitInfo doc_hit_info1(document_id1);
  DocHitInfo doc_hit_info2(document_id2);
  DocHitInfo doc_hit_info3(document_id3);
  ScoredDocumentHit scored_document_hit1(document_id1, kSectionIdMaskNone,
                                         document1.creation_timestamp_secs());
  ScoredDocumentHit scored_document_hit2(document_id2, kSectionIdMaskNone,
                                         document2.creation_timestamp_secs());
  ScoredDocumentHit scored_document_hit3(document_id3, kSectionIdMaskNone,
                                         document3.creation_timestamp_secs());

  // Creates a dummy DocHitInfoIterator with 3 results
  std::vector<DocHitInfo> doc_hit_infos = {doc_hit_info2, doc_hit_info3,
                                           doc_hit_info1};
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor which ranks in descending order
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/3),
              ElementsAre(EqualsScoredDocumentHit(scored_document_hit1),
                          EqualsScoredDocumentHit(scored_document_hit2),
                          EqualsScoredDocumentHit(scored_document_hit3)));
}

TEST_F(ScoringProcessorTest, ShouldHandleSameScoresDesc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Creates 3 documents with the same score.
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {100, 100, 100}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);
  std::vector<ScoredDocumentHit> scored_document_hits =
      std::move(doc_hit_result_pair.second);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 3 results
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor which ranks in descending order
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  // Results should be ranked in descending document id order.
  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/3),
              ElementsAre(EqualsScoredDocumentHit(scored_document_hits.at(2)),
                          EqualsScoredDocumentHit(scored_document_hits.at(1)),
                          EqualsScoredDocumentHit(scored_document_hits.at(0))));
}

TEST_F(ScoringProcessorTest, ShouldHandleSameScoresAsc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  spec_proto.set_order_by(ScoringSpecProto::Order::ASC);

  // Creates 3 documents with the same score.
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {100, 100, 100}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);
  std::vector<ScoredDocumentHit> scored_document_hits =
      std::move(doc_hit_result_pair.second);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 3 results
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Creates a ScoringProcessor which ranks in ascending order
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));

  // Results should be ranked in ascending document id order.
  EXPECT_THAT(scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                              /*num_to_return=*/3),
              ElementsAre(EqualsScoredDocumentHit(scored_document_hits.at(0)),
                          EqualsScoredDocumentHit(scored_document_hits.at(1)),
                          EqualsScoredDocumentHit(scored_document_hits.at(2))));
}

TEST_F(ScoringProcessorTest, ShouldHandleNoScoresDesc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Sets up documents, guaranteed relationship:
  // document1 < document2 < document3
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {1, 2, 3}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);
  std::vector<ScoredDocumentHit> scored_document_hits =
      std::move(doc_hit_result_pair.second);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 4 results one of which doesn't have
  // a score.
  doc_hit_infos.emplace(doc_hit_infos.begin(), /*document_id_in=*/4,
                        kSectionIdMaskNone);
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // The document hit without a score will be be assigned the default score 0 in
  // a descending order.
  ScoredDocumentHit scored_document_hit_default_desc =
      ScoredDocumentHit(4, kSectionIdMaskNone, /*score=*/0.0);

  // Creates a ScoringProcessor which ranks in descending order
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));
  EXPECT_THAT(
      scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                      /*num_to_return=*/4),
      ElementsAre(EqualsScoredDocumentHit(scored_document_hits.at(2)),
                  EqualsScoredDocumentHit(scored_document_hits.at(1)),
                  EqualsScoredDocumentHit(scored_document_hits.at(0)),
                  EqualsScoredDocumentHit(scored_document_hit_default_desc)));
}

TEST_F(ScoringProcessorTest, ShouldHandleNoScoresAsc) {
  ScoringSpecProto spec_proto;
  spec_proto.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  spec_proto.set_order_by(ScoringSpecProto::Order::ASC);

  // Sets up documents, guaranteed relationship:
  // document1 < document2 < document3
  ICING_ASSERT_OK_AND_ASSIGN(
      auto doc_hit_result_pair,
      CreateAndInsertsDocumentsWithScores(document_store(), {1, 2, 3}));
  std::vector<DocHitInfo> doc_hit_infos = std::move(doc_hit_result_pair.first);
  std::vector<ScoredDocumentHit> scored_document_hits =
      std::move(doc_hit_result_pair.second);

  // Disarrays doc_hit_infos
  std::swap(doc_hit_infos.at(0), doc_hit_infos.at(1));
  std::swap(doc_hit_infos.at(1), doc_hit_infos.at(2));

  // Creates a dummy DocHitInfoIterator with 4 results one of which doesn't have
  // a score.
  doc_hit_infos.emplace(doc_hit_infos.begin(), /*document_id_in=*/4,
                        kSectionIdMaskNone);
  std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // The document hit without a score will be be assigned the default score
  // max of float in an ascending order.
  ScoredDocumentHit scored_document_hit_default_asc = ScoredDocumentHit(
      4, kSectionIdMaskNone, /*score=*/std::numeric_limits<float>::max());

  // Creates a ScoringProcessor which ranks in ascending order
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(spec_proto, document_store()));
  EXPECT_THAT(
      scoring_processor->ScoreAndRank(std::move(doc_hit_info_iterator),
                                      /*num_to_return=*/4),
      ElementsAre(EqualsScoredDocumentHit(scored_document_hits.at(0)),
                  EqualsScoredDocumentHit(scored_document_hits.at(1)),
                  EqualsScoredDocumentHit(scored_document_hits.at(2)),
                  EqualsScoredDocumentHit(scored_document_hit_default_asc)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
