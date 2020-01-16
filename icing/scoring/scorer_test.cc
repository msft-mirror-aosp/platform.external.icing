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

#include "icing/scoring/scorer.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::testing::Eq;
using ::testing::Test;

class ScorerTest : public Test {
 protected:
  ScorerTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        doc_store_dir_(test_dir_ + "/doc_store"),
        schema_store_dir_(test_dir_ + "/schema_store") {}

  void SetUp() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    fake_clock1_.SetSystemTimeMilliseconds(1571100000000);
    fake_clock2_.SetSystemTimeMilliseconds(1572200000000);

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_));

    ICING_ASSERT_OK_AND_ASSIGN(
        document_store_,
        DocumentStore::Create(&filesystem_, doc_store_dir_, &fake_clock1_,
                              schema_store_.get()));

    // Creates a simple email schema
    SchemaProto test_email_schema;
    auto type_config = test_email_schema.add_types();
    type_config->set_schema_type("email");
    auto subject = type_config->add_properties();
    subject->set_property_name("subject");
    subject->set_data_type(PropertyConfigProto::DataType::STRING);
    subject->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

    ICING_ASSERT_OK(schema_store_->SetSchema(test_email_schema));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  DocumentStore* document_store() { return document_store_.get(); }

  const FakeClock& fake_clock1() { return fake_clock1_; }

  const FakeClock& fake_clock2() { return fake_clock2_; }

 private:
  const std::string test_dir_;
  const std::string doc_store_dir_;
  const std::string schema_store_dir_;
  Filesystem filesystem_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock1_;
  FakeClock fake_clock2_;
};

TEST_F(ScorerTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(Scorer::Create(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE,
                             /*default_score=*/0, /*document_store=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(ScorerTest, CreationWithInvalidRankingStrategyShouldFail) {
  EXPECT_THAT(Scorer::Create(ScoringSpecProto::RankingStrategy::NONE,
                             /*default_score=*/0, document_store()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorerTest, ShouldGetDefaultScore) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      Scorer::Create(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE,
                     /*default_score=*/10, document_store()));

  DocumentId non_existing_document_id = 1;
  // The caller-provided default score is returned
  EXPECT_THAT(scorer->GetScore(non_existing_document_id), Eq(10));
}

TEST_F(ScorerTest, ShouldGetDefaultDocumentScore) {
  // Creates a test document with the default document score 0
  DocumentProto test_document =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .SetCreationTimestampMs(fake_clock1().GetSystemTimeMilliseconds())
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store()->Put(test_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      Scorer::Create(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE,
                     /*default_score=*/10, document_store()));

  EXPECT_THAT(scorer->GetScore(document_id), Eq(0));
}

TEST_F(ScorerTest, ShouldGetCorrectDocumentScore) {
  // Creates a test document with document score 5
  DocumentProto test_document =
      DocumentBuilder()
          .SetScore(5)
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .SetCreationTimestampMs(fake_clock2().GetSystemTimeMilliseconds())
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store()->Put(test_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      Scorer::Create(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE,
                     /*default_score=*/0, document_store()));

  EXPECT_THAT(scorer->GetScore(document_id), Eq(5));
}

TEST_F(ScorerTest, ShouldGetCorrectCreationTimestampScore) {
  // Creates test_document1 with fake timestamp1
  DocumentProto test_document1 =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .SetCreationTimestampMs(fake_clock1().GetSystemTimeMilliseconds())
          .Build();
  // Creates test_document2 with fake timestamp2
  DocumentProto test_document2 =
      DocumentBuilder()
          .SetKey("icing", "email/2")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo 2")
          .SetCreationTimestampMs(fake_clock2().GetSystemTimeMilliseconds())
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store()->Put(test_document1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store()->Put(test_document2));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      Scorer::Create(ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP,
                     /*default_score=*/0, document_store()));

  EXPECT_THAT(scorer->GetScore(document_id1),
              Eq(fake_clock1().GetSystemTimeMilliseconds()));
  EXPECT_THAT(scorer->GetScore(document_id2),
              Eq(fake_clock2().GetSystemTimeMilliseconds()));
}

}  // namespace

}  // namespace lib
}  // namespace icing
