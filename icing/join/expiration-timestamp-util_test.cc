// Copyright (C) 2025 Google LLC
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

#include "icing/join/expiration-timestamp-util.h"

#include <memory>
#include <optional>
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
#include "icing/join/qualified-id-join-index-impl-v3.h"
#include "icing/join/qualified-id-join-indexing-handler.h"
#include "icing/portable/gzip_stream.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/status-macros.h"
#include "icing/util/tokenized-document.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Optional;
using ::testing::Property;

class ExpirationTimestampUtilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    ASSERT_THAT(feature_flags_->enable_repeated_field_joins(), IsTrue());

    test_dir_ = GetTestTempDir() + "/expiration_timestamp_util_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());

    schema_store_dir_ = test_dir_ + "/schema_store";
    doc_store_dir_ = test_dir_ + "/doc_store";
    qualified_id_join_index_dir_ = test_dir_ + "/qualified_id_join_index";

    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpIcuDataFile(
              GetTestFilePath("icing/icu.dat")));
    }

    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        lang_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    ASSERT_THAT(
        filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str()),
        IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("Label").AddProperty(
                PropertyConfigBuilder()
                    .SetName("target")
                    .SetDataTypeJoinableString(
                        JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                    .SetCardinality(CARDINALITY_REPEATED)))

            .Build();
    ASSERT_THAT(schema_store_->SetSchema(
                    schema, /*ignore_errors_and_delete_documents=*/false),
                IsOk());

    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str()),
                IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, doc_store_dir_, &fake_clock_, schema_store_.get(),
            feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
            /*initialize_stats=*/nullptr));
    doc_store_ = std::move(create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        qualified_id_join_index_,
        QualifiedIdJoinIndexImplV3::Create(
            filesystem_, qualified_id_join_index_dir_, *feature_flags_));

    ICING_ASSERT_OK_AND_ASSIGN(
        qualified_id_join_indexing_handler_,
        QualifiedIdJoinIndexingHandler::Create(&fake_clock_, doc_store_.get(),
                                               qualified_id_join_index_.get()));
  }

  void TearDown() override {
    qualified_id_join_indexing_handler_.reset();

    qualified_id_join_index_.reset();
    doc_store_.reset();
    schema_store_.reset();
    lang_segmenter_.reset();

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  // Helper function to batch add documents.
  libtextclassifier3::Status AddDocuments(
      std::vector<DocumentProto> documents) {
    // Tokenize all documents.
    std::vector<TokenizedDocument> tokenized_documents;
    tokenized_documents.reserve(documents.size());
    for (DocumentProto& document : documents) {
      ICING_ASSIGN_OR_RETURN(
          TokenizedDocument tokenized_document,
          TokenizedDocument::Create(
              schema_store_.get(), lang_segmenter_.get(),
              /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
              std::move(document)));
      tokenized_documents.push_back(std::move(tokenized_document));
    }

    // Put all documents into the document store and get document ids.
    std::vector<DocumentStore::PutResult> put_results;
    put_results.reserve(documents.size());
    for (const TokenizedDocument& tokenized_document : tokenized_documents) {
      ICING_ASSIGN_OR_RETURN(
          DocumentStore::PutResult put_result,
          doc_store_->Put(tokenized_document.document_wrapper()));
      put_results.push_back(std::move(put_result));
    }

    // Index all documents.
    for (int i = 0; i < tokenized_documents.size(); ++i) {
      ICING_RETURN_IF_ERROR(qualified_id_join_indexing_handler_->Handle(
          tokenized_documents[i], put_results[i].new_document_id,
          put_results[i].old_document_id, /*recovery_mode=*/false,
          /*put_document_stats=*/nullptr));
    }
    return libtextclassifier3::Status::OK;
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::string doc_store_dir_;
  std::string qualified_id_join_index_dir_;

  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
  std::unique_ptr<QualifiedIdJoinIndexImplV3> qualified_id_join_index_;

  std::unique_ptr<QualifiedIdJoinIndexingHandler>
      qualified_id_join_indexing_handler_;
};

TEST_F(ExpirationTimestampUtilTest, SingleDocumentPropagation) {
  // General test for SingleDocumentPropagation.
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create docs with the following (raw) expiration timestamps and relations:
  //
  //                     +------> dept1 (30) -------+
  //                     |                          |
  //                     |                          v
  // depcy1 (10) ---> doc (5) --> dept2 (30) --> dept3 (30)
  //                     ^           |
  //                     |           v
  // depcy2 (2) ---------+        dept4 (30)
  DocumentProto depcy1 = DocumentBuilder()
                             .SetCreationTimestampMs(0)
                             .SetTtlMs(10)
                             .SetKey("namespace", "depcy1")
                             .SetSchema("Label")
                             .Build();
  DocumentProto depcy2 = DocumentBuilder()
                             .SetCreationTimestampMs(0)
                             .SetTtlMs(2)
                             .SetKey("namespace", "depcy2")
                             .SetSchema("Label")
                             .Build();
  DocumentProto doc =
      DocumentBuilder()
          .SetCreationTimestampMs(0)
          .SetTtlMs(5)
          .SetKey("namespace", "doc")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#depcy1", "namespace#depcy2")
          .Build();
  DocumentProto dept1 = DocumentBuilder()
                            .SetCreationTimestampMs(0)
                            .SetTtlMs(30)
                            .SetKey("namespace", "dept1")
                            .SetSchema("Label")
                            .AddStringProperty("target", "namespace#doc")
                            .Build();
  DocumentProto dept2 = DocumentBuilder()
                            .SetCreationTimestampMs(0)
                            .SetTtlMs(30)
                            .SetKey("namespace", "dept2")
                            .SetSchema("Label")
                            .AddStringProperty("target", "namespace#doc")
                            .Build();
  DocumentProto dept3 =
      DocumentBuilder()
          .SetCreationTimestampMs(0)
          .SetTtlMs(30)
          .SetKey("namespace", "dept3")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#dept1", "namespace#dept2")
          .Build();
  DocumentProto dept4 = DocumentBuilder()
                            .SetCreationTimestampMs(0)
                            .SetTtlMs(30)
                            .SetKey("namespace", "dept4")
                            .SetSchema("Label")
                            .AddStringProperty("target", "namespace#dept2")
                            .Build();
  // Add all documents. Note that they will have document ids 0, 1, 2, 3, 4,
  // 5, 6.
  ICING_ASSERT_OK(
      AddDocuments({depcy1, depcy2, doc, dept1, dept2, dept3, dept4}));

  // Run the expiration timestamp propagation on doc (id 2) with its
  // dependencies depcy1 and depcy2 (id 0 and 1).
  //
  // The expiration timestamps should become:
  //                     +------> dept1 (2) -------+
  //                     |                         |
  //                     |                         v
  // depcy1 (10) ---> doc (2) --> dept2 (2) --> dept3 (2)
  //                     ^           |
  //                     |           v
  // depcy2 (2) ---------+        dept4 (2)
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/2, /*dependency_doc_ids=*/{0, 1},
                  *schema_store_, *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());

  // depcy1 and depcy2 should not be updated.
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(10))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));

  // Verify doc, dept1, dept2, dept3, and dept4's expiration timestamps are
  // updated.
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/3),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/4),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/5),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/6),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
}

TEST_F(
    ExpirationTimestampUtilTest,
    SingleDocumentPropagation_updateFromDependency_shouldUpdateToSmallerExpTs) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create doc0 and doc1 with raw expiration timestamps 10 and 2.
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(2)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));

  // Create doc2 with raw expiration timestamp 5 and with the following
  // relations:
  //
  // doc0 (10) --+
  //             |
  //             +---> doc2 (5)
  //             |
  // doc1 (2) ---+
  DocumentProto doc2 =
      DocumentBuilder()
          .SetCreationTimestampMs(0)
          .SetTtlMs(5)
          .SetKey("namespace", "uri/2")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#uri/0", "namespace#uri/1")
          .Build();
  ICING_ASSERT_OK(AddDocuments({doc2}));

  // Sanity check on the document filter data before propagation.
  ASSERT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(10))));
  ASSERT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  ASSERT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(5))));

  // Run the expiration timestamp propagation on doc2 with its dependencies doc0
  // and doc1. Doc2's expiration timestamp should be updated to 2.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/2, /*dependency_doc_ids=*/{0, 1},
                  *schema_store_, *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
}

TEST_F(
    ExpirationTimestampUtilTest,
    SingleDocumentPropagation_updateFromDependency_shouldIgnoreGreaterExpTs) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create doc0 and doc1 with raw expiration timestamps 10 and 15.
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(15)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));

  // Create doc2 with raw expiration timestamp 5 and with the following
  // relations:
  //
  // doc0 (10) --+
  //             |
  //             +---> doc2 (5)
  //             |
  // doc1 (15) --+
  DocumentProto doc2 =
      DocumentBuilder()
          .SetCreationTimestampMs(0)
          .SetTtlMs(5)
          .SetKey("namespace", "uri/2")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#uri/0", "namespace#uri/1")
          .Build();
  ICING_ASSERT_OK(AddDocuments({doc2}));

  // Sanity check on the document filter data before propagation.
  ASSERT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(10))));
  ASSERT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(15))));
  ASSERT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(5))));

  // Run the expiration timestamp propagation on doc2 with its dependencies doc0
  // and doc1. Since all of its dependencies have greater expiration timestamps,
  // doc2's expiration timestamp should remain 5 after propagation.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/2, /*dependency_doc_ids=*/{0, 1},
                  *schema_store_, *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(5))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_updateFromDependency_selfCycle) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create a document with a self cycle relation.
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/0")
                           .AddStringProperty("target", "namespace#uri/0")
                           .SetSchema("Label")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0}));

  // Run the expiration timestamp propagation on doc0.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{0}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(10))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_smallerExpTs) {
  // Note: in the actual integration with IcingSearchEngine, the only case to
  //   propagate to dependent is when replacing an existing parent document, but
  //   for testing purpose, we can just simply create all documents at once and
  //   run the algorithm on the target document directly without creating
  //   replacement scenario.
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following (raw) expiration timestamps and
  // relations:
  //
  // doc0 (2) ---> doc1 (5) ----> doc3 (1)
  //    |
  //    +--------> doc2 (10) ---> doc4 (3)
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(2)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(5)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc3 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(1)
                           .SetKey("namespace", "uri/3")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  DocumentProto doc4 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(3)
                           .SetKey("namespace", "uri/4")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/2")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3, doc4}));

  // Run the expiration timestamp propagation on doc0. It should propagate to
  // doc1, doc2, and doc4.
  //
  // Final graph:
  // doc0 (2) ----> doc1 (2) ----> doc3 (1)
  //    |
  //    +---------> doc2 (2) ----> doc4 (2)
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/3),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(1))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/4),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_greaterExpTs) {
  // Note: in the actual integration with IcingSearchEngine, the only case to
  //   propagate to dependent is when replacing an existing parent document, but
  //   for testing purpose, we can just simply create all documents at once and
  //   run the algorithm on the target document directly without creating
  //   replacement scenario.
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following (raw) expiration timestamps and
  // relations:
  //
  // doc0 (15) ---> doc1 (5) ----> doc3 (1)
  //    |
  //    +---------> doc2 (10) ---> doc4 (3)
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(15)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(5)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc3 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(1)
                           .SetKey("namespace", "uri/3")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  DocumentProto doc4 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(3)
                           .SetKey("namespace", "uri/4")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/2")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3, doc4}));

  // Run the expiration timestamp propagation on doc0. Since it has greater
  // expiration timestamp than all of its dependents, no dependents' expiration
  // timestamps should be updated.
  //
  // Final graph:
  // doc0 (15) ---> doc1 (5) ----> doc3 (1)
  //    |
  //    +---------> doc2 (10) ---> doc4 (3)
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(15))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(5))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(10))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/3),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(1))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/4),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(3))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_expTsShouldOnlyDecrease) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following raw expiration timestamps and
  // relations:
  //
  // doc0 (raw 10) --+
  //                 |
  //                 +---> doc2 (raw 8)
  //                 |
  // doc1 (raw 5) ---+
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(5)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc2 =
      DocumentBuilder()
          .SetCreationTimestampMs(0)
          .SetTtlMs(8)
          .SetKey("namespace", "uri/2")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#uri/0", "namespace#uri/1")
          .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2}));

  // Run the expiration timestamp propagation on doc2. It should have
  // (expiration ts, raw expiration ts) = (5, 8).
  //
  // doc0 (10, 10) --+
  //                 |
  //                 +---> doc2 (5, 8)
  //                 |
  // doc1 (5, 5) ----+
  ASSERT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/2, /*dependency_doc_ids=*/{0, 1},
                  *schema_store_, *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  std::optional<DocumentFilterData> filter_data =
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2);
  ASSERT_THAT(filter_data->expiration_timestamp_ms(), Eq(5));
  ASSERT_THAT(filter_data->raw_expiration_timestamp_ms(), Eq(8));

  // Create doc3 with raw expiration timestamp 7 and the same key as doc1.
  // Use it to replace doc1.
  DocumentProto doc3 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(7)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc3}));

  // Run the expiration timestamp propagation on doc3.
  //
  // doc0 (10, 10) --+
  //                 |
  //                 +---> doc2 (_, 8)
  //                 |
  // doc3 (7, 7) ----+
  //
  // According to the graph, if doc2 had been added after doc0 and doc3, then it
  // should've had expiration timestamp 7 after propagation. But since doc3 is a
  // replacement update now and the BFS algorithm only decreases the expiration
  // timestamp of doc2 without considering potential increase caused by
  // dependency replacement, doc2 should still have expiration timestamp 5.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/3, /*dependency_doc_ids=*/{}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(5))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_cycle) {
  // Note: in the actual integration with IcingSearchEngine, the only case to
  //   propagate to dependent is when replacing an existing parent document, but
  //   for testing purpose, we can just simply create all documents at once and
  //   run the algorithm on the target document directly without creating
  //   replacement scenario.
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following raw expiration timestamps and
  // relations:
  //
  // doc0 (raw 2) ----> doc1 (raw 5)
  //    ^                   |
  //    |                   |
  //    |                   v
  // doc3 (raw 7) <---- doc2 (raw 8)
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(2)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/3")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(5)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(8)
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  DocumentProto doc3 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(7)
                           .SetKey("namespace", "uri/3")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/2")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3}));

  // Run the expiration timestamp propagation on doc0.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{3}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());

  // All of them should have expiration timestamp 2.
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/3),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_shouldSkipDeleted) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following (raw) expiration timestamps and
  // relations:
  //
  // doc0 (5) ---> doc1 (10)
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(5)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));

  // Delete doc1.
  ICING_ASSERT_OK(doc_store_->Delete(
      /*document_id=*/1,
      /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));

  // Run the expiration timestamp propagation on doc0. doc1 should be skipped
  // since it's deleted.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(5))));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
              IsFalse());
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_shouldSkipExpired) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following (raw) expiration timestamps and
  // relations:
  //
  // doc0 (30) ---> doc1 (10)
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(30)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(10)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));

  // Adjust the current time to 20. It makes doc1 expired.
  fake_clock_.SetSystemTimeMilliseconds(20);

  // Run the expiration timestamp propagation on doc0. doc1 is still traversed
  // and we attempt to set the propagated expiration timestamp, but the new
  // value is always larger than the existing one, so it's skipped.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(30))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(10))));
}

TEST_F(ExpirationTimestampUtilTest,
       SingleDocumentPropagation_propgateToDependents_noBellmanFord) {
  // Note: in the actual integration with IcingSearchEngine, this update
  //   scenario is not possible. But for testing purpose, we want to make sure
  //   the algorithm doesn't traverse nodes on a cycle for multiple times, which
  //   has bad time complexity of O(V*E).
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create documents with the following raw expiration timestamps and
  // relations:
  //
  // doc0 (raw 4) ----> doc1 (raw 8) <---------+
  //                         |                 |
  //                         |                 |
  //                         v                 |
  //                    doc2 (raw 5) ---> doc3 (raw 2)
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(4)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 =
      DocumentBuilder()
          .SetCreationTimestampMs(0)
          .SetTtlMs(8)
          .SetKey("namespace", "uri/1")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#uri/0", "namespace#uri/3")
          .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(5)
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  DocumentProto doc3 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(2)
                           .SetKey("namespace", "uri/3")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/2")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3}));

  // Run the expiration timestamp propagation on doc0.
  EXPECT_THAT(ExpirationTimestampUtil::SingleDocumentPropagation(
                  /*document_id=*/0, /*dependency_doc_ids=*/{}, *schema_store_,
                  *qualified_id_join_index_, *doc_store_,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsOk());

  // - According to the definition of the dependency, doc1, doc2, and doc3
  //   should've had expiration timestamp 2.
  // - But here, we're not running Bellman-Ford algorithm, so when starting from
  //   doc0, the expiration timestamp of doc3 is not propagated. So doc3 should
  //   remain 2, and doc1 and doc2 should be updated to 4.
  //
  // Note: in reality, when we added doc3, another round of propagation
  //   should've propagated doc3's expiration timestamp to others, so we will
  //   never get this incorrect scenario in production. If we really need batch
  //   update for doc0 and doc3, then we should consider SCC + topological sort
  //   (linear) algorithm, as mentioned in the docstring of
  //   SingleDocumentPropagation.
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(4))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(4))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/2),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(4))));
  EXPECT_THAT(
      doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/3),
      Optional(Property(&DocumentFilterData::expiration_timestamp_ms, Eq(2))));
}

}  // namespace

}  // namespace lib
}  // namespace icing
