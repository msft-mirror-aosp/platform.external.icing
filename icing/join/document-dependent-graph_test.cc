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

#include "third_party/icing/join/document-dependent-graph.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "testing/base/public/gmock.h"
#include "testing/base/public/gunit.h"
#include "third_party/icing/document-builder.h"
#include "third_party/icing/feature-flags.h"
#include "third_party/icing/file/filesystem.h"
#include "third_party/icing/file/portable-file-backed-proto-log.h"
#include "third_party/icing/graph/graph-interface.h"
#include "third_party/icing/join/document-join-id-pair.h"
#include "third_party/icing/join/qualified-id-join-index-impl-v2.h"
#include "third_party/icing/join/qualified-id-join-index-impl-v3.h"
#include "third_party/icing/join/qualified-id-join-indexing-handler.h"
#include "third_party/icing/portable/gzip_stream.h"
#include "third_party/icing/portable/platform.h"
#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/proto/document_wrapper.proto.h"
#include "third_party/icing/proto/schema.proto.h"
#include "third_party/icing/proto/scoring.proto.h"
#include "third_party/icing/proto/search.proto.h"
#include "third_party/icing/schema-builder.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/testing/common-matchers.h"
#include "third_party/icing/testing/fake-clock.h"
#include "third_party/icing/testing/test-data.h"
#include "third_party/icing/testing/test-feature-flags.h"
#include "third_party/icing/testing/tmp-directory.h"
#include "third_party/icing/tokenization/language-segmenter-factory.h"
#include "third_party/icing/tokenization/language-segmenter.h"
#include "third_party/icing/util/icu-data-file-helper.h"
#include "third_party/icing/util/status-macros.h"
#include "third_party/icing/util/tokenized-document.h"
#include "third_party/icu/include/unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::NotNull;

class DocumentDependentGraphTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    ASSERT_THAT(feature_flags_->enable_repeated_field_joins(), IsTrue());

    test_dir_ = GetTestTempDir() + "/document_dependent_graph_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());

    schema_store_dir_ = test_dir_ + "/schema_store";
    doc_store_dir_ = test_dir_ + "/doc_store";
    join_index_dir_ = test_dir_ + "/join_index";

    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //third_party/icing/BUILD.
          icu_data_file_helper::SetUpIcuDataFile(
              GetTestFilePath("third_party/icing/icu.dat")));
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
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Label")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("target")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("target2")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("softTarget")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_NONE)
                                     .SetCardinality(CARDINALITY_REPEATED)))

            .Build();
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

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
        join_index_, QualifiedIdJoinIndexImplV3::Create(
                         filesystem_, join_index_dir_, *feature_flags_));
    ICING_ASSERT_OK_AND_ASSIGN(
        join_indexing_handler_,
        QualifiedIdJoinIndexingHandler::Create(&fake_clock_, doc_store_.get(),
                                               join_index_.get()));
  }

  void TearDown() override {
    join_indexing_handler_.reset();

    join_index_.reset();
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
      ICING_RETURN_IF_ERROR(join_indexing_handler_->Handle(
          tokenized_documents[i], put_results[i].new_document_id,
          put_results[i].old_document_id, /*recovery_mode=*/false,
          /*put_document_stats=*/nullptr));
    }
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::StatusOr<std::vector<DocumentId>> GetEdgesOfNode(
      const DocumentDependentGraph& graph, int node_id) {
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<
            typename graph::GraphInterface<DocumentId>::EdgeIteratorIf>
            edge_itr,
        graph.GetEdgesIterator(node_id));
    std::vector<DocumentId> edges;
    while (edge_itr->Advance().ok()) {
      edges.push_back(edge_itr->Get());
    }
    return edges;
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::string doc_store_dir_;
  std::string join_index_dir_;

  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
  std::unique_ptr<QualifiedIdJoinIndexImplV3> join_index_;

  std::unique_ptr<QualifiedIdJoinIndexingHandler> join_indexing_handler_;
};

TEST_F(DocumentDependentGraphTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      DocumentDependentGraph::Create(/*schema_store=*/nullptr, doc_store_.get(),
                                     join_index_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      DocumentDependentGraph::Create(schema_store_.get(), /*doc_store=*/nullptr,
                                     join_index_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     /*join_index=*/nullptr),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(DocumentDependentGraphTest,
       CreationWithWrongJoinIndexVersionShouldFail) {
  std::string join_index_v2_dir = test_dir_ + "/join_index_v2";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdJoinIndexImplV2> join_index_v2,
      QualifiedIdJoinIndexImplV2::Create(filesystem_, join_index_v2_dir,
                                         /*pre_mapping_fbv=*/false));

  EXPECT_THAT(DocumentDependentGraph::Create(
                  schema_store_.get(), doc_store_.get(), join_index_v2.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("DocumentDependentGraph only supports "
                                 "QualifiedIdJoinIndex version V3.")));
}

TEST_F(DocumentDependentGraphTest, GetNumNodes) {
  // Put and index 4 documents.
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 =
      DocumentBuilder().SetKey("namespace", "uri/1").SetSchema("Label").Build();
  DocumentProto doc2 =
      DocumentBuilder().SetKey("namespace", "uri/2").SetSchema("Label").Build();
  DocumentProto doc3 =
      DocumentBuilder().SetKey("namespace", "uri/3").SetSchema("Label").Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  EXPECT_THAT(graph->GetNumNodes(), Eq(4));
}

TEST_F(DocumentDependentGraphTest, GetNumNodes_emptyStorage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  EXPECT_THAT(graph->GetNumNodes(), Eq(0));
}

TEST_F(DocumentDependentGraphTest, GetNumNodes_withReplacedDocuments) {
  // Put and index 1 document.
  DocumentProto doc =
      DocumentBuilder().SetKey("namespace", "uri").SetSchema("Label").Build();
  ICING_ASSERT_OK(AddDocuments({doc}));

  // Replace the document with new content.
  DocumentProto doc_replaced = DocumentBuilder()
                                   .SetKey("namespace", "uri")
                                   .SetSchema("Label")
                                   .AddStringProperty("target", "namespace#uri")
                                   .Build();
  ICING_ASSERT_OK(AddDocuments({doc_replaced}));

  // Sanity check.
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
              IsFalse());
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/1),
              IsTrue());

  // Even though document 0 is replaced, num nodes should still be 2 since it is
  // computed based on last stored doc id.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  EXPECT_THAT(graph->GetNumNodes(), Eq(2));
}

TEST_F(DocumentDependentGraphTest, GetNumNodes_withDeletedDocuments) {
  // Put and index 1 document.
  DocumentProto doc =
      DocumentBuilder().SetKey("namespace", "uri").SetSchema("Label").Build();
  ICING_ASSERT_OK(AddDocuments({doc}));

  // Delete the document.
  ICING_ASSERT_OK(doc_store_->Delete(
      /*document_id=*/0,
      /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));

  // Sanity check.
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
              IsFalse());

  // Even though document 0 is deleted, num nodes should still be 1 since it is
  // computed based on last stored doc id.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  EXPECT_THAT(graph->GetNumNodes(), Eq(1));
}

TEST_F(DocumentDependentGraphTest, GetNumNodes_withExpiredDocuments) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Put and index 1 document which expires at 1000 ms.
  DocumentProto doc = DocumentBuilder()
                          .SetCreationTimestampMs(0)
                          .SetTtlMs(1000)
                          .SetKey("namespace", "uri")
                          .SetSchema("Label")
                          .Build();
  ICING_ASSERT_OK(AddDocuments({doc}));

  // Adjust the clock to expire the document.
  fake_clock_.SetSystemTimeMilliseconds(2000);

  // Sanity check.
  ASSERT_THAT(doc_store_->GetAliveDocumentFilterData(
                  /*document_id=*/0,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsFalse());
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
              IsTrue());

  // Even though document 0 is expired, num nodes should still be 1 since it is
  // computed based on last stored doc id.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  EXPECT_THAT(graph->GetNumNodes(), Eq(1));
}

TEST_F(DocumentDependentGraphTest, GetEdgesIterator) {
  // Put and index 5 documents with the following relations:
  //
  // doc0 ---+
  //         |
  //         +---> Doc2 --> Doc3
  //         |
  // doc1 ---+
  //   |
  //   +---------> Doc4
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 =
      DocumentBuilder().SetKey("namespace", "uri/1").SetSchema("Label").Build();
  DocumentProto doc2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#uri/0", "namespace#uri/1")
          .Build();
  DocumentProto doc3 = DocumentBuilder()
                           .SetKey("namespace", "uri/3")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/2")
                           .Build();
  DocumentProto doc4 = DocumentBuilder()
                           .SetKey("namespace", "uri/4")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3, doc4}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(5));

  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0),
              IsOkAndHolds(ElementsAre(2)));
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/1),
              IsOkAndHolds(ElementsAre(2, 4)));
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/2),
              IsOkAndHolds(ElementsAre(3)));
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/3), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/4), IsOkAndHolds(IsEmpty()));
}

TEST_F(DocumentDependentGraphTest,
       GetEdgesIterator_withoutDeletePropagationShouldNotBeIncludedIntoEdges) {
  // Put and index 2 documents with the following relations:
  //
  // doc0 --(no delete propagation)--> doc1
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("softTarget", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(2));

  // Even though doc0 and doc1 have join relation, since the delete propagation
  // type of "softTarget" is NONE, it should not be included into the dependent
  // edges.
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/0))));
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0), IsOkAndHolds(IsEmpty()));
}

TEST_F(DocumentDependentGraphTest, GetEdgesIterator_shouldDedupeDocumentIds) {
  // Put and index 2 documents with the following relations:
  //
  // doc0 -> doc1
  //
  // And doc0, doc1 can be joined by multiple joinable properties with delete
  // propagation enabled.
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .AddStringProperty("target2", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(2));

  // doc0 should have 2 join relations with doc1, but doc1 should be returned
  // only once by the iterator.
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/2))));
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0),
              IsOkAndHolds(ElementsAre(1)));
}

TEST_F(DocumentDependentGraphTest,
       GetEdgesIterator_replacedDocument_removeEdges) {
  // Put and index 3 documents with the following relations:
  //
  // doc0 -> doc1 -> doc2
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2}));

  // Replace doc1 with new relations.
  // doc0  doc1_replaced -> doc2
  DocumentProto doc1_replaced =
      DocumentBuilder().SetKey("namespace", "uri/1").SetSchema("Label").Build();
  ICING_ASSERT_OK(AddDocuments({doc1_replaced}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(4));

  // Sanity check: join indexing handler handled migrate parent, so doc id 1
  // should be migrated to 3. Doc 0 should still contain the original relation
  // with doc 1.
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1))));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
      IsOkAndHolds(IsEmpty()));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
      IsOkAndHolds(IsEmpty()));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/2, /*joinable_property_id=*/1))));

  // Doc 0 should skip the edge to doc 1, since doc 1 was replaced.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0), IsOkAndHolds(IsEmpty()));
  // Doc 1 should return empty iterator since it was replaced.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/1), IsOkAndHolds(IsEmpty()));
  // Doc 2 should remain the same.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/2), IsOkAndHolds(IsEmpty()));
  // Doc 3 should contain 2.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/3),
              IsOkAndHolds(ElementsAre(2)));
}

TEST_F(DocumentDependentGraphTest, GetEdgesIterator_replacedDocument_addEdges) {
  // Put and index 4 documents with the following relations:
  //
  // doc0 -> doc1 -> doc2 -> doc3
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/1")
                           .Build();
  DocumentProto doc3 = DocumentBuilder()
                           .SetKey("namespace", "uri/3")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/2")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2, doc3}));

  // Replace doc2 with new relations.
  // doc0 -> doc1 -> doc2_replaced -> doc3
  //   |                  ^
  //   |                  |
  //   +------------------+
  DocumentProto doc2_replaced =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#uri/1", "namespace#uri/0")
          .Build();
  ICING_ASSERT_OK(AddDocuments({doc2_replaced}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(5));

  // Sanity check: join indexing handler handled migrate parent, so doc id 2
  // should be migrated to 4. Doc 1 should still contain the original relation
  // with doc 2.
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/4, /*joinable_property_id=*/1))));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/2, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/4, /*joinable_property_id=*/1))));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
      IsOkAndHolds(IsEmpty()));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
      IsOkAndHolds(IsEmpty()));
  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/4),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/3, /*joinable_property_id=*/1))));

  // Doc 0 should contain both edges to doc 1 and doc 4.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0),
              IsOkAndHolds(ElementsAre(1, 4)));
  // Doc 1 should skip the edge to doc 2, since doc 2 was replaced.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/1),
              IsOkAndHolds(ElementsAre(4)));
  // Doc 2 should return empty iterator since it was replaced.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/2), IsOkAndHolds(IsEmpty()));
  // Doc 3 should remain the same.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/3), IsOkAndHolds(IsEmpty()));
  // Doc 4 should contain 3.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/4),
              IsOkAndHolds(ElementsAre(3)));
}

TEST_F(DocumentDependentGraphTest,
       GetEdgesIterator_deletedParentShouldReturnEmptyIterator) {
  // Put and index 3 documents with the following relations:
  //
  // doc0 -> doc1
  //  |
  //  +----> doc2
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2}));

  // Delete doc0.
  ICING_ASSERT_OK(doc_store_->Delete(
      /*document_id=*/0,
      /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(3));

  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/2, /*joinable_property_id=*/1))));
  // Since doc0 is deleted, the iterator should return empty.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0), IsOkAndHolds(IsEmpty()));
}

TEST_F(DocumentDependentGraphTest,
       GetEdgesIterator_deletedChildShouldBeSkipped) {
  // Put and index 3 documents with the following relations:
  //
  // doc0 -> doc1
  //  |
  //  +----> doc2
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2}));

  // Delete doc1.
  ICING_ASSERT_OK(doc_store_->Delete(
      /*document_id=*/1,
      /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(3));

  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/2, /*joinable_property_id=*/1))));
  // Since doc1 is deleted, the iterator of doc0 should skip doc1.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0),
              IsOkAndHolds(ElementsAre(2)));
}

TEST_F(DocumentDependentGraphTest,
       GetEdgesIterator_expiredParentShouldNotBeSkipped) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Put and index 3 documents with the following relations:
  //
  // doc0 -> doc1
  //  |
  //  +----> doc2
  DocumentProto doc0 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(1000)
                           .SetKey("namespace", "uri/0")
                           .SetSchema("Label")
                           .Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2}));

  // Adjust the clock to expire doc0.
  fake_clock_.SetSystemTimeMilliseconds(2000);
  ASSERT_THAT(doc_store_->GetAliveDocumentFilterData(
                  /*document_id=*/0,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              IsFalse());
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(/*document_id=*/0),
              IsTrue());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(3));

  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/2, /*joinable_property_id=*/1))));
  // We should still be able to get all of doc0's edges.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0),
              IsOkAndHolds(ElementsAre(1, 2)));
}

TEST_F(DocumentDependentGraphTest,
       GetEdgesIterator_expiredChildShouldNotBeSkipped) {
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Put and index 3 documents with the following relations:
  //
  // doc0 -> doc1
  //  |
  //  +----> doc2
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 = DocumentBuilder()
                           .SetCreationTimestampMs(0)
                           .SetTtlMs(1000)
                           .SetKey("namespace", "uri/1")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  DocumentProto doc2 = DocumentBuilder()
                           .SetKey("namespace", "uri/2")
                           .SetSchema("Label")
                           .AddStringProperty("target", "namespace#uri/0")
                           .Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1, doc2}));

  // Adjust the clock to expire doc1.
  fake_clock_.SetSystemTimeMilliseconds(2000);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));
  ASSERT_THAT(graph->GetNumNodes(), Eq(3));

  ASSERT_THAT(
      join_index_->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/1, /*joinable_property_id=*/1),
          DocumentJoinIdPair(/*document_id=*/2, /*joinable_property_id=*/1))));
  // We should still be able to get doc1 from doc0's edge iterator.
  EXPECT_THAT(GetEdgesOfNode(*graph, /*node_id=*/0),
              IsOkAndHolds(ElementsAre(1, 2)));
}

TEST_F(DocumentDependentGraphTest, GetEdgesIterator_invalidNodeIdShouldFail) {
  ASSERT_THAT(doc_store_->last_added_document_id(), Eq(kInvalidDocumentId));
  ASSERT_THAT(join_index_->last_added_document_id(), Eq(kInvalidDocumentId));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentDependentGraph> graph,
      DocumentDependentGraph::Create(schema_store_.get(), doc_store_.get(),
                                     join_index_.get()));

  EXPECT_THAT(graph->GetNumNodes(), Eq(0));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/-2),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/0),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/2),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Put and index 2 documents.
  DocumentProto doc0 =
      DocumentBuilder().SetKey("namespace", "uri/0").SetSchema("Label").Build();
  DocumentProto doc1 =
      DocumentBuilder().SetKey("namespace", "uri/1").SetSchema("Label").Build();
  ICING_ASSERT_OK(AddDocuments({doc0, doc1}));
  ASSERT_THAT(doc_store_->last_added_document_id(), Eq(1));
  ASSERT_THAT(join_index_->last_added_document_id(), Eq(1));

  EXPECT_THAT(graph->GetNumNodes(), Eq(2));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/-2),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/0), IsOkAndHolds(NotNull()));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/1), IsOkAndHolds(NotNull()));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/2),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(graph->GetEdgesIterator(/*node_id=*/kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing
