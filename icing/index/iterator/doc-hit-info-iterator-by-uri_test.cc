// Copyright (C) 2024 Google LLC
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

#include "icing/index/iterator/doc-hit-info-iterator-by-uri.h"

#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;

class DocHitInfoIteratorByUriTest : public ::testing::Test {
 protected:
  DocHitInfoIteratorByUriTest() : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());

    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("email"))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false,
        /*allow_circular_schema_definitions=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                              schema_store_.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDeflateCompressionLevel,
                              /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
  const Filesystem filesystem_;
  const std::string test_dir_;
};

TEST_F(DocHitInfoIteratorByUriTest, EmptyFilterIsInvalid) {
  // Create a search spec without a uri filter specified.
  SearchSpecProto search_spec;
  EXPECT_THAT(
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Add a namespace group with no uris.
  NamespaceDocumentUriGroup* namespace_uris =
      search_spec.add_document_uri_filters();
  namespace_uris->set_namespace_("namespace");
  EXPECT_THAT(
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(DocHitInfoIteratorByUriTest, MatchesSomeDocuments) {
  // Put documents
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "email/1")
                                .SetSchema("email")
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "email/2")
                                .SetSchema("email")
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "email/3")
                                .SetSchema("email")
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                             document_store_->Put(document1));
  DocumentId document_id1 = put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document2));
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document3));
  DocumentId document_id3 = put_result.new_document_id;

  // Create a search spec with uri filters that only match document1 and
  // document3.
  SearchSpecProto search_spec;
  NamespaceDocumentUriGroup* uris = search_spec.add_document_uri_filters();
  uris->set_namespace_("namespace");
  uris->add_document_uris("email/1");
  uris->add_document_uris("email/3");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorByUri> iterator,
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec));
  EXPECT_THAT(GetDocumentIds(iterator.get()),
              ElementsAre(document_id3, document_id1));
  EXPECT_FALSE(iterator->Advance().ok());
}

TEST_F(DocHitInfoIteratorByUriTest, MatchesAllDocuments) {
  // Put documents
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "email/1")
                                .SetSchema("email")
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "email/2")
                                .SetSchema("email")
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "email/3")
                                .SetSchema("email")
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                             document_store_->Put(document1));
  DocumentId document_id1 = put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document2));
  DocumentId document_id2 = put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document3));
  DocumentId document_id3 = put_result.new_document_id;

  // Create a search spec with uri filters that match all documents.
  SearchSpecProto search_spec;
  NamespaceDocumentUriGroup* uris = search_spec.add_document_uri_filters();
  uris->set_namespace_("namespace");
  uris->add_document_uris("email/1");
  uris->add_document_uris("email/2");
  uris->add_document_uris("email/3");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorByUri> iterator,
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec));
  EXPECT_THAT(GetDocumentIds(iterator.get()),
              ElementsAre(document_id3, document_id2, document_id1));
  EXPECT_FALSE(iterator->Advance().ok());
}

TEST_F(DocHitInfoIteratorByUriTest, NonexistentUriIsOk) {
  // Put documents
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "email/1")
                                .SetSchema("email")
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "email/2")
                                .SetSchema("email")
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "email/3")
                                .SetSchema("email")
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                             document_store_->Put(document1));
  DocumentId document_id1 = put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document2));
  DocumentId document_id2 = put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document3));
  DocumentId document_id3 = put_result.new_document_id;

  // Create a search spec with a nonexistent uri in uri filters.
  SearchSpecProto search_spec;
  NamespaceDocumentUriGroup* uris = search_spec.add_document_uri_filters();
  uris->set_namespace_("namespace");
  uris->add_document_uris("email/1");
  uris->add_document_uris("email/2");
  uris->add_document_uris("email/3");
  uris->add_document_uris("nonexistent_uri");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorByUri> iterator,
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec));
  EXPECT_THAT(GetDocumentIds(iterator.get()),
              ElementsAre(document_id3, document_id2, document_id1));
  EXPECT_FALSE(iterator->Advance().ok());
}

TEST_F(DocHitInfoIteratorByUriTest, AllNonexistentUriShouldReturnEmptyResults) {
  // Put documents
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "email/1")
                                .SetSchema("email")
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "email/2")
                                .SetSchema("email")
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "email/3")
                                .SetSchema("email")
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                             document_store_->Put(document1));
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document2));
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document3));

  // Create a search spec with all nonexistent uris.
  SearchSpecProto search_spec;
  NamespaceDocumentUriGroup* uris = search_spec.add_document_uri_filters();
  uris->set_namespace_("namespace");
  uris->add_document_uris("nonexistent_uri1");
  uris->add_document_uris("nonexistent_uri2");
  uris->add_document_uris("nonexistent_uri3");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorByUri> iterator,
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec));
  EXPECT_THAT(GetDocumentIds(iterator.get()), IsEmpty());
  EXPECT_FALSE(iterator->Advance().ok());
}

TEST_F(DocHitInfoIteratorByUriTest, MultipleNamespaces) {
  // Put documents
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "email/1")
                                .SetSchema("email")
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace1", "email/2")
                                .SetSchema("email")
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace2", "email/3")
                                .SetSchema("email")
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                             document_store_->Put(document1));
  DocumentId document_id1 = put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document2));
  ICING_ASSERT_OK_AND_ASSIGN(put_result, document_store_->Put(document3));
  DocumentId document_id3 = put_result.new_document_id;

  // Create a search spec with uri filters that match document1 and document3 in
  // different namespaces.
  SearchSpecProto search_spec;
  NamespaceDocumentUriGroup* namespace1_uris =
      search_spec.add_document_uri_filters();
  namespace1_uris->set_namespace_("namespace1");
  namespace1_uris->add_document_uris("email/1");
  NamespaceDocumentUriGroup* namespace2_uris =
      search_spec.add_document_uri_filters();
  namespace2_uris->set_namespace_("namespace2");
  namespace2_uris->add_document_uris("email/3");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorByUri> iterator,
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec));
  EXPECT_THAT(GetDocumentIds(iterator.get()),
              ElementsAre(document_id3, document_id1));
  EXPECT_FALSE(iterator->Advance().ok());
}

TEST_F(DocHitInfoIteratorByUriTest, TrimRightMostNodeResultsInError) {
  SearchSpecProto search_spec;
  NamespaceDocumentUriGroup* uris = search_spec.add_document_uri_filters();
  uris->set_namespace_("namespace");
  uris->add_document_uris("uri");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorByUri> iterator,
      DocHitInfoIteratorByUri::Create(document_store_.get(), search_spec));
  EXPECT_THAT(std::move(*iterator).TrimRightMostNode(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing
