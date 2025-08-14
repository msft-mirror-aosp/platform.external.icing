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

#include "third_party/icing/index/iterator/doc-hit-info-iterator-filter.h"

#include <memory>
#include <string>
#include <string_view>
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
#include "third_party/icing/index/hit/doc-hit-info.h"
#include "third_party/icing/index/iterator/doc-hit-info-iterator-and.h"
#include "third_party/icing/index/iterator/doc-hit-info-iterator-not.h"
#include "third_party/icing/index/iterator/doc-hit-info-iterator-or.h"
#include "third_party/icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "third_party/icing/index/iterator/doc-hit-info-iterator.h"
#include "third_party/icing/index/iterator/document-filter-predicate.h"
#include "third_party/icing/portable/gzip_stream.h"
#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/proto/schema.proto.h"
#include "third_party/icing/query/query-utils.h"
#include "third_party/icing/schema-builder.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/schema/section.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/testing/common-matchers.h"
#include "third_party/icing/testing/fake-clock.h"
#include "third_party/icing/testing/test-feature-flags.h"
#include "third_party/icing/testing/tmp-directory.h"
#include "third_party/icing/util/clock.h"
#include "third_party/icing/util/document-util.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Ne;
using ::testing::Pointee;
using ::testing::Pointer;
using ::testing::WhenDynamicCastTo;

libtextclassifier3::StatusOr<DocumentStore::CreateResult> CreateDocumentStore(
    const Filesystem* filesystem, const std::string& base_dir,
    const Clock* clock, const SchemaStore* schema_store,
    const FeatureFlags& feature_flags) {
  return DocumentStore::Create(
      filesystem, base_dir, clock, schema_store, &feature_flags,
      /*force_recovery_and_revalidate_documents=*/false,
      /*pre_mapping_fbv=*/false, /*use_persistent_hash_map=*/true,
      PortableFileBackedProtoLog<DocumentWrapper>::kDefaultCompressionLevel,
      PortableFileBackedProtoLog<
          DocumentWrapper>::kDefaultCompressionThresholdBytes,
      protobuf_ports::kDefaultMemLevel,
      /*initialize_stats=*/nullptr);
}

class DocHitInfoIteratorDummyHandlingFilter
    : public DocHitInfoIteratorHandlingFilter {
 public:
  libtextclassifier3::Status Advance() override {
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    TrimmedNode node = {nullptr, /*term=*/"", /*term_start_index_=*/0,
                        /*unnormalized_term_length_=*/0};
    return node;
  }

  std::vector<std::unique_ptr<DocHitInfoIterator>*> GetChildren() override {
    return {};
  }

  CallStats GetCallStats() const override { return CallStats(); }

  std::string ToString() const override {
    return "DocHitInfoIteratorDummyHandlingFilter";
  }

  std::vector<const DocumentFilterPredicate*> document_filter_predicates()
      const {
    return document_filter_predicates_;
  }
};

class DocHitInfoIteratorDeletedFilterTest : public ::testing::Test {
 protected:
  DocHitInfoIteratorDeletedFilterTest()
      : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    test_document1_ =
        DocumentBuilder().SetKey("icing", "email/1").SetSchema("email").Build();
    test_document2_ =
        DocumentBuilder().SetKey("icing", "email/2").SetSchema("email").Build();
    test_document3_ =
        DocumentBuilder().SetKey("icing", "email/3").SetSchema("email").Build();

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("email"))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get(), *feature_flags_));
    document_store_ = std::move(create_result.document_store);

    predicate_ = GetFilterPredicateBySchemaAndNamespace(
        SearchSpecProto::default_instance(), *document_store_, *schema_store_,
        fake_clock_.GetSystemTimeMilliseconds());
  }

  void TearDown() override {
    // Destroy objects before the whole directory is removed because they
    // persist data in the destructor.
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
  DocumentProto test_document1_;
  DocumentProto test_document2_;
  DocumentProto test_document3_;
  std::unique_ptr<DocumentFilterPredicate> predicate_;
};

TEST_F(DocHitInfoIteratorDeletedFilterTest, EmptyOriginalIterator) {
  ICING_ASSERT_OK(document_store_->Put(
      document_util::CreateDocumentWrapper(test_document1_)));

  std::unique_ptr<DocHitInfoIterator> original_iterator_empty =
      std::make_unique<DocHitInfoIteratorDummy>();

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator_empty), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

TEST_F(DocHitInfoIteratorDeletedFilterTest, DeletedDocumentsAreFiltered) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(test_document1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(test_document2_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(test_document3_)));
  DocumentId document_id3 = put_result3.new_document_id;

  // Deletes test document 2
  ICING_ASSERT_OK(document_store_->Delete(
      test_document2_.namespace_(), test_document2_.uri(),
      fake_clock_.GetSystemTimeMilliseconds()));

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1),
                                           DocHitInfo(document_id2),
                                           DocHitInfo(document_id3)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1, document_id3));
}

TEST_F(DocHitInfoIteratorDeletedFilterTest, NonExistingDocumentsAreFiltered) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(test_document1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(test_document2_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(test_document3_)));
  DocumentId document_id3 = put_result3.new_document_id;

  // Document ids 7, 8, 9 are not existing
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1),
                                           DocHitInfo(document_id2),
                                           DocHitInfo(document_id3),
                                           DocHitInfo(7),
                                           DocHitInfo(8),
                                           DocHitInfo(9)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1, document_id2, document_id3));
}

TEST_F(DocHitInfoIteratorDeletedFilterTest, NegativeDocumentIdIsIgnored) {
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(-1)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(filtered_iterator->Advance(),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST_F(DocHitInfoIteratorDeletedFilterTest, InvalidDocumentIdIsIgnored) {
  // kInvalidDocumentId should be skipped.
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(kInvalidDocumentId)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(filtered_iterator->Advance(),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST_F(DocHitInfoIteratorDeletedFilterTest, GreaterThanMaxDocumentIdIsIgnored) {
  // Document ids that are greater than the max value is invalid and should be
  // skipped.
  DocumentId invalid_greater_than_max = kMaxDocumentId + 2;
  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(invalid_greater_than_max)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(filtered_iterator->Advance(),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

class DocHitInfoIteratorNamespaceFilterTest : public ::testing::Test {
 protected:
  DocHitInfoIteratorNamespaceFilterTest()
      : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    document1_namespace1_ = DocumentBuilder()
                                .SetKey(namespace1_, "email/1")
                                .SetSchema("email")
                                .Build();
    document2_namespace1_ = DocumentBuilder()
                                .SetKey(namespace1_, "email/2")
                                .SetSchema("email")
                                .Build();
    document1_namespace2_ = DocumentBuilder()
                                .SetKey(namespace2_, "email/1")
                                .SetSchema("email")
                                .Build();
    document1_namespace3_ = DocumentBuilder()
                                .SetKey(namespace3_, "email/1")
                                .SetSchema("email")
                                .Build();

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("email"))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get(), *feature_flags_));
    document_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    // Destroy objects before the whole directory is removed because they
    // persist data in the destructor.
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
  const std::string namespace1_ = "namespace1";
  const std::string namespace2_ = "namespace2";
  const std::string namespace3_ = "namespace3";
  DocumentProto document1_namespace1_;
  DocumentProto document2_namespace1_;
  DocumentProto document1_namespace2_;
  DocumentProto document1_namespace3_;
};

TEST_F(DocHitInfoIteratorNamespaceFilterTest, EmptyOriginalIterator) {
  std::unique_ptr<DocHitInfoIterator> original_iterator_empty =
      std::make_unique<DocHitInfoIteratorDummy>();

  SearchSpecProto search_spec;
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator_empty), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

TEST_F(DocHitInfoIteratorNamespaceFilterTest,
       NonexistentNamespacesReturnsEmpty) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  search_spec.add_namespace_filters("nonexistent_namespace");
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

TEST_F(DocHitInfoIteratorNamespaceFilterTest, NoNamespacesReturnsAll) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_)));
  DocumentId document_id1 = put_result1.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorNamespaceFilterTest,
       FilterOutExistingDocumentFromDifferentNamespace) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_namespace1_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace2_)));
  DocumentId document_id3 = put_result3.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1),
                                           DocHitInfo(document_id2),
                                           DocHitInfo(document_id3)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1, document_id2));
}

TEST_F(DocHitInfoIteratorNamespaceFilterTest, FilterForMultipleNamespacesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_namespace1_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace2_)));
  DocumentId document_id3 = put_result3.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result4,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace3_)));
  DocumentId document_id4 = put_result4.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id1), DocHitInfo(document_id2),
      DocHitInfo(document_id3), DocHitInfo(document_id4)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  search_spec.add_namespace_filters(namespace1_);
  search_spec.add_namespace_filters(namespace3_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1, document_id2, document_id4));
}

class DocHitInfoIteratorSchemaTypeFilterTest : public ::testing::Test {
 protected:
  static constexpr std::string_view kSchema1 = "email";
  static constexpr std::string_view kSchema2 = "message";
  static constexpr std::string_view kSchema3 = "person";
  static constexpr std::string_view kSchema4 = "artist";
  static constexpr std::string_view kSchema5 = "emailMessage";

  DocHitInfoIteratorSchemaTypeFilterTest()
      : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    document1_schema1_ = DocumentBuilder()
                             .SetKey("namespace", "1")
                             .SetSchema(std::string(kSchema1))
                             .Build();
    document2_schema2_ = DocumentBuilder()
                             .SetKey("namespace", "2")
                             .SetSchema(std::string(kSchema2))
                             .Build();
    document3_schema3_ = DocumentBuilder()
                             .SetKey("namespace", "3")
                             .SetSchema(std::string(kSchema3))
                             .Build();
    document4_schema1_ = DocumentBuilder()
                             .SetKey("namespace", "4")
                             .SetSchema(std::string(kSchema1))
                             .Build();

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType(kSchema1))
            .AddType(SchemaTypeConfigBuilder().SetType(kSchema2))
            .AddType(SchemaTypeConfigBuilder().SetType(kSchema3))
            .AddType(SchemaTypeConfigBuilder().SetType(kSchema4).AddParentType(
                kSchema3))
            .AddType(SchemaTypeConfigBuilder()
                         .SetType(std::string(kSchema5))
                         .AddParentType(kSchema1)
                         .AddParentType(kSchema2))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get(), *feature_flags_));
    document_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    // Destroy objects before the whole directory is removed because they
    // persist data in the destructor.
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
  DocumentProto document1_schema1_;
  DocumentProto document2_schema2_;
  DocumentProto document3_schema3_;
  DocumentProto document4_schema1_;
};

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest, EmptyOriginalIterator) {
  std::unique_ptr<DocHitInfoIterator> original_iterator_empty =
      std::make_unique<DocHitInfoIteratorDummy>();

  SearchSpecProto search_spec;
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator_empty), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest,
       NonexistentSchemaTypeReturnsEmpty) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters("nonexistent_schema_type");
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest, NoSchemaTypesReturnsAll) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest,
       FilterOutExistingDocumentFromDifferentSchemaTypes) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_schema2_)));
  DocumentId document_id2 = put_result2.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1),
                                           DocHitInfo(document_id2)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(std::string(kSchema1));
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest, FilterForMultipleSchemaTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_schema2_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document3_schema3_)));
  DocumentId document_id3 = put_result3.new_document_id;
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1),
                                           DocHitInfo(document_id2),
                                           DocHitInfo(document_id3)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(std::string(kSchema2));
  search_spec.add_schema_type_filters(std::string(kSchema3));
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id2, document_id3));
}

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest,
       FilterIsExactForSchemaTypePolymorphism) {
  // Add some irrelevant documents.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_schema2_)));
  DocumentId document_id2 = put_result2.new_document_id;

  // Create a person document and an artist document, where the artist should be
  // able to be interpreted as a person by polymorphism.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult person_put_result,
      document_store_->Put(document_util::CreateDocumentWrapper(
          DocumentBuilder()
              .SetKey("namespace", "person")
              .SetSchema("person")
              .Build())));
  DocumentId person_document_id = person_put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult artist_put_result,
      document_store_->Put(document_util::CreateDocumentWrapper(
          DocumentBuilder()
              .SetKey("namespace", "artist")
              .SetSchema("artist")
              .Build())));
  DocumentId artist_document_id = artist_put_result.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id1), DocHitInfo(document_id2),
      DocHitInfo(person_document_id), DocHitInfo(artist_document_id)};

  // Filters for the "person" type should NOT include the "artist" type, since
  // schema filters should not expand for polymorphism.
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  SearchSpecProto search_spec_1;
  search_spec_1.add_schema_type_filters("person");
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec_1, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator_1 =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());
  EXPECT_THAT(GetDocumentIds(filtered_iterator_1.get()),
              ElementsAre(person_document_id));

  // Filters for the "artist" type should not include the "person" type.
  original_iterator = std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  SearchSpecProto search_spec_2;
  search_spec_2.add_schema_type_filters("artist");
  predicate = GetFilterPredicateBySchemaAndNamespace(
      search_spec_2, *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator_2 =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());
  EXPECT_THAT(GetDocumentIds(filtered_iterator_2.get()),
              ElementsAre(artist_document_id));
}

TEST_F(DocHitInfoIteratorSchemaTypeFilterTest,
       FilterIsExactForSchemaTypeMultipleParentPolymorphism) {
  // Create an email and a message document.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult email_put_result,
      document_store_->Put(
          document_util::CreateDocumentWrapper(DocumentBuilder()
                                                   .SetKey("namespace", "email")
                                                   .SetSchema("email")
                                                   .Build())));
  DocumentId email_document_id = email_put_result.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult message_put_result,
      document_store_->Put(document_util::CreateDocumentWrapper(
          DocumentBuilder()
              .SetKey("namespace", "message")
              .SetSchema("message")
              .Build())));
  DocumentId message_document_id = message_put_result.new_document_id;

  // Create a emailMessage document, which the should be able to be interpreted
  // as both an email and a message by polymorphism.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult email_message_put_result,
      document_store_->Put(document_util::CreateDocumentWrapper(
          DocumentBuilder()
              .SetKey("namespace", "emailMessage")
              .SetSchema("emailMessage")
              .Build())));
  DocumentId email_message_document_id =
      email_message_put_result.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(email_document_id), DocHitInfo(message_document_id),
      DocHitInfo(email_message_document_id)};

  // Filters for the "email" type should NOT include the "emailMessage" type,
  // since schema filters should not expand for polymorphism.
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  SearchSpecProto search_spec_1;
  search_spec_1.add_schema_type_filters("email");
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec_1, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator_1 =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());
  EXPECT_THAT(GetDocumentIds(filtered_iterator_1.get()),
              ElementsAre(email_document_id));

  // Filters for the "message" type should NOT include the "emailMessage" type,
  // since schema filters should not expand for polymorphism.
  original_iterator = std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  SearchSpecProto search_spec_2;
  search_spec_2.add_schema_type_filters("message");
  predicate = GetFilterPredicateBySchemaAndNamespace(
      search_spec_2, *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator_2 =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());
  EXPECT_THAT(GetDocumentIds(filtered_iterator_2.get()),
              ElementsAre(message_document_id));

  // Filters for a irrelevant type should return nothing.
  original_iterator = std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);
  SearchSpecProto search_spec_3;
  search_spec_3.add_schema_type_filters("person");
  predicate = GetFilterPredicateBySchemaAndNamespace(
      search_spec_3, *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator_3 =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());
  EXPECT_THAT(GetDocumentIds(filtered_iterator_3.get()), IsEmpty());
}

class DocHitInfoIteratorExpirationFilterTest : public ::testing::Test {
 protected:
  DocHitInfoIteratorExpirationFilterTest()
      : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType(email_schema_))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get(), *feature_flags_));
    document_store_ = std::move(create_result.document_store);

    predicate_ = GetFilterPredicateBySchemaAndNamespace(
        SearchSpecProto::default_instance(), *document_store_, *schema_store_,
        fake_clock_.GetSystemTimeMilliseconds());
  }

  void TearDown() override {
    // Destroy objects before the whole directory is removed because they
    // persist data in the destructor.
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
  const std::string email_schema_ = "email";
  std::unique_ptr<DocumentFilterPredicate> predicate_;
};

TEST_F(DocHitInfoIteratorExpirationFilterTest, TtlZeroIsntFilteredOut) {
  // Arbitrary value
  fake_clock_.SetSystemTimeMilliseconds(100);
  predicate_ = GetFilterPredicateBySchemaAndNamespace(
      SearchSpecProto::default_instance(), *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                          schema_store_.get(), *feature_flags_));
  std::unique_ptr<DocumentStore> document_store =
      std::move(create_result.document_store);

  // Insert a document
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema(email_schema_)
                               .SetCreationTimestampMs(0)
                               .SetTtlMs(0)
                               .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(document_util::CreateDocumentWrapper(document)));
  DocumentId document_id1 = put_result1.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorExpirationFilterTest, BeforeTtlNotFilteredOut) {
  // Arbitrary value, but must be less than document's creation_timestamp + ttl
  fake_clock_.SetSystemTimeMilliseconds(50);
  predicate_ = GetFilterPredicateBySchemaAndNamespace(
      SearchSpecProto::default_instance(), *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                          schema_store_.get(), *feature_flags_));
  std::unique_ptr<DocumentStore> document_store =
      std::move(create_result.document_store);

  // Insert a document
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema(email_schema_)
                               .SetCreationTimestampMs(1)
                               .SetTtlMs(100)
                               .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(document_util::CreateDocumentWrapper(document)));
  DocumentId document_id1 = put_result1.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorExpirationFilterTest, EqualTtlFilteredOut) {
  // Current time is exactly the document's creation_timestamp + ttl
  fake_clock_.SetSystemTimeMilliseconds(150);
  predicate_ = GetFilterPredicateBySchemaAndNamespace(
      SearchSpecProto::default_instance(), *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                          schema_store_.get(), *feature_flags_));
  std::unique_ptr<DocumentStore> document_store =
      std::move(create_result.document_store);

  // Insert a document
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema(email_schema_)
                               .SetCreationTimestampMs(50)
                               .SetTtlMs(100)
                               .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(document_util::CreateDocumentWrapper(document)));
  DocumentId document_id1 = put_result1.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

TEST_F(DocHitInfoIteratorExpirationFilterTest, PastTtlFilteredOut) {
  // Arbitrary value, but must be greater than the document's
  // creation_timestamp + ttl
  fake_clock_.SetSystemTimeMilliseconds(151);
  predicate_ = GetFilterPredicateBySchemaAndNamespace(
      SearchSpecProto::default_instance(), *document_store_, *schema_store_,
      fake_clock_.GetSystemTimeMilliseconds());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                          schema_store_.get(), *feature_flags_));
  std::unique_ptr<DocumentStore> document_store =
      std::move(create_result.document_store);

  // Insert a document
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema(email_schema_)
                               .SetCreationTimestampMs(50)
                               .SetTtlMs(100)
                               .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(document_util::CreateDocumentWrapper(document)));
  DocumentId document_id1 = put_result1.new_document_id;

  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(document_id1)};
  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate_.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()), IsEmpty());
}

class DocHitInfoIteratorFilterTest : public ::testing::Test {
 protected:
  DocHitInfoIteratorFilterTest() : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    document1_namespace1_schema1_ = DocumentBuilder()
                                        .SetKey(namespace1_, "1")
                                        .SetSchema(schema1_)
                                        .SetCreationTimestampMs(100)
                                        .SetTtlMs(100)
                                        .Build();
    document2_namespace1_schema1_ = DocumentBuilder()
                                        .SetKey(namespace1_, "2")
                                        .SetSchema(schema1_)
                                        .SetCreationTimestampMs(100)
                                        .SetTtlMs(100)
                                        .Build();
    document3_namespace2_schema1_ = DocumentBuilder()
                                        .SetKey(namespace2_, "3")
                                        .SetSchema(schema1_)
                                        .SetCreationTimestampMs(100)
                                        .SetTtlMs(100)
                                        .Build();
    document4_namespace1_schema2_ = DocumentBuilder()
                                        .SetKey(namespace1_, "4")
                                        .SetSchema(schema2_)
                                        .SetCreationTimestampMs(100)
                                        .SetTtlMs(100)
                                        .Build();
    document5_namespace1_schema1_ = DocumentBuilder()
                                        .SetKey(namespace1_, "5")
                                        .SetSchema(schema1_)
                                        .SetCreationTimestampMs(1)
                                        .SetTtlMs(100)
                                        .Build();

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType(schema1_))
            .AddType(SchemaTypeConfigBuilder().SetType(schema2_))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get(), *feature_flags_));
    document_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    // Destroy objects before the whole directory is removed because they
    // persist data in the destructor.
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
  const std::string namespace1_ = "namespace1";
  const std::string namespace2_ = "namespace2";
  const std::string schema1_ = "email";
  const std::string schema2_ = "message";
  DocumentProto document1_namespace1_schema1_;
  DocumentProto document2_namespace1_schema1_;
  DocumentProto document3_namespace2_schema1_;
  DocumentProto document4_namespace1_schema2_;
  DocumentProto document5_namespace1_schema1_;
};

TEST_F(DocHitInfoIteratorFilterTest, CombineAllFiltersOk) {
  // Filters out document5 since it's expired
  fake_clock_.SetSystemTimeMilliseconds(199);

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      CreateDocumentStore(&filesystem_, test_dir_, &fake_clock_,
                          schema_store_.get(), *feature_flags_));
  std::unique_ptr<DocumentStore> document_store =
      std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store->Put(
          document_util::CreateDocumentWrapper(document2_namespace1_schema1_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store->Put(
          document_util::CreateDocumentWrapper(document3_namespace2_schema1_)));
  DocumentId document_id3 = put_result3.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result4,
      document_store->Put(
          document_util::CreateDocumentWrapper(document4_namespace1_schema2_)));
  DocumentId document_id4 = put_result4.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result5,
      document_store->Put(
          document_util::CreateDocumentWrapper(document5_namespace1_schema1_)));
  DocumentId document_id5 = put_result5.new_document_id;

  // Deletes document2, causing it to be filtered out
  ICING_ASSERT_OK(
      document_store->Delete(document2_namespace1_schema1_.namespace_(),
                             document2_namespace1_schema1_.uri(),
                             fake_clock_.GetSystemTimeMilliseconds()));

  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id1), DocHitInfo(document_id2),
      DocHitInfo(document_id3), DocHitInfo(document_id4),
      DocHitInfo(document_id5)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  SearchSpecProto search_spec;
  // Filters out document3 by namespace
  search_spec.add_namespace_filters(namespace1_);
  // Filters out document4 by schema type
  search_spec.add_schema_type_filters(schema1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocumentIds(filtered_iterator.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorFilterTest, SectionIdMasksArePopulatedCorrectly) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_namespace1_schema1_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document3_namespace2_schema1_)));
  DocumentId document_id3 = put_result3.new_document_id;

  SectionIdMask section_id_mask1 = 0b01001001;  // hits in sections 0, 3, 6
  SectionIdMask section_id_mask2 = 0b10010010;  // hits in sections 1, 4, 7
  SectionIdMask section_id_mask3 = 0b00100100;  // hits in sections 2, 5
  std::vector<SectionId> section_ids1 = {0, 3, 6};
  std::vector<SectionId> section_ids2 = {1, 4, 7};
  std::vector<SectionId> section_ids3 = {2, 5};
  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id1, section_id_mask1),
      DocHitInfo(document_id2, section_id_mask2),
      DocHitInfo(document_id3, section_id_mask3)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          SearchSpecProto::default_instance(), *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(GetDocHitInfos(filtered_iterator.get()),
              ElementsAre(EqualsDocHitInfo(document_id1, section_ids1),
                          EqualsDocHitInfo(document_id2, section_ids2),
                          EqualsDocHitInfo(document_id3, section_ids3)));
}

TEST_F(DocHitInfoIteratorFilterTest, GetCallStats) {
  DocHitInfoIterator::CallStats original_call_stats(
      /*num_leaf_advance_calls_lite_index_in=*/2,
      /*num_leaf_advance_calls_main_index_in=*/5,
      /*num_leaf_advance_calls_integer_index_in=*/3,
      /*num_leaf_advance_calls_no_index_in=*/1,
      /*num_blocks_inspected_in=*/4);  // arbitrary value
  auto original_iterator = std::make_unique<DocHitInfoIteratorDummy>();
  original_iterator->SetCallStats(original_call_stats);

  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          SearchSpecProto::default_instance(), *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  EXPECT_THAT(filtered_iterator->GetCallStats(), Eq(original_call_stats));
}

TEST_F(DocHitInfoIteratorFilterTest, TrimFilterIterator) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document1_namespace1_schema1_)));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document2_namespace1_schema1_)));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(document3_namespace2_schema1_)));
  DocumentId document_id3 = put_result3.new_document_id;

  // Build an interator tree like:
  //                Filter
  //                   |
  //                  AND
  //             /           \
  //          {1, 3}         {2}
  std::vector<DocHitInfo> left_vector = {DocHitInfo(document_id1),
                                         DocHitInfo(document_id3)};
  std::vector<DocHitInfo> right_vector = {DocHitInfo(document_id2)};

  std::unique_ptr<DocHitInfoIterator> left_iter =
      std::make_unique<DocHitInfoIteratorDummy>(left_vector);
  std::unique_ptr<DocHitInfoIterator> right_iter =
      std::make_unique<DocHitInfoIteratorDummy>(right_vector, "term", 10);

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(left_iter),
                                              std::move(right_iter));

  SearchSpecProto search_spec;
  // Filters out document3 by namespace
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  std::unique_ptr<DocHitInfoIterator> filtered_iterator =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_iterator), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The trimmed tree.
  //          Filter
  //             |
  //          {1, 3}
  ICING_ASSERT_OK_AND_ASSIGN(DocHitInfoIterator::TrimmedNode trimmed_node,
                             std::move(*filtered_iterator).TrimRightMostNode());
  EXPECT_THAT(trimmed_node.term_, Eq("term"));
  EXPECT_THAT(trimmed_node.term_start_index_, Eq(10));
  EXPECT_THAT(GetDocumentIds(trimmed_node.iterator_.get()),
              ElementsAre(document_id1));
}

TEST_F(DocHitInfoIteratorFilterTest,
       ApplyFilterReturnsOriginalWhenRootHandles) {
  SearchSpecProto search_spec_1;
  search_spec_1.add_schema_type_filters(schema1_);
  search_spec_1.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate_1 =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec_1, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  SearchSpecProto search_spec_2;
  search_spec_2.add_schema_type_filters(schema2_);
  search_spec_2.add_namespace_filters(namespace2_);
  std::unique_ptr<DocumentFilterPredicate> predicate_2 =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec_2, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply predicate_1 to an iterator that can handle filter internally.
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> original_root =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  DocHitInfoIteratorDummyHandlingFilter* original_root_ptr =
      original_root.get();
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate_1.get(),
          feature_flags_->enable_passing_filter_to_children());
  // The filter is applied internally, so the pointer should not change.
  EXPECT_THAT(new_root.get(), Eq(original_root_ptr));
  // new_root is not a DocHitInfoIteratorFilter.
  EXPECT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(nullptr));
  // Check that the predicate is applied correctly.
  EXPECT_THAT(original_root_ptr->document_filter_predicates(),
              ElementsAre(predicate_1.get()));

  // Apply predicate_2 to the same iterator.
  new_root = DocHitInfoIteratorFilter::ApplyFilter(
      std::move(new_root), predicate_2.get(),
      feature_flags_->enable_passing_filter_to_children());
  // The pointer should not change for the same reason.
  EXPECT_THAT(new_root.get(), Eq(original_root_ptr));
  // new_root is not a DocHitInfoIteratorFilter.
  EXPECT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(nullptr));
  // Check that the predicates are applied correctly.
  EXPECT_THAT(original_root_ptr->document_filter_predicates(),
              ElementsAre(predicate_1.get(), predicate_2.get()));
}

TEST_F(DocHitInfoIteratorFilterTest,
       ApplyFilterReturnsOriginalWhenAllChildrenHandle) {
  // Create an iterator tree:
  //                        AND (root)
  //                         /    \
  //       DummyHandlingFilter    DummyHandlingFilter
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> child1 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> child2 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  DocHitInfoIteratorDummyHandlingFilter* child1_ptr = child1.get();
  DocHitInfoIteratorDummyHandlingFilter* child2_ptr = child2.get();
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(child1),
                                              std::move(child2));
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Test that AND iterator can pass predicate through, and since all children
  // can handle the filter, the original iterator is returned.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(schema1_);
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  // We should get the same tree:
  //                        AND (root)
  //                         /    \
  //       DummyHandlingFilter    DummyHandlingFilter
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be handled by children, returning the original iterator.
  EXPECT_THAT(new_root.get(), Eq(original_root_ptr));
  EXPECT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(nullptr));
  // Verify the predicate was passed down to children.
  EXPECT_THAT(child1_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  EXPECT_THAT(child2_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
}

TEST_F(DocHitInfoIteratorFilterTest,
       ApplyFilterReturnsNewWhenSomeChildrenHandle) {
  // Create an iterator tree:
  //                        AND (root)
  //                         /    \
  //       DummyHandlingFilter    Dummy
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> child1 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  std::unique_ptr<DocHitInfoIteratorDummy> child2 =
      std::make_unique<DocHitInfoIteratorDummy>();
  DocHitInfoIteratorDummyHandlingFilter* child1_ptr = child1.get();
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(child1),
                                              std::move(child2));
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Create a predicate.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(schema1_);
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the filter. Since not all children can handle the filter, a new
  // filter iterator should be created at the top. However, DummyHandlingFilter
  // should still get the predicate and try to handle it internally.
  //
  // We should get a new tree:
  //                      Filter (root)
  //                           |
  //                          AND
  //                         /   \
  //       DummyHandlingFilter   Dummy
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be applied at the top level, so the pointer should
  // change and be of type DocHitInfoIteratorFilter.
  EXPECT_THAT(new_root.get(), Ne(original_root_ptr));
  ASSERT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  // Check that the original iterator is now the delegate.
  EXPECT_THAT(new_root->GetChildren(),
              ElementsAre(Pointee(Pointer(original_root_ptr))));
  // Verify the predicate was passed down to the child that can handle it.
  EXPECT_THAT(child1_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  // The other child (Dummy) doesn't store predicates, so no check needed there.
}

TEST_F(DocHitInfoIteratorFilterTest,
       ApplyFilterReturnsNewWhenNoChildrenHandle) {
  // Create an iterator tree:
  //                        AND (root)
  //                         /    \
  //                     Dummy    Dummy
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(
          std::make_unique<DocHitInfoIteratorDummy>(),
          std::make_unique<DocHitInfoIteratorDummy>());
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Create a predicate.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(schema1_);
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the filter. Since no children can handle the filter, a new filter
  // iterator should be created at the top.
  //
  // We should get a new tree:
  //                      Filter (root)
  //                           |
  //                          AND
  //                         /   \
  //                     Dummy   Dummy
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be applied at the top level, so the pointer should
  // change and be of type DocHitInfoIteratorFilter.
  EXPECT_THAT(new_root.get(), Ne(original_root_ptr));
  ASSERT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  // Check that the original iterator is now the delegate.
  EXPECT_THAT(new_root->GetChildren(),
              ElementsAre(Pointee(Pointer(original_root_ptr))));
}

TEST_F(DocHitInfoIteratorFilterTest,
       ApplyFilterReturnsNewWhenRootHasNoChildren) {
  // Create a root iterator that cannot handle filters and has no children.
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorDummy>();
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Create a predicate.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(schema1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the filter. Since the root cannot handle the filter and has no
  // children, a new filter iterator should be created at the top.
  //
  // We should get a new tree:
  //                      Filter (root)
  //                           |
  //                         Dummy
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be applied at the top level, so the pointer should
  // change and be of type DocHitInfoIteratorFilter.
  EXPECT_THAT(new_root.get(), Ne(original_root_ptr));
  ASSERT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  // Check that the original iterator is now the delegate.
  EXPECT_THAT(new_root->GetChildren(),
              ElementsAre(Pointee(Pointer(original_root_ptr))));
}

TEST_F(DocHitInfoIteratorFilterTest,
       ApplyFilterToIteratorThatCannotPassPredicateThrough) {
  // Create an iterator tree:
  //                NOT (root)
  //                 |
  //         DummyHandlingFilter
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter>
      iterator_handling_filter =
          std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  DocHitInfoIteratorDummyHandlingFilter* iterator_handling_filter_ptr =
      iterator_handling_filter.get();
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorNot>(
          std::move(iterator_handling_filter),
          /*document_id_limit=*/100);
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Test that NOT iterator cannot pass predicate through, and the filter is
  // applied at the top level.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(schema1_);
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  // We should get a new tree:
  //            Filter (root)
  //                 |
  //                NOT
  //                 |
  //         DummyHandlingFilter
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be applied at the top level, so the pointer should
  // change and is type of DocHitInfoIteratorFilter.
  EXPECT_THAT(new_root.get(), Ne(original_root_ptr));
  ASSERT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  // Check that the original iterator is now the delegate.
  EXPECT_THAT(new_root->GetChildren(),
              ElementsAre(Pointee(Pointer(original_root_ptr))));
  // Check that the predicate is not applied to the original iterator.
  EXPECT_THAT(iterator_handling_filter_ptr->document_filter_predicates(),
              IsEmpty());
}

TEST_F(DocHitInfoIteratorFilterTest, ApplyFilterDeepTreeMixedHandling) {
  // Create an iterator tree:
  //                        AND (root)
  //                         /    \
  //       DummyHandlingFilter    AND
  //                             /   \
  //                           Dummy  DummyHandlingFilter
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> child1 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  std::unique_ptr<DocHitInfoIteratorDummy> grandchild1 =
      std::make_unique<DocHitInfoIteratorDummy>();
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> grandchild2 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();

  DocHitInfoIteratorDummyHandlingFilter* child1_ptr = child1.get();
  DocHitInfoIteratorDummyHandlingFilter* grandchild2_ptr = grandchild2.get();

  std::unique_ptr<DocHitInfoIterator> child2 =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(grandchild1),
                                              std::move(grandchild2));
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(child1),
                                              std::move(child2));
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Create a predicate.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters(schema1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the filter. Since grandchild1 (Dummy) cannot handle the filter, a
  // new filter iterator should be created at the top. However, child1 and
  // grandchild2 should still get the predicate.
  //
  // We should get a new tree:
  //                      Filter (root)
  //                           |
  //                          AND
  //                         /    \
  //       DummyHandlingFilter    AND
  //                             /   \
  //                           Dummy  DummyHandlingFilter
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be applied at the top level.
  EXPECT_THAT(new_root.get(), Ne(original_root_ptr));
  ASSERT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  // Check that the original iterator is now the delegate.
  EXPECT_THAT(new_root->GetChildren(),
              ElementsAre(Pointee(Pointer(original_root_ptr))));
  // Verify the predicate was passed down to the children that can handle it.
  EXPECT_THAT(child1_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  EXPECT_THAT(grandchild2_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
}

TEST_F(DocHitInfoIteratorFilterTest, ApplyFilterNestedNonPassingIterator) {
  // Create an iterator tree:
  //                        AND (root)
  //                         /    \
  //       DummyHandlingFilter    NOT
  //                               |
  //                       DummyHandlingFilter
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> child1 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> grandchild1 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();

  DocHitInfoIteratorDummyHandlingFilter* child1_ptr = child1.get();
  DocHitInfoIteratorDummyHandlingFilter* grandchild1_ptr = grandchild1.get();

  std::unique_ptr<DocHitInfoIterator> child2 =
      std::make_unique<DocHitInfoIteratorNot>(std::move(grandchild1),
                                              /*document_id_limit=*/100);
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(child1),
                                              std::move(child2));
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Create a predicate.
  SearchSpecProto search_spec;
  search_spec.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the filter. Since child2 (NOT) cannot pass the filter through, a
  // new filter iterator should be created at the top. child1 should still get
  // the predicate, but grandchild1 (under NOT) should not.
  //
  // We should get a new tree:
  //                      Filter (root)
  //                           |
  //                          AND
  //                         /    \
  //       DummyHandlingFilter    NOT
  //                               |
  //                       DummyHandlingFilter
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // The filter should be applied at the top level.
  EXPECT_THAT(new_root.get(), Ne(original_root_ptr));
  ASSERT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  // Check that the original iterator is now the delegate.
  EXPECT_THAT(new_root->GetChildren(),
              ElementsAre(Pointee(Pointer(original_root_ptr))));
  // Verify the predicate was passed down only to child1.
  EXPECT_THAT(child1_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  // Verify the predicate was *not* passed down to grandchild1.
  EXPECT_THAT(grandchild1_ptr->document_filter_predicates(), IsEmpty());
}

TEST_F(DocHitInfoIteratorFilterTest, ApplyFilterComplexTreeAllHandling) {
  // Create leaf nodes that can handle filters.
  auto leaf1 = std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  auto leaf2 = std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  auto leaf3 = std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  auto leaf4 = std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  DocHitInfoIteratorDummyHandlingFilter* leaf1_ptr = leaf1.get();
  DocHitInfoIteratorDummyHandlingFilter* leaf2_ptr = leaf2.get();
  DocHitInfoIteratorDummyHandlingFilter* leaf3_ptr = leaf3.get();
  DocHitInfoIteratorDummyHandlingFilter* leaf4_ptr = leaf4.get();

  // Build a complex tree: AND(OR(leaf1, leaf2), AND(leaf3, leaf4))
  auto or_node = std::make_unique<DocHitInfoIteratorOr>(std::move(leaf1),
                                                        std::move(leaf2));

  auto and_node2 = std::make_unique<DocHitInfoIteratorAnd>(std::move(leaf3),
                                                           std::move(leaf4));

  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(or_node),
                                              std::move(and_node2));
  DocHitInfoIterator* original_root_ptr = original_root.get();

  // Create a predicate.
  SearchSpecProto search_spec;
  search_spec.add_namespace_filters(namespace1_);
  search_spec.add_schema_type_filters(schema1_);
  std::unique_ptr<DocumentFilterPredicate> predicate =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the filter.
  std::unique_ptr<DocHitInfoIterator> new_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate.get(),
          feature_flags_->enable_passing_filter_to_children());

  // Since all leaf nodes can handle the filter, the original root should be
  // returned without a Filter wrapper.
  EXPECT_THAT(new_root.get(), Eq(original_root_ptr));
  EXPECT_THAT(new_root.get(),
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(nullptr));

  // Verify the predicate was passed down to all leaf nodes.
  EXPECT_THAT(leaf1_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  EXPECT_THAT(leaf2_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  EXPECT_THAT(leaf3_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
  EXPECT_THAT(leaf4_ptr->document_filter_predicates(),
              ElementsAre(predicate.get()));
}

TEST_F(DocHitInfoIteratorFilterTest, ApplyMultipleFiltersSequentially) {
  // Create an iterator tree:
  //                        AND (root)
  //                         /    \
  //       DummyHandlingFilter    Dummy
  std::unique_ptr<DocHitInfoIteratorDummyHandlingFilter> child1 =
      std::make_unique<DocHitInfoIteratorDummyHandlingFilter>();
  std::unique_ptr<DocHitInfoIteratorDummy> child2 =
      std::make_unique<DocHitInfoIteratorDummy>();
  DocHitInfoIteratorDummyHandlingFilter* child1_ptr = child1.get();
  std::unique_ptr<DocHitInfoIterator> original_root =
      std::make_unique<DocHitInfoIteratorAnd>(std::move(child1),
                                              std::move(child2));

  // Create predicates.
  SearchSpecProto search_spec_1;
  search_spec_1.add_schema_type_filters(schema1_);
  std::unique_ptr<DocumentFilterPredicate> predicate_1 =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec_1, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());
  SearchSpecProto search_spec_2;
  search_spec_2.add_namespace_filters(namespace1_);
  std::unique_ptr<DocumentFilterPredicate> predicate_2 =
      GetFilterPredicateBySchemaAndNamespace(
          search_spec_2, *document_store_, *schema_store_,
          fake_clock_.GetSystemTimeMilliseconds());

  // Apply the first filter. Since child2 cannot handle it, a Filter iterator
  // is added. child1 gets predicate_1.
  // Tree: Filter1(AND(DummyHandlingFilter, Dummy))
  std::unique_ptr<DocHitInfoIterator> current_root =
      DocHitInfoIteratorFilter::ApplyFilter(
          std::move(original_root), predicate_1.get(),
          feature_flags_->enable_passing_filter_to_children());
  DocHitInfoIterator* filter1_ptr = current_root.get();
  ASSERT_THAT(filter1_ptr,
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  EXPECT_THAT(child1_ptr->document_filter_predicates(),
              ElementsAre(predicate_1.get()));

  // Apply the second filter. A new Filter2 iterator is added on top. child1
  // gets predicate_2. Tree: Filter2(Filter1(AND(DummyHandlingFilter, Dummy)))
  current_root = DocHitInfoIteratorFilter::ApplyFilter(
      std::move(current_root), predicate_2.get(),
      feature_flags_->enable_passing_filter_to_children());
  DocHitInfoIterator* filter2_ptr = current_root.get();
  ASSERT_THAT(filter2_ptr,
              WhenDynamicCastTo<DocHitInfoIteratorFilter*>(Ne(nullptr)));
  EXPECT_THAT(filter2_ptr, Ne(filter1_ptr));  // New filter added

  // Check delegate of Filter2 is Filter1
  ASSERT_THAT(current_root->GetChildren(),
              ElementsAre(Pointee(Pointer(filter1_ptr))));

  // Verify child1 now has both predicates.
  EXPECT_THAT(child1_ptr->document_filter_predicates(),
              ElementsAre(predicate_1.get(), predicate_2.get()));
}

}  // namespace

}  // namespace lib
}  // namespace icing
