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

#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;

class DocHitInfoIteratorSectionRestrictTest : public ::testing::Test {
 protected:
  DocHitInfoIteratorSectionRestrictTest()
      : test_dir_(GetTestTempDir() + "/icing") {}

  void SetUp() override {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    document_ =
        DocumentBuilder().SetKey("namespace", "uri").SetSchema("email").Build();

    auto type_config = schema_.add_types();
    type_config->set_schema_type("email");

    // Add an indexed property so we generate section metadata on it
    auto property = type_config->add_properties();
    property->set_property_name(indexed_property_);
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    property->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);

    // First and only indexed property, so it gets the first id of 0
    indexed_section_id_ = 0;

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
    ICING_ASSERT_OK(schema_store_->SetSchema(schema_));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                              schema_store_.get()));
    document_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  const Filesystem filesystem_;
  const std::string test_dir_;
  SchemaProto schema_;
  DocumentProto document_;
  const std::string indexed_property_ = "subject";
  int indexed_section_id_;
  FakeClock fake_clock_;
};

TEST_F(DocHitInfoIteratorSectionRestrictTest, EmptyOriginalIterator) {
  std::unique_ptr<DocHitInfoIterator> original_iterator_empty =
      std::make_unique<DocHitInfoIteratorDummy>();

  DocHitInfoIteratorSectionRestrict filtered_iterator(
      std::move(original_iterator_empty), document_store_.get(),
      schema_store_.get(), /*target_section=*/"");

  EXPECT_THAT(GetDocumentIds(&filtered_iterator), IsEmpty());
}

TEST_F(DocHitInfoIteratorSectionRestrictTest, IncludesHitWithMatchingSection) {
  // Populate the DocumentStore's FilterCache with this document's data
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(document_));

  SectionIdMask section_id_mask = 1U << indexed_section_id_;

  // Create a hit that was found in the indexed section
  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id, section_id_mask)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Filtering for the indexed section name should get a result
  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      indexed_property_);

  EXPECT_THAT(GetDocumentIds(&section_restrict_iterator),
              ElementsAre(document_id));
}

TEST_F(DocHitInfoIteratorSectionRestrictTest, NoMatchingDocumentFilterData) {
  // Create a hit with a document id that doesn't exist in the DocumentStore yet
  std::vector<DocHitInfo> doc_hit_infos = {DocHitInfo(/*document_id_in=*/0)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Filtering for the indexed section name should get a result
  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      /*target_section=*/"");

  EXPECT_THAT(GetDocumentIds(&section_restrict_iterator), IsEmpty());
}

TEST_F(DocHitInfoIteratorSectionRestrictTest,
       DoesntIncludeHitWithWrongSectionName) {
  // Populate the DocumentStore's FilterCache with this document's data
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(document_));

  SectionIdMask section_id_mask = 1U << indexed_section_id_;

  // Create a hit that was found in the indexed section
  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id, section_id_mask)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  // Filtering for the indexed section name should get a result
  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      "some_section_name");

  EXPECT_THAT(GetDocumentIds(&section_restrict_iterator), IsEmpty());
}

TEST_F(DocHitInfoIteratorSectionRestrictTest,
       DoesntIncludeHitWithNoSectionIds) {
  // Populate the DocumentStore's FilterCache with this document's data
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(document_));

  // Create a hit that doesn't exist in any sections, so it shouldn't match any
  // section filters
  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id, kSectionIdMaskNone)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      indexed_property_);

  EXPECT_THAT(GetDocumentIds(&section_restrict_iterator), IsEmpty());
}

TEST_F(DocHitInfoIteratorSectionRestrictTest,
       DoesntIncludeHitWithDifferentSectionId) {
  // Populate the DocumentStore's FilterCache with this document's data
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(document_));

  // Anything that's not 0, which is the indexed property
  SectionId not_matching_section_id = 2;

  // Create a hit that exists in a different section, so it shouldn't match any
  // section filters
  std::vector<DocHitInfo> doc_hit_infos = {
      DocHitInfo(document_id, kSectionIdMaskNone << not_matching_section_id)};

  std::unique_ptr<DocHitInfoIterator> original_iterator =
      std::make_unique<DocHitInfoIteratorDummy>(doc_hit_infos);

  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      indexed_property_);

  EXPECT_THAT(GetDocumentIds(&section_restrict_iterator), IsEmpty());
}

TEST_F(DocHitInfoIteratorSectionRestrictTest, GetNumBlocksInspected) {
  auto original_iterator = std::make_unique<DocHitInfoIteratorDummy>();
  original_iterator->SetNumBlocksInspected(5);

  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      /*target_section=*/"");

  EXPECT_THAT(section_restrict_iterator.GetNumBlocksInspected(), Eq(5));
}

TEST_F(DocHitInfoIteratorSectionRestrictTest, GetNumLeafAdvanceCalls) {
  auto original_iterator = std::make_unique<DocHitInfoIteratorDummy>();
  original_iterator->SetNumLeafAdvanceCalls(6);

  DocHitInfoIteratorSectionRestrict section_restrict_iterator(
      std::move(original_iterator), document_store_.get(), schema_store_.get(),
      /*target_section=*/"");

  EXPECT_THAT(section_restrict_iterator.GetNumLeafAdvanceCalls(), Eq(6));
}

}  // namespace

}  // namespace lib
}  // namespace icing
