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

#include "icing/util/tokenized-document.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/icu-data-file-helper.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::SizeIs;

// schema types
static constexpr std::string_view kFakeType = "FakeType";

// Indexable properties and section Id. Section Id is determined by the
// lexicographical order of indexable property path.
static constexpr std::string_view kIndexableIntegerProperty1 =
    "indexableInteger1";
static constexpr std::string_view kIndexableIntegerProperty2 =
    "indexableInteger2";
static constexpr std::string_view kIndexableVectorProperty1 =
    "indexableVector1";
static constexpr std::string_view kIndexableVectorProperty2 =
    "indexableVector2";
static constexpr std::string_view kStringExactProperty = "stringExact";
static constexpr std::string_view kStringPrefixProperty = "stringPrefix";

static constexpr SectionId kIndexableInteger1SectionId = 0;
static constexpr SectionId kIndexableInteger2SectionId = 1;
static constexpr SectionId kIndexableVector1SectionId = 2;
static constexpr SectionId kIndexableVector2SectionId = 3;
static constexpr SectionId kStringExactSectionId = 4;
static constexpr SectionId kStringPrefixSectionId = 5;

// Joinable properties and joinable property id. Joinable property id is
// determined by the lexicographical order of joinable property path.
static constexpr std::string_view kQualifiedId1 = "qualifiedId1";
static constexpr std::string_view kQualifiedId2 = "qualifiedId2";

static constexpr JoinablePropertyId kQualifiedId1JoinablePropertyId = 0;
static constexpr JoinablePropertyId kQualifiedId2JoinablePropertyId = 1;

const SectionMetadata kIndexableInteger1SectionMetadata(
    kIndexableInteger1SectionId, TYPE_INT64, TOKENIZER_NONE, TERM_MATCH_UNKNOWN,
    NUMERIC_MATCH_RANGE, EMBEDDING_INDEXING_UNKNOWN, QUANTIZATION_TYPE_NONE,
    std::string(kIndexableIntegerProperty1));

const SectionMetadata kIndexableInteger2SectionMetadata(
    kIndexableInteger2SectionId, TYPE_INT64, TOKENIZER_NONE, TERM_MATCH_UNKNOWN,
    NUMERIC_MATCH_RANGE, EMBEDDING_INDEXING_UNKNOWN, QUANTIZATION_TYPE_NONE,
    std::string(kIndexableIntegerProperty2));

const SectionMetadata kIndexableVector1SectionMetadata(
    kIndexableVector1SectionId, TYPE_VECTOR, TOKENIZER_NONE, TERM_MATCH_UNKNOWN,
    NUMERIC_MATCH_UNKNOWN, EMBEDDING_INDEXING_LINEAR_SEARCH,
    QUANTIZATION_TYPE_NONE, std::string(kIndexableVectorProperty1));

const SectionMetadata kIndexableVector2SectionMetadata(
    kIndexableVector2SectionId, TYPE_VECTOR, TOKENIZER_NONE, TERM_MATCH_UNKNOWN,
    NUMERIC_MATCH_UNKNOWN, EMBEDDING_INDEXING_LINEAR_SEARCH,
    QUANTIZATION_TYPE_QUANTIZE_8_BIT, std::string(kIndexableVectorProperty2));

const SectionMetadata kStringExactSectionMetadata(
    kStringExactSectionId, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_EXACT,
    NUMERIC_MATCH_UNKNOWN, EMBEDDING_INDEXING_UNKNOWN, QUANTIZATION_TYPE_NONE,
    std::string(kStringExactProperty));

const SectionMetadata kStringPrefixSectionMetadata(
    kStringPrefixSectionId, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_PREFIX,
    NUMERIC_MATCH_UNKNOWN, EMBEDDING_INDEXING_UNKNOWN, QUANTIZATION_TYPE_NONE,
    std::string(kStringPrefixProperty));

const JoinablePropertyMetadata kQualifiedId1JoinablePropertyMetadata(
    kQualifiedId1JoinablePropertyId, TYPE_STRING,
    JOINABLE_VALUE_TYPE_QUALIFIED_ID, DELETE_PROPAGATION_TYPE_PROPAGATE_FROM,
    std::string(kQualifiedId1));

const JoinablePropertyMetadata kQualifiedId2JoinablePropertyMetadata(
    kQualifiedId2JoinablePropertyId, TYPE_STRING,
    JOINABLE_VALUE_TYPE_QUALIFIED_ID, DELETE_PROPAGATION_TYPE_NONE,
    std::string(kQualifiedId2));

// Other non-indexable/joinable properties.
constexpr std::string_view kUnindexedStringProperty = "unindexedString";
constexpr std::string_view kUnindexedIntegerProperty = "unindexedInteger";
constexpr std::string_view kUnindexedVectorProperty = "unindexedVector";

class TokenizedDocumentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    test_dir_ = GetTestTempDir() + "/icing";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

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

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType(kFakeType)
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kUnindexedStringProperty)
                                     .SetDataType(TYPE_STRING)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kUnindexedIntegerProperty)
                                     .SetDataType(TYPE_INT64)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kUnindexedVectorProperty)
                                     .SetDataType(TYPE_VECTOR)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kIndexableIntegerProperty1)
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kIndexableIntegerProperty2)
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName(kIndexableVectorProperty1)
                            .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH)
                            .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName(kIndexableVectorProperty2)
                            .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH,
                                               QUANTIZATION_TYPE_QUANTIZE_8_BIT)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kStringExactProperty)
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kStringPrefixProperty)
                                     .SetDataTypeString(TERM_MATCH_PREFIX,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kQualifiedId1)
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kQualifiedId2)
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_NONE)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false,
        /*allow_circular_schema_definitions=*/false));
  }

  void TearDown() override {
    schema_store_.reset();

    // Check that the schema store directory is the *only* directory in the
    // schema_store_dir_. IOW, ensure that all temporary directories have been
    // properly cleaned up.
    std::vector<std::string> sub_dirs;
    ASSERT_TRUE(filesystem_.ListDirectory(test_dir_.c_str(), &sub_dirs));
    ASSERT_THAT(sub_dirs, ElementsAre("schema_store"));

    // Finally, clean everything up.
    ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(test_dir_.c_str()));
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
};

TEST_F(TokenizedDocumentTest, CreateAll) {
  PropertyProto::VectorProto vector1;
  vector1.set_model_signature("my_model1");
  vector1.add_values(1.0f);
  vector1.add_values(2.0f);
  PropertyProto::VectorProto vector2;
  vector2.set_model_signature("my_model2");
  vector2.add_values(-1.0f);
  vector2.add_values(-2.0f);
  vector2.add_values(-3.0f);

  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kUnindexedStringProperty),
                             "hello world unindexed")
          .AddStringProperty(std::string(kStringExactProperty), "test foo",
                             "test bar", "test baz")
          .AddStringProperty(std::string(kStringPrefixProperty), "foo bar baz")
          .AddInt64Property(std::string(kUnindexedIntegerProperty), 789)
          .AddInt64Property(std::string(kIndexableIntegerProperty1), 1, 2, 3)
          .AddInt64Property(std::string(kIndexableIntegerProperty2), 456)
          .AddVectorProperty(std::string(kUnindexedVectorProperty), vector1)
          .AddVectorProperty(std::string(kIndexableVectorProperty1), vector1,
                             vector2)
          .AddVectorProperty(std::string(kIndexableVectorProperty2), vector1)
          .AddStringProperty(std::string(kQualifiedId1), "pkg$db/ns#uri1")
          .AddStringProperty(std::string(kQualifiedId2), "pkg$db/ns#uri2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(9));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), SizeIs(2));
  EXPECT_THAT(tokenized_document.tokenized_string_sections().at(0).metadata,
              Eq(kStringExactSectionMetadata));
  EXPECT_THAT(
      tokenized_document.tokenized_string_sections().at(0).token_sequence,
      ElementsAre("test", "foo", "test", "bar", "test", "baz"));
  EXPECT_THAT(tokenized_document.tokenized_string_sections().at(1).metadata,
              Eq(kStringPrefixSectionMetadata));
  EXPECT_THAT(
      tokenized_document.tokenized_string_sections().at(1).token_sequence,
      ElementsAre("foo", "bar", "baz"));

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), SizeIs(2));
  EXPECT_THAT(tokenized_document.integer_sections().at(0).metadata,
              Eq(kIndexableInteger1SectionMetadata));
  EXPECT_THAT(tokenized_document.integer_sections().at(0).content,
              ElementsAre(1, 2, 3));
  EXPECT_THAT(tokenized_document.integer_sections().at(1).metadata,
              Eq(kIndexableInteger2SectionMetadata));
  EXPECT_THAT(tokenized_document.integer_sections().at(1).content,
              ElementsAre(456));

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), SizeIs(2));
  EXPECT_THAT(tokenized_document.vector_sections().at(0).metadata,
              Eq(kIndexableVector1SectionMetadata));
  EXPECT_THAT(tokenized_document.vector_sections().at(0).content,
              ElementsAre(EqualsProto(vector1), EqualsProto(vector2)));
  EXPECT_THAT(tokenized_document.vector_sections().at(1).metadata,
              Eq(kIndexableVector2SectionMetadata));
  EXPECT_THAT(tokenized_document.vector_sections().at(1).content,
              ElementsAre(EqualsProto(vector1)));

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), SizeIs(2));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(0).metadata,
              Eq(kQualifiedId1JoinablePropertyMetadata));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(0).values,
              ElementsAre("pkg$db/ns#uri1"));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(1).metadata,
              Eq(kQualifiedId2JoinablePropertyMetadata));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(1).values,
              ElementsAre("pkg$db/ns#uri2"));
}

TEST_F(TokenizedDocumentTest, CreateNoIndexableIntegerProperties) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddInt64Property(std::string(kUnindexedIntegerProperty), 789)
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateMultipleIndexableIntegerProperties) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddInt64Property(std::string(kUnindexedIntegerProperty), 789)
          .AddInt64Property(std::string(kIndexableIntegerProperty1), 1, 2, 3)
          .AddInt64Property(std::string(kIndexableIntegerProperty2), 456)
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), SizeIs(2));
  EXPECT_THAT(tokenized_document.integer_sections().at(0).metadata,
              Eq(kIndexableInteger1SectionMetadata));
  EXPECT_THAT(tokenized_document.integer_sections().at(0).content,
              ElementsAre(1, 2, 3));
  EXPECT_THAT(tokenized_document.integer_sections().at(1).metadata,
              Eq(kIndexableInteger2SectionMetadata));
  EXPECT_THAT(tokenized_document.integer_sections().at(1).content,
              ElementsAre(456));

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateNoIndexableStringProperties) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kUnindexedStringProperty),
                             "hello world unindexed")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateMultipleIndexableStringProperties) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kUnindexedStringProperty),
                             "hello world unindexed")
          .AddStringProperty(std::string(kStringExactProperty), "test foo",
                             "test bar", "test baz")
          .AddStringProperty(std::string(kStringPrefixProperty), "foo bar baz")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(9));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), SizeIs(2));
  EXPECT_THAT(tokenized_document.tokenized_string_sections().at(0).metadata,
              Eq(kStringExactSectionMetadata));
  EXPECT_THAT(
      tokenized_document.tokenized_string_sections().at(0).token_sequence,
      ElementsAre("test", "foo", "test", "bar", "test", "baz"));
  EXPECT_THAT(tokenized_document.tokenized_string_sections().at(1).metadata,
              Eq(kStringPrefixSectionMetadata));
  EXPECT_THAT(
      tokenized_document.tokenized_string_sections().at(1).token_sequence,
      ElementsAre("foo", "bar", "baz"));

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateNoIndexableVectorProperties) {
  PropertyProto::VectorProto vector;
  vector.set_model_signature("my_model");
  vector.add_values(1.0f);

  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddVectorProperty(std::string(kUnindexedVectorProperty), vector)
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateMultipleIndexableVectorProperties) {
  PropertyProto::VectorProto vector1;
  vector1.set_model_signature("my_model1");
  vector1.add_values(1.0f);
  vector1.add_values(2.0f);
  PropertyProto::VectorProto vector2;
  vector2.set_model_signature("my_model2");
  vector2.add_values(-1.0f);
  vector2.add_values(-2.0f);
  vector2.add_values(-3.0f);

  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddVectorProperty(std::string(kUnindexedVectorProperty), vector1)
          .AddVectorProperty(std::string(kIndexableVectorProperty1), vector1,
                             vector2)
          .AddVectorProperty(std::string(kIndexableVectorProperty2), vector1)
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), SizeIs(2));
  EXPECT_THAT(tokenized_document.vector_sections().at(0).metadata,
              Eq(kIndexableVector1SectionMetadata));
  EXPECT_THAT(tokenized_document.vector_sections().at(0).content,
              ElementsAre(EqualsProto(vector1), EqualsProto(vector2)));
  EXPECT_THAT(tokenized_document.vector_sections().at(1).metadata,
              Eq(kIndexableVector2SectionMetadata));
  EXPECT_THAT(tokenized_document.vector_sections().at(1).content,
              ElementsAre(EqualsProto(vector1)));

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateNoJoinQualifiedIdProperties) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kUnindexedStringProperty),
                             "hello world unindexed")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), IsEmpty());
}

TEST_F(TokenizedDocumentTest, CreateMultipleJoinQualifiedIdProperties) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kUnindexedStringProperty),
                             "hello world unindexed")
          .AddStringProperty(std::string(kQualifiedId1), "pkg$db/ns#uri1")
          .AddStringProperty(std::string(kQualifiedId2), "pkg$db/ns#uri2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                document));

  EXPECT_THAT(tokenized_document.document(), EqualsProto(document));
  EXPECT_THAT(tokenized_document.num_string_tokens(), Eq(0));

  // string sections
  EXPECT_THAT(tokenized_document.tokenized_string_sections(), IsEmpty());

  // integer sections
  EXPECT_THAT(tokenized_document.integer_sections(), IsEmpty());

  // vector sections
  EXPECT_THAT(tokenized_document.vector_sections(), IsEmpty());

  // Qualified id join properties
  EXPECT_THAT(tokenized_document.qualified_id_join_properties(), SizeIs(2));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(0).metadata,
              Eq(kQualifiedId1JoinablePropertyMetadata));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(0).values,
              ElementsAre("pkg$db/ns#uri1"));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(1).metadata,
              Eq(kQualifiedId2JoinablePropertyMetadata));
  EXPECT_THAT(tokenized_document.qualified_id_join_properties().at(1).values,
              ElementsAre("pkg$db/ns#uri2"));
}

}  // namespace

}  // namespace lib
}  // namespace icing
