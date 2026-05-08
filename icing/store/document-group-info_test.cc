// Copyright (C) 2026 Google LLC
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

#include "icing/store/document-group-info.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

TEST(DocumentGroupInfoTest, AddDocument) {
  DocumentGroupInfo document_group_info;
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 1});
  EXPECT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1)))));

  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 2});
  EXPECT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2)))));

  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 3});
  EXPECT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3)))));

  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri1",
                       .document_id = 4});
  EXPECT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));

  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri3",
                       .document_id = 5});
  EXPECT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));
}

TEST(DocumentGroupInfoTest, Merge) {
  DocumentGroupInfo document_group_info1;
  document_group_info1.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 1});
  document_group_info1.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 2});
  document_group_info1.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 3});
  document_group_info1.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri1",
                       .document_id = 4});
  document_group_info1.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri3",
                       .document_id = 5});
  ASSERT_THAT(
      document_group_info1.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));

  DocumentGroupInfo document_group_info2;
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri4",
                       .document_id = 6});
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 7});
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri2",
                       .document_id = 8});
  ASSERT_THAT(
      document_group_info2.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri4", 6))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri2", 7))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri2", 8)))));

  document_group_info1.Merge(std::move(document_group_info2));
  EXPECT_THAT(
      document_group_info1.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5),
                                            EqualsDocumentUriId("uri4", 6))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3),
                                            EqualsDocumentUriId("uri2", 7))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4),
                                            EqualsDocumentUriId("uri2", 8)))));
}

TEST(DocumentGroupInfoTest, Merge_withAnEmptyMap) {
  DocumentGroupInfo document_group_info;
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 1});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 2});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 3});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri1",
                       .document_id = 4});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri3",
                       .document_id = 5});
  ASSERT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));

  document_group_info.Merge(DocumentGroupInfo());
  EXPECT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));
}

TEST(DocumentGroupInfoTest, Merge_selfEmpty) {
  DocumentGroupInfo document_group_info1;

  DocumentGroupInfo document_group_info2;
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 1});
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 2});
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 3});
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri1",
                       .document_id = 4});
  document_group_info2.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri3",
                       .document_id = 5});
  ASSERT_THAT(
      document_group_info2.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));

  document_group_info1.Merge(std::move(document_group_info2));
  EXPECT_THAT(
      document_group_info1.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));
}

TEST(DocumentGroupInfoTest, GetTotalNumDocs) {
  DocumentGroupInfo document_group_info;
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 1});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 2});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 3});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri1",
                       .document_id = 4});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri3",
                       .document_id = 5});
  ASSERT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));

  EXPECT_THAT(document_group_info.GetTotalNumDocs(), 5);
}

TEST(DocumentGroupInfoTest, GetTotalNumDocs_emptyMap) {
  DocumentGroupInfo document_group_info;
  EXPECT_THAT(document_group_info.GetTotalNumDocs(), 0);
}

TEST(DocumentGroupInfoTest, GetAllDocumentIds) {
  DocumentGroupInfo document_group_info;
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 1});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri2",
                       .document_id = 2});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema2",
                       .name_space = "namespace1",
                       .uri = "uri1",
                       .document_id = 3});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace2",
                       .uri = "uri1",
                       .document_id = 4});
  document_group_info.AddDocument(
      DocumentMetadata{.schema_type_name = "schema1",
                       .name_space = "namespace1",
                       .uri = "uri3",
                       .document_id = 5});
  ASSERT_THAT(
      document_group_info.Get(),
      UnorderedElementsAre(Pair(EqualsDocumentGroupKey("schema1", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 1),
                                            EqualsDocumentUriId("uri2", 2),
                                            EqualsDocumentUriId("uri3", 5))),
                           Pair(EqualsDocumentGroupKey("schema2", "namespace1"),
                                ElementsAre(EqualsDocumentUriId("uri1", 3))),
                           Pair(EqualsDocumentGroupKey("schema1", "namespace2"),
                                ElementsAre(EqualsDocumentUriId("uri1", 4)))));

  EXPECT_THAT(document_group_info.GetAllDocumentIds(),
              UnorderedElementsAre(1, 2, 3, 4, 5));
}

TEST(DocumentGroupInfoTest, GetAllDocumentIds_emptyMap) {
  DocumentGroupInfo document_group_info;
  EXPECT_THAT(document_group_info.GetAllDocumentIds(), IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
