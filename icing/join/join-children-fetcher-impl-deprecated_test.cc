// Copyright (C) 2023 Google LLC
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

#include "icing/join/join-children-fetcher-impl-deprecated.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/join/join-processor.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(JoinChildrenFetcherImplDeprecatedTest, FetchQualifiedIdJoinChildren) {
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");

  std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      map_joinable_qualified_id;
  DocumentId parent_doc_id = 0;
  ScoredDocumentHit child1(/*document_id=*/1, kSectionIdMaskNone,
                           /*score=*/1.0);
  ScoredDocumentHit child2(/*document_id=*/2, kSectionIdMaskNone,
                           /*score=*/2.0);
  map_joinable_qualified_id[parent_doc_id].push_back(child1);
  map_joinable_qualified_id[parent_doc_id].push_back(child2);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplDeprecated> fetcher,
      JoinChildrenFetcherImplDeprecated::Create(
          join_spec, std::move(map_joinable_qualified_id)));
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<ScoredDocumentHit> children,
                             fetcher->GetChildren(parent_doc_id));
  EXPECT_THAT(children, ElementsAre(EqualsScoredDocumentHit(child1),
                                    EqualsScoredDocumentHit(child2)));
}

TEST(JoinChildrenFetcherImplDeprecatedTest, FetchJoinEmptyChildren) {
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");

  DocumentId parent_doc_id = 0;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplDeprecated> fetcher,
      JoinChildrenFetcherImplDeprecated::Create(
          join_spec,
          /*map_joinable_qualified_id=*/{}));
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<ScoredDocumentHit> children,
                             fetcher->GetChildren(parent_doc_id));
  EXPECT_THAT(children, IsEmpty());
}

TEST(JoinChildrenFetcherImplDeprecatedTest, UnsupportedJoin) {
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression("name");
  join_spec.set_child_property_expression("sender");

  EXPECT_THAT(JoinChildrenFetcherImplDeprecated::Create(
                  join_spec, /*map_joinable_qualified_id=*/{}),
              StatusIs(libtextclassifier3::StatusCode::UNIMPLEMENTED));
}

}  // namespace

}  // namespace lib
}  // namespace icing
