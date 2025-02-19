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

#include "icing/index/embed/embedding-hit.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/hit/hit.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsFalse;

static constexpr DocumentId kSomeDocumentId = 24;
static constexpr SectionId kSomeSectionid = 5;
static constexpr uint32_t kSomeLocation = 123;

TEST(EmbeddingHitTest, Accessors) {
  BasicHit basic_hit(kSomeSectionid, kSomeDocumentId);
  EmbeddingHit embedding_hit(basic_hit, kSomeLocation);
  EXPECT_THAT(embedding_hit.basic_hit(), Eq(basic_hit));
  EXPECT_THAT(embedding_hit.location(), Eq(kSomeLocation));
}

TEST(EmbeddingHitTest, Invalid) {
  EmbeddingHit invalid_hit(EmbeddingHit::kInvalidValue);
  EXPECT_THAT(invalid_hit.is_valid(), IsFalse());

  // Also make sure the invalid EmbeddingHit contains an invalid document id.
  EXPECT_THAT(invalid_hit.basic_hit().document_id(), Eq(kInvalidDocumentId));
  EXPECT_THAT(invalid_hit.basic_hit().section_id(), Eq(kMinSectionId));
  EXPECT_THAT(invalid_hit.location(), Eq(0));
}

TEST(EmbeddingHitTest, Comparison) {
  // Create basic hits with basic_hit1 < basic_hit2 < basic_hit3.
  BasicHit basic_hit1(/*section_id=*/1, /*document_id=*/2409);
  BasicHit basic_hit2(/*section_id=*/1, /*document_id=*/243);
  BasicHit basic_hit3(/*section_id=*/15, /*document_id=*/243);

  // Embedding hits are sorted by BasicHit first, and then by location.
  // So embedding_hit3 < embedding_hit4 < embedding_hit2 < embedding_hit1.
  EmbeddingHit embedding_hit1(basic_hit3, /*location=*/10);
  EmbeddingHit embedding_hit2(basic_hit3, /*location=*/0);
  EmbeddingHit embedding_hit3(basic_hit1, /*location=*/100);
  EmbeddingHit embedding_hit4(basic_hit2, /*location=*/0);

  std::vector<EmbeddingHit> embedding_hits{embedding_hit1, embedding_hit2,
                                           embedding_hit3, embedding_hit4};
  std::sort(embedding_hits.begin(), embedding_hits.end());
  EXPECT_THAT(embedding_hits, ElementsAre(embedding_hit3, embedding_hit4,
                                          embedding_hit2, embedding_hit1));
}

}  // namespace

}  // namespace lib
}  // namespace icing
