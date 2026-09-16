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

#include "icing/util/document-util.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {
namespace document_util {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;

TEST(DocumentUtilTest, CreateDocumentWrapper) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/1")
                               .SetSchema("FakeType")
                               .AddStringProperty("prop1", "foo", "bar", "baz")
                               .Build();

  DocumentWrapper document_wrapper = CreateDocumentWrapper(document);
  EXPECT_THAT(document_wrapper.document(), EqualsProto(document));
}

TEST(DocumentUtilTest, GetOptimizedDocumentId) {
  std::vector<DocumentId> document_id_old_to_new = {
      kInvalidDocumentId, 5, 4, 2, 1, kInvalidDocumentId, 3, 6};

  EXPECT_THAT(GetOptimizedDocumentId(0, document_id_old_to_new),
              Eq(kInvalidDocumentId));
  EXPECT_THAT(GetOptimizedDocumentId(1, document_id_old_to_new), Eq(5));
  EXPECT_THAT(GetOptimizedDocumentId(2, document_id_old_to_new), Eq(4));
  EXPECT_THAT(GetOptimizedDocumentId(3, document_id_old_to_new), Eq(2));
  EXPECT_THAT(GetOptimizedDocumentId(4, document_id_old_to_new), Eq(1));
  EXPECT_THAT(GetOptimizedDocumentId(5, document_id_old_to_new),
              Eq(kInvalidDocumentId));
  EXPECT_THAT(GetOptimizedDocumentId(6, document_id_old_to_new), Eq(3));
  EXPECT_THAT(GetOptimizedDocumentId(7, document_id_old_to_new), Eq(6));
}

TEST(DocumentUtilTest, GetOptimizedDocumentId_OutOfRange) {
  std::vector<DocumentId> document_id_old_to_new = {
      kInvalidDocumentId, 5, 4, 2, 1, kInvalidDocumentId, 3, 6};

  EXPECT_THAT(GetOptimizedDocumentId(-2, document_id_old_to_new),
              Eq(kInvalidDocumentId));
  EXPECT_THAT(GetOptimizedDocumentId(-1, document_id_old_to_new),
              Eq(kInvalidDocumentId));
  EXPECT_THAT(GetOptimizedDocumentId(8, document_id_old_to_new),
              Eq(kInvalidDocumentId));
  EXPECT_THAT(GetOptimizedDocumentId(9, document_id_old_to_new),
              Eq(kInvalidDocumentId));
  EXPECT_THAT(
      GetOptimizedDocumentId(kInvalidDocumentId, document_id_old_to_new),
      Eq(kInvalidDocumentId));
}

}  // namespace

}  // namespace document_util
}  // namespace lib
}  // namespace icing
