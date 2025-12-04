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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document_wrapper.pb.h"

namespace icing {
namespace lib {
namespace document_util {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;

TEST(DocumentUtilTest, CreateDocumentWrapper) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/1")
                               .SetSchema("FakeType")
                               .AddStringProperty("prop1", "foo", "bar", "baz")
                               .Build();

  DocumentWrapper document_wrapper = CreateDocumentWrapper(document);
  EXPECT_THAT(document_wrapper.document(), EqualsProto(document));
}

}  // namespace

}  // namespace document_util

}  // namespace lib
}  // namespace icing
