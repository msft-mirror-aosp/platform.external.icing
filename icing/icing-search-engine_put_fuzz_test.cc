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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "testing/fuzzing/fuzztest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/icu-data-file-helper.h"
#include "unicode/umachine.h"

namespace icing {
namespace lib {
namespace {

IcingSearchEngineOptions Setup() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestTempDir() + "/icing");
  return icing_options;
}

DocumentProto MakeDocument(const std::string& data) {
  return DocumentBuilder()
      .SetKey("namespace", "uri1")
      .SetSchema("Message")
      .AddStringProperty("body", data)
      .SetCreationTimestampMs(0L)
      .Build();
}

void PutUtf8DocumentFuzzTest(const std::string& utf8_string) {
  // Initialize
  IcingSearchEngineOptions icing_options = Setup();
  std::string icu_data_file_path = GetTestFilePath("icing/icu.dat");
  if (!icu_data_file_helper::SetUpIcuDataFile(icu_data_file_path).ok()) {
    return;
  }
  IcingSearchEngine icing(icing_options);
  const Filesystem filesystem_;
  filesystem_.DeleteDirectoryRecursively(icing_options.base_dir().c_str());
  ASSERT_THAT(icing.Initialize().status(), ProtoStatusIs(StatusProto::OK));

  SchemaProto schema_proto =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema_proto).status(),
              ProtoStatusIs(StatusProto::OK));

  // Index
  DocumentProto document = MakeDocument(utf8_string);
  EXPECT_THAT(icing.Put(document).status(), ProtoStatusIs(StatusProto::OK));
}

auto ValidUChar32() {
  return fuzztest::OneOf(fuzztest::InRange<UChar32>(0, 0xD7FF),
                         fuzztest::InRange<UChar32>(0xE000, 0x10FFFF));
}

std::string ToUtf8String(const std::vector<UChar32>& code_points) {
  std::string result;
  for (UChar32 cp : code_points) {
    i18n_utils::AppendUchar32ToUtf8(&result, cp);
  }
  return result;
}

auto Utf8StringOfLength10() {
  return fuzztest::Map(ToUtf8String,
                       fuzztest::VectorOf(ValidUChar32()).WithSize(10));
}

FUZZ_TEST(TestSuite, PutUtf8DocumentFuzzTest)
    .WithDomains(Utf8StringOfLength10());

}  // namespace
}  // namespace lib
}  // namespace icing
