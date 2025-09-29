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

#include <cstdint>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/proto/document.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::HasSubstr;
using ::testing::SizeIs;

std::string GetTestBaseDir() { return GetTestTempDir() + "/icing"; }

// ICU initialization is effectively global and cannot be reset between tests.
// In order to properly test ICU initialization failure cases, we need an
// independent test class.
// If ICU initialization is unsuccessful, Reverse JNI will be used to
// support segmentation.
class IcingSearchEngineInitializeIcuFailureTest : public testing::Test {
 protected:
  void SetUp() override {
    filesystem_.CreateDirectoryRecursively(GetTestBaseDir().c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  const Filesystem* filesystem() const { return &filesystem_; }

 private:
  Filesystem filesystem_;
};

// Non-zero value so we don't override it to be the current time
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

SchemaProto CreateMessageSchema() {
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
          PropertyConfigBuilder()
              .SetName("body")
              .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
              .SetCardinality(CARDINALITY_REQUIRED)))
      .Build();
}

DocumentProto CreateMessageDocument(std::string name_space, std::string uri,
                                    std::string string_value) {
  return DocumentBuilder()
      .SetKey(std::move(name_space), std::move(uri))
      .SetSchema("Message")
      .AddStringProperty("body", std::move(string_value))
      .SetCreationTimestampMs(kDefaultCreationTimestampMs)
      .Build();
}

TEST_F(IcingSearchEngineInitializeIcuFailureTest,
       InitializeIcuDataNotSpecifiedFails) {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  // ICU data file path is not specified.
  IcingSearchEngine icing(icing_options, GetTestJniCache());

  InitializeResultProto initialize_result = icing.Initialize();
  ASSERT_THAT(initialize_result.status(), ProtoIsOk());
  ASSERT_THAT(initialize_result.initialize_stats().initialize_icu_data_status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT,
                            HasSubstr("ICU data file path is empty.")));

  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document =
      CreateMessageDocument("namespace1", "uri1", "foo bar baz qux");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("qux");

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::NONE);

  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document));
}

TEST_F(IcingSearchEngineInitializeIcuFailureTest,
       InitializeIcuDataInvalidPathFails) {
  const std::string icu_data_file_absolute_path = "invalid path";
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_icu_data_file_absolute_path(icu_data_file_absolute_path);
  IcingSearchEngine icing(icing_options, GetTestJniCache());

  InitializeResultProto initialize_result = icing.Initialize();
  ASSERT_THAT(initialize_result.status(), ProtoIsOk());
  ASSERT_THAT(initialize_result.initialize_stats().initialize_icu_data_status(),
              ProtoStatusIs(StatusProto::INTERNAL));

  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document =
      CreateMessageDocument("namespace1", "uri1", "foo bar");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("bar");

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::NONE);

  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document));
}

}  // namespace

}  // namespace lib
}  // namespace icing
