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


#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "testing/fuzzing/fuzztest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/icu-data-file-helper.h"

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

SearchSpecProto SetSearchSpec(const std::string& data) {
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query(data);
  return search_spec;
}

void StringFuzzTest(const std::string& data) {
  // Initialize
  IcingSearchEngineOptions icing_options = Setup();
  std::string icu_data_file_path = GetTestFilePath("icing/icu.dat");
  if (!icu_data_file_helper::SetUpIcuDataFile(icu_data_file_path).ok()) {
    return;
  }
  IcingSearchEngine icing(icing_options);
  const Filesystem filesystem_;
  // TODO (b/145758378): Deleting directory should not be required.
  filesystem_.DeleteDirectoryRecursively(icing_options.base_dir().c_str());
  icing.Initialize();

  SchemaProto schema_proto =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  icing.SetSchema(schema_proto);

  // Index
  DocumentProto document = MakeDocument(data);
  icing.Put(document);

  // Query
  SearchSpecProto search_spec = SetSearchSpec(data);
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  SearchResultProto result = icing.Search(search_spec, scoring_spec,
                                          ResultSpecProto::default_instance());
  EXPECT_THAT(result.results(0).document().uri(), document.uri());
}
// TODO(b/416553583): Add more advanced fuzz tests including emojis and
// decomposed characters.
FUZZ_TEST(TestSuite, StringFuzzTest)
    .WithDomains(/* data= */ fuzztest::InRegexp("[a-z]+"));

}  // namespace
}  // namespace lib
}  // namespace icing
