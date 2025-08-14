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


#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "testing/base/public/gmock.h"
#include "testing/fuzzing/fuzztest.h"
#include "third_party/icing/document-builder.h"
#include "third_party/icing/file/filesystem.h"
#include "third_party/icing/icing-search-engine.h"
#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/proto/initialize.proto.h"
#include "third_party/icing/proto/schema.proto.h"
#include "third_party/icing/proto/scoring.proto.h"
#include "third_party/icing/proto/search.proto.h"
#include "third_party/icing/proto/status.proto.h"
#include "third_party/icing/proto/term.proto.h"
#include "third_party/icing/schema-builder.h"
#include "third_party/icing/testing/common-matchers.h"
#include "third_party/icing/testing/test-data.h"
#include "third_party/icing/testing/tmp-directory.h"
#include "third_party/icing/util/icu-data-file-helper.h"

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
  std::string icu_data_file_path = GetTestFilePath("third_party/icing/icu.dat");
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

// vector of emojis to be used in the fuzz test
const std::vector<std::string> emojis = {"😀", "😂", "😊", "😘",
  "😠", "😢", "😱", "🍻"};

// add a random emoji to a string containing a decomposed character
std::string addEmoji(const std::string& str, const std::string& emoji) {
  return str + emoji;
}

// dictionary including Spanish words with accents (lowercase and uppercase)
std::vector<std::string> data = {"dìas", "Sí", "Más", "Tú", "Él",
  "Dónde", "Qué", "miércoles", "café",
  "sábado"};

FUZZ_TEST(TestSuite, StringFuzzTest)
    .WithDomains(fuzztest::Map(addEmoji, fuzztest::ElementOf(data),
                               fuzztest::ElementOf(emojis)));

}  // namespace
}  // namespace lib
}  // namespace icing
