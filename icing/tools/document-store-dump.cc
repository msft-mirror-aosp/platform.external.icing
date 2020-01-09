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

#include "icing/tools/document-store-dump.h"

#include <cinttypes>

#include "icing/absl_ports/str_cat.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {
namespace {

void AppendDocumentProto(DocId document_id, const Document& doc,
                         std::string* output) {
  absl_ports::StrAppend(
      output, IcingStringUtil::StringPrintf(
                  "Document {\n   document_id: %d\n  corpus_id: %d\n  uri: "
                  "'%s'\n  score: %d\n  created_timestamp_ms: %" PRIu64 "\n",
                  static_cast<int>(document_id), doc.corpus_id(),
                  doc.uri().c_str(), static_cast<int>(doc.score()),
                  static_cast<int64_t>(doc.created_timestamp_ms())));
  for (const auto& section : doc.sections()) {
    absl_ports::StrAppend(
        output, IcingStringUtil::StringPrintf(
                    "  section {\n    id: %d\n    indexed_length: "
                    "%d\n    content: '%s'\n    snippet: '%s'\n",
                    static_cast<int>(section.id()),
                    static_cast<int>(section.indexed_length()),
                    section.content().c_str(), section.snippet().c_str()));
    for (int64_t extracted_number : section.extracted_numbers()) {
      absl_ports::StrAppend(output, IcingStringUtil::StringPrintf(
                                        "    extracted_numbers: %" PRId64 "\n",
                                        extracted_number));
    }
    for (const std::string& annotation_token : section.annotation_tokens()) {
      absl_ports::StrAppend(
          output, IcingStringUtil::StringPrintf("    annotation_tokens: '%s'\n",
                                                annotation_token.c_str()));
    }
    std::string indexed = (section.config().indexed()) ? "true" : "false";
    std::string index_prefixes =
        (section.config().index_prefixes()) ? "true" : "false";
    absl_ports::StrAppend(
        output,
        IcingStringUtil::StringPrintf(
            "    config {\n      name: '%s'\n      indexed: %s\n      "
            "tokenizer: %d\n      weight: %d\n      index_prefixes: %s\n      "
            "subsection_separator: '%s'\n",
            section.config().name().c_str(), indexed.c_str(),
            section.config().tokenizer(),
            static_cast<int>(section.config().weight()), index_prefixes.c_str(),
            section.config().subsection_separator().c_str()));
    for (const auto& variant_generator :
         section.config().variant_generators()) {
      absl_ports::StrAppend(
          output, IcingStringUtil::StringPrintf(
                      "      variant_generators: %d\n", variant_generator));
    }
    absl_ports::StrAppend(
        output,
        IcingStringUtil::StringPrintf(
            "      common_term_legacy_hit_score: %d\n      "
            "rfc822_host_name_term_legacy_hit_score: %d\n      "
            "semantic_property: '%s'\n      universal_section_id: %d\n      "
            "omnibox_section_type: %d\n      st_section_type: %d\n    }\n  }\n",
            section.config().common_term_legacy_hit_score(),
            section.config().rfc822_host_name_term_legacy_hit_score(),
            section.config().semantic_property().c_str(),
            section.config().universal_section_id(),
            section.config().omnibox_section_type(),
            section.config().st_section_type()));
  }
  for (const auto& language : doc.languages()) {
    std::string used_classifier =
        (language.used_classifier()) ? "true" : "false";
    absl_ports::StrAppend(
        output, IcingStringUtil::StringPrintf(
                    "  languages {\n    language: %d\n    score: %d\n    "
                    "used_classifier: %s\n  }\n",
                    language.language(), static_cast<int>(language.score()),
                    used_classifier.c_str()));
  }
  absl_ports::StrAppend(
      output, IcingStringUtil::StringPrintf(
                  " ANNOTATIONS PRINTING NOT IMPLEMENTED YET IN ICING-TOOL\n"));
}

}  // namespace

std::string GetDocumentStoreDump(const DocumentStore& document_store) {
  std::string output;
  for (DocId document_id = 0; document_id < document_store.num_documents();
       document_id++) {
    Document doc;
    if (!document_store.ReadDocument(document_id, &doc)) {
      ICING_LOG(FATAL) << "Failed to read document";
    }

    AppendDocumentProto(document_id, doc, &output);
  }
  return output;
}

}  // namespace lib
}  // namespace icing
