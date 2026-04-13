// Copyright (C) 2022 Google LLC
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

#ifndef ICING_MONKEY_TEST_MONKEY_TOKENIZED_DOCUMENT_H_
#define ICING_MONKEY_TEST_MONKEY_TOKENIZED_DOCUMENT_H_

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/absl_ports/str_cat.h"
#include "icing/absl_ports/str_join.h"
#include "icing/proto/document.pb.h"

namespace icing {
namespace lib {

// No matter whether the property is indexable, we have to create a section for
// it since a non-indexable property can become indexable after a schema type
// change. The in-memory icing will automatically skip sections that are
// non-indexable at the time of search requests.
struct MonkeyTokenizedSection {
  std::string path;
  std::vector<std::string> token_sequence;
  std::vector<PropertyProto::VectorProto> embedding_vectors;
};

struct MonkeyTokenizedDocument {
  DocumentProto document;
  std::vector<MonkeyTokenizedSection> tokenized_sections;

  static MonkeyTokenizedDocument Reload(DocumentProto document) {
    MonkeyTokenizedDocument tokenized_document;
    tokenized_document.document = std::move(document);
    ExtractTokenizedSections(tokenized_document.document, /*curr_path=*/"",
                             tokenized_document.tokenized_sections);
    return tokenized_document;
  }

 private:
  static void ExtractTokenizedSections(
      const DocumentProto& document, std::string curr_path,
      std::vector<MonkeyTokenizedSection>& tokenized_sections) {
    for (const PropertyProto& property : document.properties()) {
      std::string new_path =
          curr_path.empty()
              ? property.name()
              : absl_ports::StrCat(curr_path, ".", property.name());
      if (property.string_values_size() > 0) {
        std::vector<std::string> token_sequence;
        for (const std::string& value : property.string_values()) {
          for (std::string_view token : absl_ports::StrSplit(value, " ")) {
            token_sequence.push_back(std::string(token));
          }
        }
        tokenized_sections.push_back(MonkeyTokenizedSection{
            .path = new_path, .token_sequence = std::move(token_sequence)});
      } else if (property.vector_values_size() > 0) {
        std::vector<PropertyProto::VectorProto> embedding_vectors;
        for (const PropertyProto::VectorProto& vector :
             property.vector_values()) {
          embedding_vectors.push_back(vector);
        }
        tokenized_sections.push_back(MonkeyTokenizedSection{
            .path = new_path,
            .embedding_vectors = std::move(embedding_vectors)});
      } else if (property.document_values_size() > 0) {
        for (const DocumentProto& document_value : property.document_values()) {
          ExtractTokenizedSections(document_value, new_path,
                                   tokenized_sections);
        }
      }
    }
  }
};

}  // namespace lib
}  // namespace icing
#endif  // ICING_MONKEY_TEST_MONKEY_TOKENIZED_DOCUMENT_H_
