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

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/absl_ports/str_cat.h"
#include "icing/proto/document.pb.h"

namespace icing {
namespace lib {

// No matter whether the property is indexable, we have to create a section for
// it since a non-indexable property can become indexable after a schema type
// change. The in-memory icing will automatically skip sections that are
// non-indexable at the time of search requests.
struct MonkeySection {
  std::string path;
  std::vector<std::string> string_values;
  std::vector<PropertyProto::VectorProto> vector_values;
  std::vector<int64_t> integer_values;
};

// Note:
// - Unlike TokenizedDocument in prod Icing, data held by MonkeySection may not
//   be indexable or joinable.
// - In fact, they are only "potentially" indexable or joinable. It is more
//   convenient to extract and store them separately, so
//   InMemoryIcingSearchEngine can easily use sections/joinable properties if
//   they're indexable/joinable, OR ignore them if non-indexable/non-joinable.

struct MonkeyTokenizedDocument {
  DocumentProto document;
  std::vector<MonkeySection> sections;

  static MonkeyTokenizedDocument Reload(DocumentProto document) {
    MonkeyTokenizedDocument tokenized_document;
    tokenized_document.document = std::move(document);

    ExtractSections(tokenized_document.document, /*curr_path=*/"",
                    tokenized_document.sections);

    return tokenized_document;
  }

  void Clear() {
    document.Clear();
    sections.clear();
  }

  const MonkeySection* GetSectionByPath(std::string_view path) const {
    for (const MonkeySection& section : sections) {
      if (section.path == path) {
        return &section;
      }
    }
    return nullptr;
  }

 private:
  static void ExtractSections(const DocumentProto& document,
                              std::string curr_path,
                              std::vector<MonkeySection>& sections) {
    for (const PropertyProto& property : document.properties()) {
      std::string new_path =
          curr_path.empty()
              ? property.name()
              : absl_ports::StrCat(curr_path, ".", property.name());
      if (!property.string_values().empty()) {
        sections.push_back(MonkeySection{
            .path = new_path,
            .string_values =
                std::vector<std::string>(property.string_values().cbegin(),
                                         property.string_values().cend())});
      } else if (!property.vector_values().empty()) {
        sections.push_back(MonkeySection{
            .path = new_path,
            .vector_values = std::vector<PropertyProto::VectorProto>(
                property.vector_values().cbegin(),
                property.vector_values().cend())});
      } else if (!property.int64_values().empty()) {
        sections.push_back(MonkeySection{
            .path = new_path,
            .integer_values = std::vector<int64_t>(
                property.int64_values().cbegin(),
                property.int64_values().cend())});
      } else if (property.document_values_size() > 0) {
        for (const DocumentProto& document_value : property.document_values()) {
          ExtractSections(document_value, new_path, sections);
        }
      }
    }
  }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_MONKEY_TOKENIZED_DOCUMENT_H_
