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

#include "icing/testing/snippet-helpers.h"

#include <algorithm>
#include <string_view>

#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

const SnippetMatchProto* GetSnippetMatch(const SnippetProto& snippet_proto,
                                         const std::string& property_name,
                                         int snippet_index) {
  auto iterator = std::find_if(
      snippet_proto.entries().begin(), snippet_proto.entries().end(),
      [&property_name](const SnippetProto::EntryProto& entry) {
        return entry.property_name() == property_name;
      });
  if (iterator == snippet_proto.entries().end() ||
      iterator->snippet_matches_size() <= snippet_index) {
    return nullptr;
  }
  return &iterator->snippet_matches(snippet_index);
}

const PropertyProto* GetProperty(const DocumentProto& document,
                                 const std::string& property_name) {
  const PropertyProto* property = nullptr;
  for (const PropertyProto& prop : document.properties()) {
    if (prop.name() == property_name) {
      property = &prop;
    }
  }
  return property;
}

std::string GetWindow(const DocumentProto& document,
                      const SnippetProto& snippet_proto,
                      const std::string& property_name, int snippet_index) {
  const SnippetMatchProto* match =
      GetSnippetMatch(snippet_proto, property_name, snippet_index);
  const PropertyProto* property = GetProperty(document, property_name);
  if (match == nullptr || property == nullptr) {
    return "";
  }
  std::string_view value = property->string_values(match->values_index());
  return std::string(
      value.substr(match->window_position(), match->window_bytes()));
}

std::string GetMatch(const DocumentProto& document,
                     const SnippetProto& snippet_proto,
                     const std::string& property_name, int snippet_index) {
  const SnippetMatchProto* match =
      GetSnippetMatch(snippet_proto, property_name, snippet_index);
  const PropertyProto* property = GetProperty(document, property_name);
  if (match == nullptr || property == nullptr) {
    return "";
  }
  std::string_view value = property->string_values(match->values_index());
  return std::string(
      value.substr(match->exact_match_position(), match->exact_match_bytes()));
}

}  // namespace lib
}  // namespace icing
