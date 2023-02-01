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

#ifndef ICING_TESTING_SNIPPET_HELPERS_H_
#define ICING_TESTING_SNIPPET_HELPERS_H_

#include <string>

#include "icing/proto/document.pb.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

// Retrieve pointer to the snippet_index'th SnippetMatchProto within the
// EntryProto identified by property_name within snippet_proto.
// Returns nullptr
//   - if there is no EntryProto within snippet_proto corresponding to
//     property_name.
//   - if there is no SnippetMatchProto at snippet_index within the EntryProto
const SnippetMatchProto* GetSnippetMatch(const SnippetProto& snippet_proto,
                                         const std::string& property_name,
                                         int snippet_index);

// Retrieve pointer to the PropertyProto identified by property_name.
// Returns nullptr if no such property exists.
const PropertyProto* GetProperty(const DocumentProto& document,
                                 const std::string& property_name);

// Retrieves the window defined by the SnippetMatchProto returned by
// GetSnippetMatch(snippet_proto, property_name, snippet_index) for the property
// returned by GetProperty(document, property_name).
// Returns "" if no such property, snippet or window exists.
std::string GetWindow(const DocumentProto& document,
                      const SnippetProto& snippet_proto,
                      const std::string& property_name, int snippet_index);

// Retrieves the match defined by the SnippetMatchProto returned by
// GetSnippetMatch(snippet_proto, property_name, snippet_index) for the property
// returned by GetProperty(document, property_name).
// Returns "" if no such property or snippet exists.
std::string GetMatch(const DocumentProto& document,
                     const SnippetProto& snippet_proto,
                     const std::string& property_name, int snippet_index);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_SNIPPET_HELPERS_H_
