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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_HAS_PROPERTY_QUERY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_HAS_PROPERTY_QUERY_NODE_H_

#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"

namespace icing {
namespace lib {

// MonkeyNode that checks if a document has values in a given property path.
// This node is equivalent to the `hasProperty(property_path)` function in
// Icing's query language.
class MonkeyHasPropertyQueryNode : public MonkeyAbstractLeafQueryNode {
 public:
  explicit MonkeyHasPropertyQueryNode(
      std::string property_path,
      std::vector<std::string> document_namespaces = {},
      std::vector<std::string> document_schema_types = {});

  std::string GenerateQueryString() const override;

 private:
  libtextclassifier3::StatusOr<bool> DoesDocumentMatchQuery(
      const InMemoryIcingSearchEngine* engine,
      const MonkeyTokenizedDocument& document) const override;

  std::string property_path_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_HAS_PROPERTY_QUERY_NODE_H_
