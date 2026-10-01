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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_LEAF_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_LEAF_NODE_H_

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Representation of leaf nodes in the abstract query tree.
//
// Leaf nodes do not have child nodes and is where the actual matching against
// documents occurs based on the derived class. Leaf nodes also contain the
// document filters, i.e. namespaces and schema types used to filter out
// documents to search over.
class MonkeyAbstractLeafQueryNode : public MonkeyAbstractQueryNode {
 public:
  explicit MonkeyAbstractLeafQueryNode(
      std::vector<std::string> document_namespaces,
      std::vector<std::string> document_schema_types)
      : document_namespaces_(std::move(document_namespaces)),
        document_schema_types_(std::move(document_schema_types)) {}

  libtextclassifier3::StatusOr<std::vector<DocumentId>> EvaluateQuery(
      const InMemoryIcingSearchEngine* engine) const override;

 protected:
  // Returns true if the document matches the query represented by this node.
  virtual libtextclassifier3::StatusOr<bool> DoesDocumentMatchQuery(
      const InMemoryIcingSearchEngine* engine,
      const MonkeyTokenizedDocument& document) const = 0;

  // The document namespaces to filter out documents to search over.
  std::vector<std::string> document_namespaces_;
  // The document schema types to filter out documents to search over.
  std::vector<std::string> document_schema_types_;
};

// Representation of leaf nodes in the abstract query tree that support property
// restricts.
class MonkeyAbstractRestrictableLeafQueryNode
    : public MonkeyAbstractLeafQueryNode {
 public:
  explicit MonkeyAbstractRestrictableLeafQueryNode(
      std::unordered_set<std::string> property_restricts,
      std::vector<std::string> document_namespaces,
      std::vector<std::string> document_schema_types)
      : MonkeyAbstractLeafQueryNode(std::move(document_namespaces),
                                    std::move(document_schema_types)),
        property_restricts_(std::move(property_restricts)) {}

  explicit MonkeyAbstractRestrictableLeafQueryNode(
      std::unordered_set<std::string> property_restricts)
      : MonkeyAbstractRestrictableLeafQueryNode(std::move(property_restricts),
                                                /*document_namespaces=*/{},
                                                /*document_schema_types=*/{}) {}

  explicit MonkeyAbstractRestrictableLeafQueryNode(
      std::vector<std::string> document_namespaces,
      std::vector<std::string> document_schema_types)
      : MonkeyAbstractRestrictableLeafQueryNode(
            /*property_restricts=*/std::unordered_set<std::string>(),
            /*document_namespaces=*/std::move(document_namespaces),
            /*document_schema_types=*/std::move(document_schema_types)) {}

  void AddPropertyRestricts(const std::vector<std::string>& property_paths) {
    property_restricts_.insert(property_paths.begin(), property_paths.end());
  }

  bool IsRestrictedSection(const MonkeySection& section) const {
    return property_restricts_.empty() ||
           property_restricts_.contains(section.path);
  }

 protected:
  std::unordered_set<std::string> property_restricts_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_TREE_NODE_H_
