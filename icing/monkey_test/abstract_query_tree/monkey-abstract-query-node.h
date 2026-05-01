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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_QUERY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_QUERY_NODE_H_

#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Forward declaration to avoid circular dependency.
class InMemoryIcingSearchEngine;

// Base class for abstract query tree nodes used in Icing monkey tests.
//
// In monkey tests, Node subclasses are used to represent a specific query
// structure. The two main methods facilitate testing by providing different
// ways to interact with this structure:
// 1.  EvaluateQuery: This method is used with an InMemoryIcingSearchEngine
//     instance to "brute force" find the set of DocumentProtos that should
//     match the query represented by this node. This serves as the ground
//     truth.
// 2.  GenerateQueryString: This method generates a query string representation
//     of the node, which is then passed to the actual Icing search engine
//     implementation under test. The results from the Icing search engine
//     are compared against the DocumentProtos returned by EvaluateQuery.
class MonkeyAbstractQueryNode {
 public:
  // Virtual destructor to allow proper cleanup of derived classes.
  virtual ~MonkeyAbstractQueryNode() = default;

  // Evaluates the query represented by this node against the provided
  // InMemoryIcingSearchEngine and SearchSpecProto. Returns a vector of
  // DocumentProtos matching the query.
  virtual libtextclassifier3::StatusOr<std::vector<DocumentId>> EvaluateQuery(
      const InMemoryIcingSearchEngine* engine) const = 0;

  // Generates a query string representation of this node.
  virtual std::string GenerateQueryString() const = 0;
};

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
      std::vector<std::string> document_schema_types);

  libtextclassifier3::StatusOr<std::vector<DocumentId>> EvaluateQuery(
      const InMemoryIcingSearchEngine* engine) const override;

 private:
  // Returns true if the document matches the query represented by this node.
  virtual libtextclassifier3::StatusOr<bool> DoesDocumentMatchQuery(
      const InMemoryIcingSearchEngine* engine,
      const MonkeyTokenizedDocument& document) const = 0;

  // The document namespaces to filter out documents to search over.
  std::vector<std::string> document_namespaces_;
  // The document schema types to filter out documents to search over.
  std::vector<std::string> document_schema_types_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_QUERY_NODE_H_
