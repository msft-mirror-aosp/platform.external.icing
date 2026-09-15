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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_TERM_QUERY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_TERM_QUERY_NODE_H_

#include <string>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"

namespace icing {
namespace lib {

class InMemoryIcingSearchEngine;

// MonkeyNode that stores text. This node is equivalent to
// androidx.appsearch.ast.TextNode in the AppSearch AST library.
//
// Text may represent a string or number. For example in the query
// `hello AND "world peace" -cat price:49.99`
//   - hello and cat are strings.
//   - "world peace" is a verbatim string, i.e. a quoted string that can be
//     represented by setting is_verbatim_ to true. Because it is a verbatim
//     string, it will be treated as a single term "world peace" instead of
//     terms "world" and "peace".
//   - 49.99 is a number. MonkeyTermQueryNode may represent numbers that are
//     integers or doubles as terms.
//   - price is NOT a string but a property path as part of a
//     PropertyRestrictNode.
//
// The node will be segmented and normalized based on the flags set in the
// Node. For example, if the node containing the string "foo" has both
// is_prefix_ and is_verbatim_ set to true, then the resulting tree will be
// treated as the query `"foo"*` i.e. the prefix of the quoted string "foo".
//
// MonkeyTermQueryNode is guaranteed to not have child nodes.
class MonkeyTermQueryNode : public MonkeyAbstractRestrictableLeafQueryNode {
 public:
  explicit MonkeyTermQueryNode(std::string term, bool is_prefix,
                               bool is_verbatim,
                               TermMatchType::Code term_match_type);

  explicit MonkeyTermQueryNode(
      std::string term, bool is_prefix, bool is_verbatim,
      TermMatchType::Code term_match_type,
      std::unordered_set<std::string> property_restricts);

  explicit MonkeyTermQueryNode(
      std::string term, bool is_prefix, bool is_verbatim,
      TermMatchType::Code term_match_type,
      std::vector<std::string> document_namespaces,
      std::vector<std::string> document_schema_types,
      std::unordered_set<std::string> property_restricts);

  std::string GenerateQueryString() const override;

 private:
  // Returns true if the document matches the query represented by this node.
  libtextclassifier3::StatusOr<bool> DoesDocumentMatchQuery(
      const InMemoryIcingSearchEngine* engine,
      const MonkeyTokenizedDocument& document) const;

  // The text that we are searching for.
  std::string term_;
  // Whether the text is a prefix.
  bool is_prefix_;
  // Whether the text is a verbatim string.
  bool is_verbatim_;

  // Search spec fields that don't need to be represented in the query string.
  // The term match type.
  TermMatchType::Code term_match_type_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_TERM_QUERY_NODE_H_
