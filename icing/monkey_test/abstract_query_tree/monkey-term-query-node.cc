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

#include "icing/monkey_test/abstract_query_tree/monkey-term-query-node.h"

#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/term.pb.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Check if s1 is a prefix of s2.
bool IsPrefix(std::string_view s1, std::string_view s2) {
  if (s1.length() > s2.length()) {
    return false;
  }
  return s1 == s2.substr(0, s1.length());
}

}  // namespace

MonkeyTermQueryNode::MonkeyTermQueryNode(
    std::string term, bool is_prefix, bool is_verbatim,
    TermMatchType::Code term_match_type,
    std::vector<std::string> document_namespaces,
    std::vector<std::string> document_schema_types,
    std::unordered_set<std::string> property_restricts)
    : MonkeyAbstractRestrictableLeafQueryNode(std::move(property_restricts),
                                              std::move(document_namespaces),
                                              std::move(document_schema_types)),
      term_(std::move(term)),
      is_prefix_(is_prefix),
      is_verbatim_(is_verbatim),
      term_match_type_(term_match_type) {}

MonkeyTermQueryNode::MonkeyTermQueryNode(
    std::string term, bool is_prefix, bool is_verbatim,
    TermMatchType::Code term_match_type,
    std::unordered_set<std::string> property_restricts)
    : MonkeyTermQueryNode(std::move(term), is_prefix, is_verbatim,
                          term_match_type, /*document_namespaces=*/{},
                          /*document_schema_types=*/{},
                          std::move(property_restricts)) {}

MonkeyTermQueryNode::MonkeyTermQueryNode(std::string term, bool is_prefix,
                                         bool is_verbatim,
                                         TermMatchType::Code term_match_type)
    : MonkeyTermQueryNode(
          std::move(term), is_prefix, is_verbatim, term_match_type,
          /*document_namespaces=*/{},
          /*document_schema_types=*/{},
          /*property_restricts=*/std::unordered_set<std::string>()) {}

libtextclassifier3::StatusOr<bool> MonkeyTermQueryNode::DoesDocumentMatchQuery(
    const InMemoryIcingSearchEngine* search_engine,
    const MonkeyTokenizedDocument& document) const {
  for (const MonkeySection& section : document.sections) {
    if (!MonkeyAbstractRestrictableLeafQueryNode::IsRestrictedSection(
            section)) {
      continue;
    }
    ICING_ASSIGN_OR_RETURN(
        InMemoryIcingSearchEngine::PropertyIndexInfo property_index_info,
        search_engine->GetPropertyIndexInfo(document.document.schema(),
                                            section.path));
    if (!property_index_info.indexable ||
        property_index_info.data_type !=
            PropertyConfigProto::DataType::STRING) {
      continue;
    }
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer> tokenizer,
                           tokenizer_factory::CreateIndexingTokenizer(
                               property_index_info.tokenizer_type,
                               search_engine->GetLanguageSegmenter()));
    for (const std::string& string_value : section.string_values) {
      ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer->Tokenize(string_value));
      while (itr->Advance()) {
        for (const Token& token : itr->GetTokensForTest()) {
          // Exact match if either the property index info or the search spec is
          // EXACT_ONLY.
          if (property_index_info.term_match_type ==
                  TermMatchType::EXACT_ONLY ||
              term_match_type_ == TermMatchType::EXACT_ONLY) {
            if (token.text == term_) {
              return true;
            }
          } else if (IsPrefix(term_, token.text)) {
            return true;
          }
        }
      }
    }
  }
  return false;
}

std::string MonkeyTermQueryNode::GenerateQueryString() const {
  std::string query_string = term_;
  if (is_verbatim_) {
    query_string = absl_ports::StrCat("\"", query_string, "\"");
  }
  if (is_prefix_) {
    query_string += '*';
  }
  // TermMatchType is a SearchSpecProto field, so it is not included in the
  // query string.

  // TODO(b/491571627) - Handle multiple property restricts.
  if (!property_restricts_.empty()) {
    query_string =
        absl_ports::StrCat(*property_restricts_.begin(), ":", query_string);
  }
  return query_string;
}

}  // namespace lib
}  // namespace icing
