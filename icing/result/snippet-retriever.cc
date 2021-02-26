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

#include "icing/result/snippet-retriever.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/absl_ports/str_join.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-terms.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section-manager.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/transform/normalizer.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

const PropertyProto* GetProperty(const DocumentProto& document,
                                 std::string_view property_name) {
  for (const PropertyProto& property : document.properties()) {
    if (property.name() == property_name) {
      return &property;
    }
  }
  return nullptr;
}

inline std::string AddPropertyToPath(const std::string& current_path,
                                     std::string_view property) {
  if (current_path.empty()) {
    return std::string(property);
  }
  return absl_ports::StrCat(current_path, kPropertySeparator, property);
}

inline std::string AddIndexToPath(int values_size, int index,
                                  const std::string& property_path) {
  if (values_size == 1) {
    return property_path;
  }
  return absl_ports::StrCat(property_path, kLBracket, std::to_string(index),
                            kRBracket);
}

class TokenMatcher {
 public:
  virtual ~TokenMatcher() = default;
  virtual bool Matches(Token token) const = 0;
};

class TokenMatcherExact : public TokenMatcher {
 public:
  explicit TokenMatcherExact(
      const std::unordered_set<std::string>& unrestricted_query_terms,
      const std::unordered_set<std::string>& restricted_query_terms,
      const Normalizer& normalizer)
      : unrestricted_query_terms_(unrestricted_query_terms),
        restricted_query_terms_(restricted_query_terms),
        normalizer_(normalizer) {}

  bool Matches(Token token) const override {
    std::string s = normalizer_.NormalizeTerm(token.text);
    return (unrestricted_query_terms_.count(s) > 0) ||
           (restricted_query_terms_.count(s) > 0);
  }

 private:
  const std::unordered_set<std::string>& unrestricted_query_terms_;
  const std::unordered_set<std::string>& restricted_query_terms_;
  const Normalizer& normalizer_;
};

class TokenMatcherPrefix : public TokenMatcher {
 public:
  explicit TokenMatcherPrefix(
      const std::unordered_set<std::string>& unrestricted_query_terms,
      const std::unordered_set<std::string>& restricted_query_terms,
      const Normalizer& normalizer)
      : unrestricted_query_terms_(unrestricted_query_terms),
        restricted_query_terms_(restricted_query_terms),
        normalizer_(normalizer) {}

  bool Matches(Token token) const override {
    std::string s = normalizer_.NormalizeTerm(token.text);
    if (std::any_of(unrestricted_query_terms_.begin(),
                    unrestricted_query_terms_.end(),
                    [&s](const std::string& term) {
                      return term.length() <= s.length() &&
                             s.compare(0, term.length(), term) == 0;
                    })) {
      return true;
    }
    return std::any_of(restricted_query_terms_.begin(),
                       restricted_query_terms_.end(),
                       [&s](const std::string& term) {
                         return term.length() <= s.length() &&
                                s.compare(0, term.length(), term) == 0;
                       });
  }

 private:
  const std::unordered_set<std::string>& unrestricted_query_terms_;
  const std::unordered_set<std::string>& restricted_query_terms_;
  const Normalizer& normalizer_;
};

libtextclassifier3::StatusOr<std::unique_ptr<TokenMatcher>> CreateTokenMatcher(
    TermMatchType::Code match_type,
    const std::unordered_set<std::string>& unrestricted_query_terms,
    const std::unordered_set<std::string>& restricted_query_terms,
    const Normalizer& normalizer) {
  switch (match_type) {
    case TermMatchType::EXACT_ONLY:
      return std::make_unique<TokenMatcherExact>(
          unrestricted_query_terms, restricted_query_terms, normalizer);
    case TermMatchType::PREFIX:
      return std::make_unique<TokenMatcherPrefix>(
          unrestricted_query_terms, restricted_query_terms, normalizer);
    case TermMatchType::UNKNOWN:
      [[fallthrough]];
    default:
      return absl_ports::InvalidArgumentError("Invalid match type provided.");
  }
}

// Returns true if token matches any of the terms in query terms according to
// the provided match type.
//
// Returns:
//   the position of the window start if successful
//   INTERNAL_ERROR - if a tokenizer error is encountered
libtextclassifier3::StatusOr<int> DetermineWindowStart(
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    std::string_view value, int match_mid, Tokenizer::Iterator* iterator) {
  int window_start_min = (match_mid - snippet_spec.max_window_bytes() / 2) - 1;
  if (window_start_min < 0) {
    return 0;
  }
  if (!iterator->ResetToTokenAfter(window_start_min)) {
    return absl_ports::InternalError(
        "Couldn't reset tokenizer to determine snippet window!");
  }
  return iterator->GetToken().text.data() - value.data();
}

// Increments window_end_exclusive so long as the character at the position
// of window_end_exclusive is punctuation and does not exceed
// window_end_max_exclusive.
int IncludeTrailingPunctuation(std::string_view value, int window_end_exclusive,
                               int window_end_max_exclusive) {
  while (window_end_exclusive < window_end_max_exclusive) {
    int char_len = 0;
    if (!i18n_utils::IsPunctuationAt(value, window_end_exclusive, &char_len)) {
      break;
    }
    if (window_end_exclusive + char_len > window_end_max_exclusive) {
      // This is punctuation, but it goes beyond the window end max. Don't
      // include it.
      break;
    }
    // Expand window by char_len and check the next character.
    window_end_exclusive += char_len;
  }
  return window_end_exclusive;
}

// Returns:
//   the position of the window end if successful
//   INTERNAL_ERROR - if a tokenizer error is encountered
libtextclassifier3::StatusOr<int> DetermineWindowEnd(
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    std::string_view value, int match_mid, Tokenizer::Iterator* iterator) {
  int window_end_max_exclusive =
      match_mid + snippet_spec.max_window_bytes() / 2;
  if (window_end_max_exclusive >= value.length()) {
    return value.length();
  }
  if (!iterator->ResetToTokenBefore(window_end_max_exclusive)) {
    return absl_ports::InternalError(
        "Couldn't reset tokenizer to determine snippet window!");
  }
  int window_end_exclusive = iterator->GetToken().text.data() - value.data() +
                             iterator->GetToken().text.length();
  return IncludeTrailingPunctuation(value, window_end_exclusive,
                                    window_end_max_exclusive);
}

struct SectionData {
  std::string_view section_name;
  std::string_view section_subcontent;
};

libtextclassifier3::StatusOr<SnippetMatchProto> RetrieveMatch(
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    const SectionData& value, Tokenizer::Iterator* iterator) {
  SnippetMatchProto snippet_match;
  Token match = iterator->GetToken();
  int match_pos = match.text.data() - value.section_subcontent.data();
  int match_mid = match_pos + match.text.length() / 2;

  snippet_match.set_exact_match_position(match_pos);
  snippet_match.set_exact_match_bytes(match.text.length());

  if (snippet_spec.max_window_bytes() > match.text.length()) {
    // Find the beginning of the window.
    ICING_ASSIGN_OR_RETURN(
        int window_start,
        DetermineWindowStart(snippet_spec, value.section_subcontent, match_mid,
                             iterator));
    snippet_match.set_window_position(window_start);

    // Find the end of the window.
    ICING_ASSIGN_OR_RETURN(
        int window_end_exclusive,
        DetermineWindowEnd(snippet_spec, value.section_subcontent, match_mid,
                           iterator));
    snippet_match.set_window_bytes(window_end_exclusive - window_start);

    // DetermineWindowStart/End may change the position of the iterator. So,
    // reset the iterator back to the original position.
    bool success = (match_pos > 0) ? iterator->ResetToTokenAfter(match_pos - 1)
                                   : iterator->ResetToStart();
    if (!success) {
      return absl_ports::InternalError(
          "Couldn't reset tokenizer to determine snippet window!");
    }
  }

  return snippet_match;
}

struct MatchOptions {
  const ResultSpecProto::SnippetSpecProto& snippet_spec;
  int max_matches_remaining;
};

// Retrieves snippets in the string values of current_property.
// Tokenizer is provided to tokenize string content and matcher is provided to
// indicate when a token matches content in the query.
//
// current_property is the property with the string values to snippet.
// property_path is the path in the document to current_property.
//
// MatchOptions holds the snippet spec and number of desired matches remaining.
// Each call to GetEntriesFromProperty will decrement max_matches_remaining
// by the number of entries that it adds to snippet_proto.
//
// The SnippetEntries found for matched content will be added to snippet_proto.
void GetEntriesFromProperty(const PropertyProto* current_property,
                            const std::string& property_path,
                            const TokenMatcher* matcher,
                            const Tokenizer* tokenizer,
                            MatchOptions* match_options,
                            SnippetProto* snippet_proto) {
  // We're at the end. Let's check our values.
  for (int i = 0; i < current_property->string_values_size(); ++i) {
    SnippetProto::EntryProto snippet_entry;
    snippet_entry.set_property_name(AddIndexToPath(
        current_property->string_values_size(), /*index=*/i, property_path));
    std::string_view value = current_property->string_values(i);
    std::unique_ptr<Tokenizer::Iterator> iterator =
        tokenizer->Tokenize(value).ValueOrDie();
    while (iterator->Advance()) {
      Token token = iterator->GetToken();
      if (matcher->Matches(token)) {
        // If there was an error while retrieving the match, the tokenizer
        // iterator is probably in an invalid state. There's nothing we can do
        // here, so just return.
        SectionData data = {property_path, value};
        SnippetMatchProto match =
            RetrieveMatch(match_options->snippet_spec, data, iterator.get())
                .ValueOrDie();
        snippet_entry.mutable_snippet_matches()->Add(std::move(match));
        if (--match_options->max_matches_remaining <= 0) {
          *snippet_proto->add_entries() = std::move(snippet_entry);
          return;
        }
      }
    }
    if (!snippet_entry.snippet_matches().empty()) {
      *snippet_proto->add_entries() = std::move(snippet_entry);
    }
  }
}

// Retrieves snippets in document from content at section_path.
// Tokenizer is provided to tokenize string content and matcher is provided to
// indicate when a token matches content in the query.
//
// section_path_index refers to the current property that is held by document.
// current_path is equivalent to the first section_path_index values in
// section_path, but with value indices present.
//
// For example, suppose that a hit appeared somewhere in the "bcc.emailAddress".
// The arguments for RetrieveSnippetForSection might be
// {section_path=["bcc", "emailAddress"], section_path_index=0, current_path=""}
// on the first call and
// {section_path=["bcc", "emailAddress"], section_path_index=1,
// current_path="bcc[1]"} on the second recursive call.
//
// MatchOptions holds the snippet spec and number of desired matches remaining.
// Each call to RetrieveSnippetForSection will decrement max_matches_remaining
// by the number of entries that it adds to snippet_proto.
//
// The SnippetEntries found for matched content will be added to snippet_proto.
void RetrieveSnippetForSection(
    const DocumentProto& document, const TokenMatcher* matcher,
    const Tokenizer* tokenizer,
    const std::vector<std::string_view>& section_path, int section_path_index,
    const std::string& current_path, MatchOptions* match_options,
    SnippetProto* snippet_proto) {
  std::string_view next_property_name = section_path.at(section_path_index);
  const PropertyProto* current_property =
      GetProperty(document, next_property_name);
  if (current_property == nullptr) {
    ICING_VLOG(1) << "No property " << next_property_name << " found at path "
                  << current_path;
    return;
  }
  std::string property_path =
      AddPropertyToPath(current_path, next_property_name);
  if (section_path_index == section_path.size() - 1) {
    // We're at the end. Let's check our values.
    GetEntriesFromProperty(current_property, property_path, matcher, tokenizer,
                           match_options, snippet_proto);
  } else {
    // Still got more to go. Let's look through our subdocuments.
    std::vector<SnippetProto::EntryProto> entries;
    for (int i = 0; i < current_property->document_values_size(); ++i) {
      std::string new_path = AddIndexToPath(
          current_property->document_values_size(), /*index=*/i, property_path);
      RetrieveSnippetForSection(current_property->document_values(i), matcher,
                                tokenizer, section_path, section_path_index + 1,
                                new_path, match_options, snippet_proto);
      if (match_options->max_matches_remaining <= 0) {
        break;
      }
    }
  }
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<SnippetRetriever>>
SnippetRetriever::Create(const SchemaStore* schema_store,
                         const LanguageSegmenter* language_segmenter,
                         const Normalizer* normalizer) {
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(language_segmenter);
  ICING_RETURN_ERROR_IF_NULL(normalizer);

  return std::unique_ptr<SnippetRetriever>(
      new SnippetRetriever(schema_store, language_segmenter, normalizer));
}

SnippetProto SnippetRetriever::RetrieveSnippet(
    const SectionRestrictQueryTermsMap& query_terms,
    TermMatchType::Code match_type,
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    const DocumentProto& document, SectionIdMask section_id_mask) const {
  SnippetProto snippet_proto;
  ICING_ASSIGN_OR_RETURN(SchemaTypeId type_id,
                         schema_store_.GetSchemaTypeId(document.schema()),
                         snippet_proto);
  const std::unordered_set<std::string> empty_set;
  auto itr = query_terms.find("");
  const std::unordered_set<std::string>& unrestricted_set =
      (itr != query_terms.end()) ? itr->second : empty_set;
  while (section_id_mask != kSectionIdMaskNone) {
    SectionId section_id = __builtin_ctz(section_id_mask);
    // Remove this section from the mask.
    section_id_mask &= ~(1u << section_id);

    MatchOptions match_options = {snippet_spec};
    match_options.max_matches_remaining =
        std::min(snippet_spec.num_to_snippet() - snippet_proto.entries_size(),
                 snippet_spec.num_matches_per_property());

    // Determine the section name and match type.
    auto section_metadata_or =
        schema_store_.GetSectionMetadata(type_id, section_id);
    if (!section_metadata_or.ok()) {
      continue;
    }
    const SectionMetadata* metadata = section_metadata_or.ValueOrDie();
    std::vector<std::string_view> section_path =
        absl_ports::StrSplit(metadata->path, kPropertySeparator);

    // Match type must be as restrictive as possible. Prefix matches for a
    // snippet should only be included if both the query is Prefix and the
    // section has prefixes enabled.
    TermMatchType::Code section_match_type = TermMatchType::EXACT_ONLY;
    if (match_type == TermMatchType::PREFIX &&
        metadata->term_match_type == TermMatchType::PREFIX) {
      section_match_type = TermMatchType::PREFIX;
    }

    itr = query_terms.find(metadata->path);
    const std::unordered_set<std::string>& restricted_set =
        (itr != query_terms.end()) ? itr->second : empty_set;
    libtextclassifier3::StatusOr<std::unique_ptr<TokenMatcher>> matcher_or =
        CreateTokenMatcher(section_match_type, unrestricted_set, restricted_set,
                           normalizer_);
    if (!matcher_or.ok()) {
      continue;
    }
    std::unique_ptr<TokenMatcher> matcher = std::move(matcher_or).ValueOrDie();

    auto tokenizer_or = tokenizer_factory::CreateIndexingTokenizer(
        metadata->tokenizer, &language_segmenter_);
    if (!tokenizer_or.ok()) {
      // If we couldn't create the tokenizer properly, just skip this section.
      continue;
    }
    std::unique_ptr<Tokenizer> tokenizer = std::move(tokenizer_or).ValueOrDie();
    RetrieveSnippetForSection(
        document, matcher.get(), tokenizer.get(), section_path,
        /*section_path_index=*/0, "", &match_options, &snippet_proto);
  }
  return snippet_proto;
}

}  // namespace lib
}  // namespace icing
