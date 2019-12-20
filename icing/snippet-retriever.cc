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

#include "icing/snippet-retriever.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <string_view>
#include <unordered_set>

#include "utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/section.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/i18n-utils.h"
#include "unicode/utf8.h"

namespace icing {
namespace lib {

namespace {

class TokenMatcher {
 public:
  virtual ~TokenMatcher() = default;
  virtual bool Matches(Token token) const = 0;
};

class TokenMatcherExact : public TokenMatcher {
 public:
  explicit TokenMatcherExact(
      const std::unordered_set<std::string>& unrestricted_query_terms,
      const std::unordered_set<std::string>& restricted_query_terms)
      : unrestricted_query_terms_(unrestricted_query_terms),
        restricted_query_terms_(restricted_query_terms) {}

  bool Matches(Token token) const override {
    // TODO(tjbarron) : Add normalization of token.
    std::string s(token.text);
    return (unrestricted_query_terms_.count(s) > 0) ||
           (restricted_query_terms_.count(s) > 0);
  }

 private:
  const std::unordered_set<std::string>& unrestricted_query_terms_;
  const std::unordered_set<std::string>& restricted_query_terms_;
};

class TokenMatcherPrefix : public TokenMatcher {
 public:
  explicit TokenMatcherPrefix(
      const std::unordered_set<std::string>& unrestricted_query_terms,
      const std::unordered_set<std::string>& restricted_query_terms)
      : unrestricted_query_terms_(unrestricted_query_terms),
        restricted_query_terms_(restricted_query_terms) {}

  bool Matches(Token token) const override {
    if (std::any_of(unrestricted_query_terms_.begin(),
                    unrestricted_query_terms_.end(),
                    [&token](const std::string& term) {
                      return term.length() <= token.text.length() &&
                             token.text.compare(0, term.length(), term) == 0;
                    })) {
      return true;
    }
    return std::any_of(restricted_query_terms_.begin(),
                       restricted_query_terms_.end(),
                       [token](const std::string& term) {
                         return term.length() <= token.text.length() &&
                                token.text.compare(0, term.length(), term) == 0;
                       });
  }

 private:
  const std::unordered_set<std::string>& unrestricted_query_terms_;
  const std::unordered_set<std::string>& restricted_query_terms_;
};

libtextclassifier3::StatusOr<std::unique_ptr<TokenMatcher>> CreateTokenMatcher(
    TermMatchType::Code match_type,
    const std::unordered_set<std::string>& unrestricted_query_terms,
    const std::unordered_set<std::string>& restricted_query_terms) {
  switch (match_type) {
    case TermMatchType::EXACT_ONLY:
      return std::make_unique<TokenMatcherExact>(unrestricted_query_terms,
                                                 restricted_query_terms);
    case TermMatchType::PREFIX:
      return std::make_unique<TokenMatcherPrefix>(unrestricted_query_terms,
                                                  restricted_query_terms);
    case TermMatchType::UNKNOWN:
      U_FALLTHROUGH;
    default:
      return absl_ports::InvalidArgumentError("Invalid match type provided.");
  }
}

// Returns true if token matches any of the terms in query terms according to
// the provided match type.

// Returns:
//   the position of the window start if successful
//   INTERNAL_ERROR - if a tokenizer error is encountered
libtextclassifier3::StatusOr<int> DetermineWindowStart(
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    std::string_view value, int match_mid, Tokenizer::Iterator* iterator) {
  int window_start_min =
      std::max((match_mid - snippet_spec.max_window_bytes() / 2), 0);
  if (window_start_min == 0) {
    return 0;
  }
  if (!iterator->ResetToTokenAfter(window_start_min - 1)) {
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
      std::min((match_mid + snippet_spec.max_window_bytes() / 2),
               static_cast<int>(value.length()));
  if (window_end_max_exclusive == value.length()) {
    return window_end_max_exclusive;
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
  // Identifies which subsection of the section content, section_subcontent has
  // come from.
  // Ex. "recipient.address" :
  //       ["foo@google.com", "bar@google.com", "baz@google.com"]
  // The subcontent_index of "bar@google.com" is 1.
  int subcontent_index;
};

libtextclassifier3::StatusOr<SnippetMatchProto> RetrieveMatch(
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    const SectionData& value, Tokenizer::Iterator* iterator) {
  SnippetMatchProto snippet_match;
  snippet_match.set_values_index(value.subcontent_index);

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

    // Reset the iterator back to the original position.
    if (!iterator->ResetToTokenAfter(match_pos - 1)) {
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

libtextclassifier3::StatusOr<SnippetProto::EntryProto> RetrieveMatches(
    const TokenMatcher* matcher, const MatchOptions& match_options,
    const SectionData& value, const Tokenizer* tokenizer) {
  SnippetProto::EntryProto snippet_entry;
  snippet_entry.set_property_name(std::string(value.section_name));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> iterator,
                         tokenizer->Tokenize(value.section_subcontent));
  while (iterator->Advance()) {
    if (snippet_entry.snippet_matches_size() >=
        match_options.max_matches_remaining) {
      break;
    }
    Token token = iterator->GetToken();
    if (matcher->Matches(token)) {
      // If there was an error while retrieving the match, the tokenizer
      // iterator is probably in an invalid state. There's nothing we can do
      // here, so just return.
      ICING_ASSIGN_OR_RETURN(
          SnippetMatchProto match,
          RetrieveMatch(match_options.snippet_spec, value, iterator.get()));
      snippet_entry.mutable_snippet_matches()->Add(std::move(match));
    }
  }
  if (snippet_entry.snippet_matches().empty()) {
    return absl_ports::NotFoundError("No matches found in value!");
  }
  return snippet_entry;
}

}  // namespace

SnippetProto SnippetRetriever::RetrieveSnippet(
    const SectionRestrictQueryTermsMap& query_terms,
    TermMatchType::Code match_type,
    const ResultSpecProto::SnippetSpecProto& snippet_spec,
    const DocumentProto& document, SectionIdMask section_id_mask) const {
  SnippetProto snippet_proto;
  ICING_ASSIGN_OR_RETURN_VAL(SchemaTypeId type_id,
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

    // Determine the section name and match type.
    auto section_metadata_or =
        schema_store_.GetSectionMetadata(type_id, section_id);
    if (!section_metadata_or.ok()) {
      continue;
    }
    const SectionMetadata* metadata = section_metadata_or.ValueOrDie();
    MatchOptions match_options = {snippet_spec};
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
        CreateTokenMatcher(section_match_type, unrestricted_set,
                           restricted_set);
    if (!matcher_or.ok()) {
      continue;
    }
    match_options.max_matches_remaining =
        snippet_spec.num_matches_per_property();

    // Retrieve values and snippet them.
    auto values_or = schema_store_.GetSectionContent(document, metadata->path);
    if (!values_or.ok()) {
      continue;
    }
    auto tokenizer_or = tokenizer_factory::CreateIndexingTokenizer(
        metadata->tokenizer, &language_segmenter_);
    if (!tokenizer_or.ok()) {
      // If we couldn't create the tokenizer properly, just skip this section.
      continue;
    }
    std::vector<std::string> values = values_or.ValueOrDie();
    for (int value_index = 0; value_index < values.size(); ++value_index) {
      if (match_options.max_matches_remaining <= 0) {
        break;
      }
      SectionData value = {metadata->path, values.at(value_index), value_index};
      auto entry_or =
          RetrieveMatches(matcher_or.ValueOrDie().get(), match_options, value,
                          tokenizer_or.ValueOrDie().get());

      // Drop any entries that encountered errors or didn't find any matches.
      if (entry_or.ok()) {
        match_options.max_matches_remaining -=
            entry_or.ValueOrDie().snippet_matches_size();
        snippet_proto.mutable_entries()->Add(std::move(entry_or).ValueOrDie());
      }
    }
  }
  return snippet_proto;
}

}  // namespace lib
}  // namespace icing
