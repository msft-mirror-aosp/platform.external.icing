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

#include "icing/tokenization/url-tokenizer.h"

#include <string_view>
#include <unordered_set>

#include "third_party/googleurl/src/url/third_party/mozilla/url_parse.h"
#include "icing/absl_ports/arraysize_macros.h"
#include "icing/absl_ports/ascii_str_to_lower.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/character-iterator.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

class CommonComponents {
 public:
  CommonComponents() {
    // Only these standard schemes are supported.
    standard_schemes_.insert("http");
    standard_schemes_.insert("https");
    standard_schemes_.insert("ftp");
    standard_schemes_.insert("content");

    common_sub_domains_.insert("m");
    common_sub_domains_.insert("www");
    common_sub_domains_.insert("www1");
    common_sub_domains_.insert("ww2");
    common_sub_domains_.insert("www2");
  }

  bool IsStandardScheme(std::string_view text,
                        const url::Component& scheme) const {
    std::string_view scheme_text = text.substr(scheme.begin, scheme.len);
    return standard_schemes_.find(absl_ports::AsciiStrToLower(scheme_text)) !=
           standard_schemes_.end();
  }

  bool IsCommonSubDomain(std::string_view subdomain_text) const {
    return common_sub_domains_.find(absl_ports::AsciiStrToLower(
               subdomain_text)) != common_sub_domains_.end();
  }

 private:
  std::unordered_set<std::string> standard_schemes_;
  std::unordered_set<std::string> common_sub_domains_;
};

static const CommonComponents kCommonComponents;
constexpr static char kUrlHostComponentSeparator = '.';
constexpr static char kUrlPathComponentSeparator = '/';

class UrlTokenizerIterator : public Tokenizer::Iterator {
 public:
  explicit UrlTokenizerIterator(std::string_view text)
      : text_(std::move(text)),
        iterator_(text, 0, 0, 0),
        state_(State::SPLIT_TOKENS),
        token_(std::make_unique<Token>(Token::Type::INVALID)),
        current_component_idx_(0),
        start_idx_(0),
        end_idx_(text_.length()) {
    SetupValidComponents();
  }

  bool Advance() override {
    if (iterator_.utf8_index() >= end_idx_) {
      return false;
    }

    switch (state_) {
      case State::SPLIT_TOKENS:
        GetSplitToken();
        if (iterator_.utf8_index() ==
            valid_components_[current_component_idx_].component.end()) {
          if (current_component_idx_ == valid_components_.size() - 1) {
            // Need to switch to SUFFIX state.
            state_ = State::SUFFIX;
            iterator_.RewindToUtf8(start_idx_);
            return true;
          }
          current_component_idx_++;
        } else {
          // Verify that we're currently in the host or path component.
          if (!(in_component(url::Parsed::HOST) ||
                in_component(url::Parsed::PATH))) {
            ICING_LOG(ERROR) << "Stopped in the middle of a component in "
                                "UrlTokenizerIterator that is not HOST or PATH";
            return false;
          }
        }
        break;
      case State::SUFFIX:
        GetSuffixToken();
        break;
      case State::INVALID:
        return false;
      default:
        ICING_LOG(ERROR) << "Unexpected state in UrlTokenizerIterator";
        return false;
    }

    return true;
  }

  std::vector<Token> GetTokens() const override {
    std::vector<Token> result;

    if (state_ == State::INVALID || token_->type == Token::Type::INVALID) {
      return result;
    }

    result.push_back(*token_);
    return result;
  }

 private:
  enum class State {
    SPLIT_TOKENS,  // Returning split tokens, [http], [www], [google] etc.
    SUFFIX,        // Returning url suffix tokens, e.g.
                   // [http://www.google.com], [www.google.com], [google.com]
    INVALID,       // Indicates that the URL is not supported or that the
                   // iterator has entered some invalid state.
  };

  struct UrlComponentPair {
    explicit UrlComponentPair(url::Component component,
                              url::Parsed::ComponentType type)
        : component(component), type(type) {}

    url::Component component;
    url::Parsed::ComponentType type;
  };

  // Parses url components using the URL parser and stores them in
  // valid_components_ as UrlComponentPair(component, type) pairs.
  // Also checks that the input URL is valid and that the scheme is supported.
  void SetupValidComponents() {
    url::Component scheme;
    if (!url::ExtractScheme(text_.data(), text_.length(), &scheme)) {
      ICING_LOG(ERROR) << "Failed to parse scheme";
      state_ = State::INVALID;
      return;
    } else if (!scheme.is_nonempty() ||
               !kCommonComponents.IsStandardScheme(text_, scheme)) {
      ICING_LOG(WARNING) << "Unsupported scheme";
      state_ = State::INVALID;
      return;
    }

    url::Parsed parsed;
    url::ParseStandardURL(text_.data(), text_.length(), &parsed);
    bool is_valid = parsed.scheme.is_valid() && parsed.host.is_valid() &&
                parsed.host.is_nonempty();
    if (!is_valid) {
      ICING_LOG(WARNING) << "Invalid URL";
      state_ = State::INVALID;
      return;
    }

    // Store all valid, non-empty components in valid_components_.
    const struct {
      url::Component* component;
      url::Parsed::ComponentType type;
    } components[] = {
        {&parsed.scheme, url::Parsed::SCHEME},
        {&parsed.username, url::Parsed::USERNAME},
        {&parsed.password, url::Parsed::PASSWORD},
        {&parsed.host, url::Parsed::HOST},
        {&parsed.port, url::Parsed::PORT},
        {&parsed.path, url::Parsed::PATH},
        {&parsed.query, url::Parsed::QUERY},
        {&parsed.ref, url::Parsed::REF},
    };

    for (size_t i = 0; i < ABSL_PORT_ARRAYSIZE(components); i++) {
      const url::Component* component = components[i].component;
      const url::Parsed::ComponentType type = components[i].type;
      // The parsed path compponent always starts with a '/'.
      // We want to treat the path "/" as an empty path.
      bool is_nonempty = component->is_nonempty() &&
                         (type != url::Parsed::PATH || component->len > 1);
      if (component->is_valid() && is_nonempty) {
        valid_components_.push_back(UrlComponentPair(*component, type));
      }
    }

    // Set the start and end indexes of the string in case the input string_view
    // has any leading or trailing spaces.
    start_idx_ = valid_components_[0].component.begin;
    end_idx_ = valid_components_[valid_components_.size() - 1].component.end();
    // This is the case where the url ends with a trailing backslash. The last
    // backslash is not considered a valid component, but is still a part of the
    // url and should be present in the suffix tokens. Push end_idx_ back by one
    // more to account for the trailing backslash.
    if (end_idx_ < text_.length() &&
        text_.at(end_idx_) == kUrlPathComponentSeparator) {
      end_idx_++;
    }
    iterator_.MoveToUtf8(start_idx_);

    // These values are stored for finding tokens quickly in state SUFFIX.
    scheme_start_idx_ = parsed.scheme.begin;
    host_start_idx_ = parsed.host.begin;
    innermost_host_start_idx_ = -1;
  }

  // Stores a split token in *token_ and advances iterator_ to the end of the
  // token.
  void GetSplitToken() {
    if (state_ != State::SPLIT_TOKENS || iterator_.utf8_index() >= end_idx_) {
      return;
    }

    // Iterate to start of token.
    if (current_component_idx_ > 0 &&
        iterator_.utf8_index() ==
            valid_components_[current_component_idx_ - 1].component.end()) {
      iterator_.AdvanceToUtf8(
          valid_components_[current_component_idx_].component.begin);
    }
    // This is the case where we're in the middle of the host/path components
    // and need to split the component up into smaller tokens.
    // The url-parser parses the whole host/path as a single component but our
    // tokenizer needs to separate it into many split tokens.
    //
    // Eg. www.google.com/path/subpath would be parsed as:
    // Host component: www.google.com
    // Path component: /path/subpath
    // But we want to split this into 3 separate host tokens (www, google, com)
    // and 2 separate path tokens (path, subpath).
    // Calling AdvanceInHostOrPath() to find the individual tokens here.
    if (in_component(url::Parsed::HOST) || in_component(url::Parsed::PATH)) {
      AdvanceInHostOrPath(/*advance_to_start=*/true);
    }
    int token_start = iterator_.utf8_index();

    // Iterate to end of token.
    if (in_component(url::Parsed::HOST) || in_component(url::Parsed::PATH)) {
      AdvanceInHostOrPath(/*advance_to_start=*/false);
    } else {
      iterator_.AdvanceToUtf8(
          valid_components_[current_component_idx_].component.end());
    }
    int token_end = iterator_.utf8_index();

    if (token_start >= token_end) {
      token_->type = Token::Type::INVALID;
      token_->text = "";
      ICING_LOG(ERROR) << "Unexpected empty token in UrlTokenizerIterator";
      return;
    }

    std::string_view token_text =
        text_.substr(token_start, token_end - token_start);

    bool is_common_host = false;
    // If the current token is a part of the host, find out whether it's
    // common or significant.
    if (in_component(url::Parsed::HOST)) {
      is_common_host = IsCommonHostToken(token_text, token_start, token_end);

      // If the host is common, suffix starting with host is still a regular
      // suffix.
      if (innermost_host_start_idx_ == -1 && !is_common_host) {
        innermost_host_start_idx_ = token_start;
      }
    }
    token_->type = GetTokenType(valid_components_[current_component_idx_].type,
                                is_common_host);
    token_->text = token_text;
  }

  // Stores a suffix token in *token_ and advances iterator_ to the end of the
  // token.
  void GetSuffixToken() {
    if (state_ != State::SUFFIX) {
      return;
    }

    int cursor = iterator_.utf8_index();
    std::string_view token_text = text_.substr(cursor, end_idx_ - cursor);
    token_->text = token_text;

    if (cursor == scheme_start_idx_) {
      token_->type = Token::Type::URL_SUFFIX;
      iterator_.AdvanceToUtf8(host_start_idx_);
    } else if (cursor == innermost_host_start_idx_) {
      // can only be true if innermost_host_start_idx has been set (!= -1) which
      // indicates that the suffix is the innermost case
      token_->type = Token::Type::URL_SUFFIX_INNERMOST;
      iterator_.AdvanceToUtf8(end_idx_);
    } else if (cursor == host_start_idx_) {
      token_->type = Token::Type::URL_SUFFIX;
      if (innermost_host_start_idx_ == -1) {
        iterator_.AdvanceToUtf8(end_idx_);
      } else {
        iterator_.AdvanceToUtf8(innermost_host_start_idx_);
      }
    }
  }

  // Returns true if host token is common.
  bool IsCommonHostToken(std::string_view token_text, int token_start,
                         int token_end) {
    bool at_host_start =
        token_start ==
        valid_components_[current_component_idx_].component.begin;
    bool at_host_end =
        token_end == valid_components_[current_component_idx_].component.end();

    // If the token is the whole host, it's never common.
    if (at_host_start && at_host_end) {
      return false;
    }

    bool is_common_subdomain =
        at_host_start && kCommonComponents.IsCommonSubDomain(token_text);
    // If the token is the last part of the host, it's most likely a top-level
    // domain, which is common.
    bool is_top_level_domain = at_host_end;
    return is_common_subdomain || is_top_level_domain;
  }

  // Advances iterator_ in the host or path component. advance_to_start is true
  // if advancing to the start of the token.
  void AdvanceInHostOrPath(bool advance_to_start) {
    if (!(in_component(url::Parsed::HOST) || in_component(url::Parsed::PATH))) {
      return;
    }

    char component_separation = in_component(url::Parsed::HOST)
                                    ? kUrlHostComponentSeparator
                                    : kUrlPathComponentSeparator;
    while (iterator_.utf8_index() <
               valid_components_[current_component_idx_].component.end() &&
           (iterator_.GetCurrentChar() == component_separation) ==
               advance_to_start) {
      iterator_.AdvanceToUtf8(iterator_.utf8_index() + 1);
    }
  }

  bool in_component(url::Parsed::ComponentType type) const {
    return current_component_idx_ != -1 &&
           valid_components_[current_component_idx_].type == type;
  }

  Token::Type GetTokenType(url::Parsed::ComponentType component_type,
                           bool is_common_host) {
    switch (component_type) {
      case url::Parsed::SCHEME:
        return Token::Type::URL_SCHEME;
      case url::Parsed::USERNAME:
        return Token::Type::URL_USERNAME;
      case url::Parsed::PASSWORD:
        return Token::Type::URL_PASSWORD;
      case url::Parsed::HOST:
        return is_common_host ? Token::Type::URL_HOST_COMMON_PART
                              : Token::Type::URL_HOST_SIGNIFICANT_PART;
      case url::Parsed::PORT:
        return Token::Type::URL_PORT;
      case url::Parsed::PATH:
        return Token::Type::URL_PATH_PART;
      case url::Parsed::QUERY:
        return Token::Type::URL_QUERY;
      case url::Parsed::REF:
        return Token::Type::URL_REF;
      default:
        ICING_LOG(ERROR) << "unrecognized component type";
        return Token::Type::INVALID;
    }
  }

  std::string_view text_;
  CharacterIterator iterator_;
  State state_;
  std::unique_ptr<Token> token_;

  // Index of the current component retrieved by the url parser
  int current_component_idx_;
  // Parsed components retrieved by the url parser.
  std::vector<UrlComponentPair> valid_components_;

  // Start and end indexes of the actual url without leading or trailing spaces.
  //
  // Start index of the first valid_component of the url
  int start_idx_;
  // End index of the last valid_component of the url (index is one after the
  // last character). If the url has a trailing backslash, the index will be one
  // after the backslash.
  int end_idx_;

  // Indexes needed for SUFFIX state.
  int scheme_start_idx_;
  int host_start_idx_;
  int innermost_host_start_idx_;
};

libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>>
UrlTokenizer::Tokenize(std::string_view text) const {
  return std::make_unique<UrlTokenizerIterator>(text);
}

libtextclassifier3::StatusOr<std::vector<Token>> UrlTokenizer::TokenizeAll(
    std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> iterator,
                         Tokenize(text));
  std::vector<Token> tokens;
  while (iterator->Advance()) {
    std::vector<Token> batch_tokens = iterator->GetTokens();
    tokens.insert(tokens.end(), batch_tokens.begin(), batch_tokens.end());
  }
  return tokens;
}

}  // namespace lib
}  // namespace icing
