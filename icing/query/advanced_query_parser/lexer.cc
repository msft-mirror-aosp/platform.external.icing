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

#include "icing/query/advanced_query_parser/lexer.h"

#include <string>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/util/i18n-utils.h"

namespace icing {
namespace lib {

bool Lexer::ConsumeWhitespace() {
  if (current_char_ == '\0') {
    return false;
  }
  if (i18n_utils::IsWhitespaceAt(query_, current_index_)) {
    UChar32 uchar32 = i18n_utils::GetUChar32At(query_.data(), query_.length(),
                                               current_index_);
    int length = i18n_utils::GetUtf8Length(uchar32);
    Advance(length);
    return true;
  }
  return false;
}

bool Lexer::ConsumeQuerySingleChar() {
  if (current_char_ != ':') {
    return false;
  }
  tokens_.push_back({":", TokenType::COMPARATOR});
  Advance();
  return true;
}

bool Lexer::ConsumeScoringSingleChar() {
  switch (current_char_) {
    case '+':
      tokens_.push_back({"", TokenType::PLUS});
      break;
    case '*':
      tokens_.push_back({"", TokenType::TIMES});
      break;
    case '/':
      tokens_.push_back({"", TokenType::DIV});
      break;
    default:
      return false;
  }
  Advance();
  return true;
}

bool Lexer::ConsumeGeneralSingleChar() {
  switch (current_char_) {
    case ',':
      tokens_.push_back({"", TokenType::COMMA});
      break;
    case '.':
      tokens_.push_back({"", TokenType::DOT});
      break;
    case '-':
      tokens_.push_back({"", TokenType::MINUS});
      break;
    case '(':
      tokens_.push_back({"", TokenType::LPAREN});
      break;
    case ')':
      tokens_.push_back({"", TokenType::RPAREN});
      break;
    default:
      return false;
  }
  Advance();
  return true;
}

bool Lexer::ConsumeSingleChar() {
  if (language_ == Language::QUERY) {
    if (ConsumeQuerySingleChar()) {
      return true;
    }
  } else if (language_ == Language::SCORING) {
    if (ConsumeScoringSingleChar()) {
      return true;
    }
  }
  return ConsumeGeneralSingleChar();
}

bool Lexer::ConsumeComparator() {
  if (current_char_ != '<' && current_char_ != '>' && current_char_ != '!' &&
      current_char_ != '=') {
    return false;
  }
  // Now, current_char_ must be one of '<', '>', '!', or '='.
  // Matching for '<=', '>=', '!=', or '=='.
  char next_char = PeekNext(1);
  if (next_char == '=') {
    tokens_.push_back({{current_char_, next_char}, TokenType::COMPARATOR});
    Advance(2);
    return true;
  }
  // Now, next_char must not be '='. Let's match for '<' and '>'.
  if (current_char_ == '<' || current_char_ == '>') {
    tokens_.push_back({{current_char_}, TokenType::COMPARATOR});
    Advance();
    return true;
  }
  return false;
}

bool Lexer::ConsumeAndOr() {
  if (current_char_ != '&' && current_char_ != '|') {
    return false;
  }
  char next_char = PeekNext(1);
  if (current_char_ != next_char) {
    return false;
  }
  if (current_char_ == '&') {
    tokens_.push_back({"", TokenType::AND});
  } else {
    tokens_.push_back({"", TokenType::OR});
  }
  Advance(2);
  return true;
}

bool Lexer::ConsumeStringLiteral() {
  if (current_char_ != '"') {
    return false;
  }
  std::string text;
  Advance();
  while (current_char_ != '\0' && current_char_ != '"') {
    // When getting a backslash, we will always match the next character, even
    // if the next character is a quotation mark
    if (current_char_ == '\\') {
      text.push_back(current_char_);
      Advance();
      if (current_char_ == '\0') {
        // In this case, we are missing a terminating quotation mark.
        break;
      }
    }
    text.push_back(current_char_);
    Advance();
  }
  if (current_char_ == '\0') {
    SyntaxError("missing terminating \" character");
    return false;
  }
  tokens_.push_back({text, TokenType::STRING});
  Advance();
  return true;
}

bool Lexer::Text() {
  if (current_char_ == '\0') {
    return false;
  }
  tokens_.push_back({"", TokenType::TEXT});
  int token_index = tokens_.size() - 1;
  while (!ConsumeNonText() && current_char_ != '\0') {
    // When getting a backslash in TEXT, unescape it by accepting its following
    // character no matter which character it is, including white spaces,
    // operator symbols, parentheses, etc.
    if (current_char_ == '\\') {
      Advance();
      if (current_char_ == '\0') {
        SyntaxError("missing a escaping character after \\");
        break;
      }
    }
    tokens_[token_index].text.push_back(current_char_);
    Advance();
    if (current_char_ == '(') {
      // A TEXT followed by a LPAREN is a FUNCTION_NAME.
      tokens_.back().type = TokenType::FUNCTION_NAME;
      // No need to break, since NonText() must be true at this point.
    }
  }
  if (language_ == Lexer::Language::QUERY) {
    std::string &text = tokens_[token_index].text;
    TokenType &type = tokens_[token_index].type;
    if (text == "AND") {
      text.clear();
      type = TokenType::AND;
    } else if (text == "OR") {
      text.clear();
      type = TokenType::OR;
    } else if (text == "NOT") {
      text.clear();
      type = TokenType::NOT;
    }
  }
  return true;
}

libtextclassifier3::StatusOr<std::vector<Lexer::LexerToken>>
Lexer::ExtractTokens() {
  while (current_char_ != '\0') {
    // Clear out any non-text before matching a Text.
    while (ConsumeNonText()) {
    }
    Text();
  }
  if (!error_.empty()) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Syntax Error: ", error_));
  }
  if (tokens_.size() > kMaxNumTokens) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("The maximum number of tokens allowed is ",
                           std::to_string(kMaxNumTokens), ", but got ",
                           std::to_string(tokens_.size()), " tokens."));
  }
  return tokens_;
}

}  // namespace lib
}  // namespace icing
