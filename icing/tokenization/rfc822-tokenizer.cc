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

#include "icing/tokenization/rfc822-tokenizer.h"

#include <algorithm>
#include <deque>
#include <queue>
#include <string_view>
#include <utility>

#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/character-iterator.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/status-macros.h"
#include "unicode/umachine.h"

namespace icing {
namespace lib {

class Rfc822TokenIterator : public Tokenizer::Iterator {
 public:
  // Cursor is the index into the string_view, text_end_ is the length.
  explicit Rfc822TokenIterator(std::string_view text)
      : term_(std::move(text)),
        iterator_(text, 0, 0, 0),
        text_end_(text.length()) {}

  struct NameInfo {
    NameInfo(const char* at_sign, bool name_found)
        : at_sign(at_sign), name_found(name_found) {}
    const char* at_sign;
    bool name_found;
  };

  bool Advance() override {
    // Advance through the queue.
    if (!token_queue_.empty()) {
      token_queue_.pop_front();
    }

    // There is still something left.
    if (!token_queue_.empty()) {
      return true;
    }

    // Done with the entire string_view
    if (iterator_.utf8_index() >= text_end_) {
      return false;
    }

    AdvancePastWhitespace();

    GetNextRfc822Token();

    return true;
  }

  // Advance until the next email delimiter, generating as many tokens as
  // necessary.
  void GetNextRfc822Token() {
    int token_start = iterator_.utf8_index();
    const char* at_sign_in_name = nullptr;
    bool address_found = false;
    bool name_found = false;
    // We start at unquoted and run until a ",;\n<( .
    while (iterator_.utf8_index() < text_end_) {
      UChar32 c = iterator_.GetCurrentChar();
      if (c == ',' || c == ';' || c == '\n') {
        // End of the token, advance cursor past this then quit
        token_queue_.push_back(Token(
            Token::Type::RFC822_TOKEN,
            term_.substr(token_start, iterator_.utf8_index() - token_start)));
        AdvanceCursor();
        break;
      }

      if (c == '"') {
        NameInfo quoted_result = ConsumeQuotedSection();
        if (quoted_result.at_sign != nullptr) {
          at_sign_in_name = quoted_result.at_sign;
        }
        if (!name_found) {
          name_found = quoted_result.name_found;
        }
      } else if (c == '(') {
        ConsumeParenthesizedSection();
      } else if (c == '<') {
        // Only set address_found to true if ConsumeAdress returns true.
        // Otherwise, keep address_found as is to prevent setting address_found
        // back to false if it is true
        if (ConsumeAddress()) {
          address_found = true;
        }
      } else {
        NameInfo unquoted_result = ConsumeUnquotedSection();
        if (unquoted_result.at_sign != nullptr) {
          at_sign_in_name = unquoted_result.at_sign;
        }
        if (!name_found) {
          name_found = unquoted_result.name_found;
        }
      }
    }
    if (iterator_.utf8_index() >= text_end_) {
      token_queue_.push_back(
          Token(Token::Type::RFC822_TOKEN,
                term_.substr(token_start, text_end_ - token_start)));
    }

    // At this point the token_queue is not empty.
    // If an address is found, use the tokens we have
    // If an address isn't found, and a name isn't found, also use the tokens
    // we have.
    // If an address isn't found but a name is, convert name Tokens to email
    // Tokens
    if (!address_found && name_found) {
      ConvertNameToEmail(at_sign_in_name);
    }
  }

  void ConvertNameToEmail(const char* at_sign_in_name) {
    // The name tokens will be will be used as the address now
    const char* address_start = nullptr;
    const char* local_address_end = nullptr;
    const char* address_end = term_.begin();

    // If we need to transform name tokens into various tokens, we keep the
    // order of which the name tokens appeared. Name tokens that appear before
    // an @ sign in the name will become RFC822_ADDRESS_COMPONENT_LOCAL, and
    // those after will become RFC822_ADDRESS_COMPONENT_HOST. We aren't able
    // to determine RFC822_ADDRESS and RFC822_LOCAL_ADDRESS before checking
    // the name tokens, so they will be added after the component tokens.

    for (Token& token : token_queue_) {
      if (token.type == Token::Type::RFC822_NAME) {
        // Names need to be converted to address tokens
        std::string_view text = token.text;

        // Find the ADDRESS and LOCAL_ADDRESS.
        if (address_start == nullptr) {
          address_start = text.begin();
        }

        if (at_sign_in_name >= text.end()) {
          local_address_end = text.end();
        }

        address_end = text.end();

        if (text.begin() < at_sign_in_name) {
          token = Token(Token::Type::RFC822_ADDRESS_COMPONENT_LOCAL, text);
        } else if (text.begin() > at_sign_in_name) {
          token = Token(Token::Type::RFC822_ADDRESS_COMPONENT_HOST, text);
        }
      }
    }

    token_queue_.push_back(
        Token(Token::Type::RFC822_ADDRESS,
              std::string_view(address_start, address_end - address_start)));

    if (local_address_end != nullptr) {
      token_queue_.push_back(Token(
          Token::Type::RFC822_LOCAL_ADDRESS,
          std::string_view(address_start, local_address_end - address_start)));
    }
  }

  // Returns the location of the last at sign in the unquoted section, and if
  // we have found a name. This is useful in case we do not find an address
  // and have to use the name. An unquoted section may look like "Alex Sav", or
  // "alex@google.com". In the absense of a bracketed email address, the
  // unquoted section will be used as the email address along with the quoted
  // section.
  NameInfo ConsumeUnquotedSection() {
    const char* at_sign_location = nullptr;
    UChar32 c;

    int token_start = -1;
    bool name_found = false;

    // Advance to another state or a character marking the end of token, one
    // of \n,; .
    while (iterator_.utf8_index() < text_end_) {
      c = iterator_.GetCurrentChar();

      if (i18n_utils::IsAlphaNumeric(c)) {
        name_found = true;

        if (token_start == -1) {
          // Start recording
          token_start = iterator_.utf8_index();
        }
        AdvanceCursor();

      } else {
        if (token_start != -1) {
          if (c == '@') {
            // Mark the last @ sign.
            at_sign_location = term_.data() + iterator_.utf8_index();
          }

          // The character is non alphabetic, save a token.
          token_queue_.push_back(Token(
              Token::Type::RFC822_NAME,
              term_.substr(token_start, iterator_.utf8_index() - token_start)));
          token_start = -1;
        }

        if (c == '"' || c == '<' || c == '(' || c == '\n' || c == ';' ||
            c == ',') {
          // Stay on the token.
          break;
        }

        AdvanceCursor();
      }
    }
    if (token_start != -1) {
      token_queue_.push_back(Token(
          Token::Type::RFC822_NAME,
          term_.substr(token_start, iterator_.utf8_index() - token_start)));
    }
    return NameInfo(at_sign_location, name_found);
  }

  // Names that are within quotes should have all characters blindly unescaped.
  // When a name is made into an address, it isn't re-escaped.

  // Returns the location of the last at sign in the quoted section. This is
  // useful in case we do not find an address and have to use the name. The
  // quoted section may contain whitespaces
  NameInfo ConsumeQuotedSection() {
    // Get past the first quote.
    AdvanceCursor();
    const char* at_sign_location = nullptr;

    bool end_quote_found = false;
    bool name_found = false;
    UChar32 c;

    int token_start = -1;

    while (!end_quote_found && (iterator_.utf8_index() < text_end_)) {
      c = iterator_.GetCurrentChar();

      if (i18n_utils::IsAlphaNumeric(c)) {
        name_found = true;

        if (token_start == -1) {
          // Start tracking the token.
          token_start = iterator_.utf8_index();
        }
        AdvanceCursor();

      } else {
        // Non- alphabetic
        if (c == '\\') {
          // A backslash, let's look at the next character.
          CharacterIterator temp = iterator_;
          temp.AdvanceToUtf32(iterator_.utf32_index() + 1);
          UChar32 n = temp.GetCurrentChar();
          if (i18n_utils::IsAlphaNumeric(n)) {
            // The next character is alphabetic, skip the slash and don't end
            // the last token. For quoted sections, the only things that are
            // escaped are double quotes and slashes. For example, in "a\lex",
            // an l appears after the slash. We want to treat this as if it was
            // just "alex". So we tokenize it as <RFC822_NAME, "a\lex">.
            AdvanceCursor();
          } else {
            // Not alphabetic, so save the last token if necessary.
            if (token_start != -1) {
              token_queue_.push_back(
                  Token(Token::Type::RFC822_NAME,
                        term_.substr(token_start,
                                     iterator_.utf8_index() - token_start)));
              token_start = -1;
            }

            // Skip the backslash.
            AdvanceCursor();

            if (n == '"' || n == '\\' || n == '@') {
              // Skip these too if they're next.
              AdvanceCursor();
            }
          }

        } else {
          // Not a backslash.

          if (c == '@') {
            // Mark the last @ sign.
            at_sign_location = term_.data() + iterator_.utf8_index();
          }

          if (token_start != -1) {
            token_queue_.push_back(
                Token(Token::Type::RFC822_NAME,
                      term_.substr(token_start,
                                   iterator_.utf8_index() - token_start)));
            token_start = -1;
          }

          if (c == '"') {
            end_quote_found = true;
          }
          // Advance one more time to get past the non-alphabetic character.
          AdvanceCursor();
        }
      }
    }
    if (token_start != -1) {
      token_queue_.push_back(Token(
          Token::Type::RFC822_NAME,
          term_.substr(token_start, iterator_.utf8_index() - token_start)));
    }
    return NameInfo(at_sign_location, name_found);
  }

  // '(', ')', '\\' chars should be escaped. All other escaped chars should be
  // unescaped.
  void ConsumeParenthesizedSection() {
    // Skip the initial (
    AdvanceCursor();

    int paren_layer = 1;
    UChar32 c;

    int token_start = -1;

    while (paren_layer > 0 && (iterator_.utf8_index() < text_end_)) {
      c = iterator_.GetCurrentChar();

      if (i18n_utils::IsAlphaNumeric(c)) {
        if (token_start == -1) {
          // Start tracking a token.
          token_start = iterator_.utf8_index();
        }
        AdvanceCursor();

      } else {
        // Non alphabetic.
        if (c == '\\') {
          // A backslash, let's look at the next character.
          UChar32 n = i18n_utils::GetUChar32At(term_.data(), term_.length(),
                                               iterator_.utf8_index() + 1);
          if (i18n_utils::IsAlphaNumeric(n)) {
            // Alphabetic, skip the slash and don't end the last token.
            AdvanceCursor();
          } else {
            // Not alphabetic, save the last token if necessary.
            if (token_start != -1) {
              token_queue_.push_back(
                  Token(Token::Type::RFC822_COMMENT,
                        term_.substr(token_start,
                                     iterator_.utf8_index() - token_start)));
              token_start = -1;
            }

            // Skip the backslash.
            AdvanceCursor();

            if (n == ')' || n == '(' || n == '\\') {
              // Skip these too if they're next.
              AdvanceCursor();
            }
          }
        } else {
          // Not a backslash.
          if (token_start != -1) {
            token_queue_.push_back(
                Token(Token::Type::RFC822_COMMENT,
                      term_.substr(token_start,
                                   iterator_.utf8_index() - token_start)));
            token_start = -1;
          }

          if (c == '(') {
            paren_layer++;
          } else if (c == ')') {
            paren_layer--;
          }
          AdvanceCursor();
        }
      }
    }

    if (token_start != -1) {
      // Ran past the end of term_ without getting the last token.

      // substr returns "a view of the substring [pos, pos + // rcount), where
      // rcount is the smaller of count and size() - pos" therefore the count
      // argument can be any value >= this->cursor - token_start. Therefore,
      // ignoring the mutation warning.
      token_queue_.push_back(Token(
          Token::Type::RFC822_COMMENT,
          term_.substr(token_start, iterator_.utf8_index() - token_start)));
    }
  }

  // Returns true if we find an address.
  bool ConsumeAddress() {
    // Skip the first <.
    AdvanceCursor();

    // Save the start position.
    CharacterIterator address_start_iterator = iterator_;

    int at_sign = -1;
    int address_end = -1;

    UChar32 c = iterator_.GetCurrentChar();
    // Quick scan for @ and > signs.
    while (c != '>' && iterator_.utf8_index() < text_end_) {
      AdvanceCursor();
      c = iterator_.GetCurrentChar();
      if (c == '@') {
        at_sign = iterator_.utf8_index();
      }
    }

    if (iterator_.utf8_index() <= address_start_iterator.utf8_index()) {
      // There is nothing between the brackets, either we have "<" or "<>"
      return false;
    }

    // Either we find a > or run to the end, either way this is the end of the
    // address. The ending bracket will be handled by ConsumeUnquoted.
    address_end = iterator_.utf8_index();

    // Reset to the start.
    iterator_ = address_start_iterator;

    int address_start = address_start_iterator.utf8_index();

    Token::Type type = Token::Type::RFC822_ADDRESS_COMPONENT_LOCAL;

    // Create a local address token.
    if (at_sign != -1) {
      token_queue_.push_back(
          Token(Token::Type::RFC822_LOCAL_ADDRESS,
                term_.substr(address_start, at_sign - address_start)));
    } else {
      // All the tokens in the address are host components.
      type = Token::Type::RFC822_ADDRESS_COMPONENT_HOST;
    }

    token_queue_.push_back(
        Token(Token::Type::RFC822_ADDRESS,
              term_.substr(address_start, address_end - address_start)));

    int token_start = -1;

    while (iterator_.utf8_index() < address_end) {
      c = iterator_.GetCurrentChar();

      if (i18n_utils::IsAlphaNumeric(c)) {
        if (token_start == -1) {
          token_start = iterator_.utf8_index();
        }

      } else {
        // non alphabetic
        if (c == '\\') {
          // A backslash, let's look at the next character.
          CharacterIterator temp = iterator_;
          temp.AdvanceToUtf32(iterator_.utf32_index() + 1);
          UChar32 n = temp.GetCurrentChar();
          if (!i18n_utils::IsAlphaNumeric(n)) {
            // Not alphabetic, end the last token if necessary.
            if (token_start != -1) {
              token_queue_.push_back(Token(
                  type, term_.substr(token_start,
                                     iterator_.utf8_index() - token_start)));
              token_start = -1;
            }
          }
        } else {
          // Not backslash.
          if (token_start != -1) {
            token_queue_.push_back(Token(
                type, term_.substr(token_start,
                                   iterator_.utf8_index() - token_start)));
            token_start = -1;
          }
          // Switch to host component tokens.
          if (iterator_.utf8_index() == at_sign) {
            type = Token::Type::RFC822_ADDRESS_COMPONENT_HOST;
          }
        }
      }
      AdvanceCursor();
    }
    if (token_start != -1) {
      token_queue_.push_back(Token(
          type,
          term_.substr(token_start, iterator_.utf8_index() - token_start)));
    }
    // Unquoted will handle the closing bracket > if these is one.
    return true;
  }

  Token GetToken() const override {
    if (token_queue_.empty()) {
      return Token(Token::Type::INVALID, term_);
    }
    return token_queue_.front();
  }

 private:
  void AdvanceCursor() {
    iterator_.AdvanceToUtf32(iterator_.utf32_index() + 1);
  }

  void AdvancePastWhitespace() {
    while (i18n_utils::IsWhitespaceAt(term_, iterator_.utf8_index())) {
      AdvanceCursor();
    }
  }

  std::string_view term_;
  CharacterIterator iterator_;
  int text_end_;

  // A temporary store of Tokens. As we advance through the provided string, we
  // parse entire addresses at a time rather than one token at a time. However,
  // since we call the tokenizer with Advance() alternating with GetToken(), we
  // need to store tokens for subsequent GetToken calls if Advance generates
  // multiple tokens (it usually does). A queue is used as we want the first
  // token generated to be the first token returned from GetToken.
  std::deque<Token> token_queue_;
};

libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>>
Rfc822Tokenizer::Tokenize(std::string_view text) const {
  return std::make_unique<Rfc822TokenIterator>(text);
}

libtextclassifier3::StatusOr<std::vector<Token>> Rfc822Tokenizer::TokenizeAll(
    std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> iterator,
                         Tokenize(text));
  std::vector<Token> tokens;
  while (iterator->Advance()) {
    tokens.push_back(iterator->GetToken());
  }
  return tokens;
}

}  // namespace lib
}  // namespace icing
