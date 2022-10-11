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

#ifndef ICING_TOKENIZATION_URL_TOKENIZER_H_
#define ICING_TOKENIZATION_URL_TOKENIZER_H_

#include <string>
#include <string_view>
#include <vector>

#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"

namespace icing {
namespace lib {

// Provides tokenization for URL strings.
// The input string must be a single URL and must have a standard URL scheme.
// (See implementation for list of standard schemes)
//
// The tokenizer splits the url into a token for each significant components,
// as well as any valid url suffixes. For example:
//
// Input URL string:
// "http://user:pass@www.google.com:99/path/sub_path?param1=abc&param2=def#ref"
//
// Output tokens:
// 1.  Token::Type::URL_SCHEME, "http"
// 2.  Token::Type::URL_USERNAME, "user"
// 3.  Token::Type::URL_PASSWORD, "pass"
// 4.  Token::Type::URL_HOST_COMMON_PART, "www"
// 5.  Token::Type::URL_HOST_SIGNIFICANT_PART, "google"
// 6.  Token::Type::URL_HOST_COMMON_PART, "com"
// 7.  Token::Type::URL_PORT, "99"
// 8.  Token::Type::URL_PATH_PART, "path"
// 9.  Token::Type::URL_PATH_PART, "sub_path"
// 10. Token::Type::URL_QUERY, "param1=abc&param2=def"
// 11. Token::Type::URL_REF, "ref"
// 12. Token::Type::URL_SUFFIX,
//      "http://user:pass@www.google.com:99/path/sub_path?param1=abc&param2=def#ref"
// 13. Token::Type::URL_SUFFIX,
//      "www.google.com:99/path/sub_path?param1=abc&param2=def#ref"
// 14. Token::Type::URL_SUFFIX_INNERMOST,
//      "google.com:99/path/sub_path?param1=abc&param2=def#ref"
//
class UrlTokenizer : public Tokenizer {
 public:
  libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>> Tokenize(
      std::string_view text) const override;

  libtextclassifier3::StatusOr<std::vector<Token>> TokenizeAll(
      std::string_view text) const override;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_URL_TOKENIZER_H_
