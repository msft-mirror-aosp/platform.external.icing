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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {
using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(UrlTokenizerTest, Simple) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://www.google.com");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "www"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
          EqualsToken(Token::Type::URL_SUFFIX, "http://www.google.com"),
          EqualsToken(Token::Type::URL_SUFFIX, "www.google.com"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "google.com"))));
}

TEST(UrlTokenizerTest, LeadingAndTrailingSpacesShouldBeIgnored) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("  http://www.google.com   ");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "www"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
          EqualsToken(Token::Type::URL_SUFFIX, "http://www.google.com"),
          EqualsToken(Token::Type::URL_SUFFIX, "www.google.com"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "google.com"))));
}

TEST(UrlTokenizerTest, UncommonSubDomain) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("https://mail.google.com/calendar/render");

  EXPECT_THAT(url_tokenizer.TokenizeAll(s),
              IsOkAndHolds(ElementsAre(
                  EqualsToken(Token::Type::URL_SCHEME, "https"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "mail"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
                  EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
                  EqualsToken(Token::Type::URL_PATH_PART, "calendar"),
                  EqualsToken(Token::Type::URL_PATH_PART, "render"),
                  EqualsToken(Token::Type::URL_SUFFIX,
                              "https://mail.google.com/calendar/render"),
                  EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                              "mail.google.com/calendar/render"))));
}

TEST(UrlTokenizerTest, SimplePathWithQuery) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("https://mail.google.com/calendar/render?pli=1");

  EXPECT_THAT(url_tokenizer.TokenizeAll(s),
              IsOkAndHolds(ElementsAre(
                  EqualsToken(Token::Type::URL_SCHEME, "https"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "mail"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
                  EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
                  EqualsToken(Token::Type::URL_PATH_PART, "calendar"),
                  EqualsToken(Token::Type::URL_PATH_PART, "render"),
                  EqualsToken(Token::Type::URL_QUERY, "pli=1"),
                  EqualsToken(Token::Type::URL_SUFFIX,
                              "https://mail.google.com/calendar/render?pli=1"),
                  EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                              "mail.google.com/calendar/render?pli=1"))));
}

TEST(UrlTokenizerTest, LongPathWithQuery) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s(
      "http://www.amazon.com/Acer-C720-Chromebook-11-6-Inch-2GB"
      "/dp/B00FNPD1VW/ref=sr_1_1?ie=UTF8&qid=1398274203&sr=8-1");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "www"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "amazon"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
          EqualsToken(Token::Type::URL_PATH_PART,
                      "Acer-C720-Chromebook-11-6-Inch-2GB"),
          EqualsToken(Token::Type::URL_PATH_PART, "dp"),
          EqualsToken(Token::Type::URL_PATH_PART, "B00FNPD1VW"),
          EqualsToken(Token::Type::URL_PATH_PART, "ref=sr_1_1"),
          EqualsToken(Token::Type::URL_QUERY, "ie=UTF8&qid=1398274203&sr=8-1"),
          EqualsToken(
              Token::Type::URL_SUFFIX,
              "http://www.amazon.com/Acer-C720-Chromebook-11-6-Inch-2GB/dp/"
              "B00FNPD1VW/ref=sr_1_1?ie=UTF8&qid=1398274203&sr=8-1"),
          EqualsToken(Token::Type::URL_SUFFIX,
                      "www.amazon.com/Acer-C720-Chromebook-11-6-Inch-2GB/dp/"
                      "B00FNPD1VW/ref=sr_1_1?ie=UTF8&qid=1398274203&sr=8-1"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                      "amazon.com/Acer-C720-Chromebook-11-6-Inch-2GB/dp/"
                      "B00FNPD1VW/ref=sr_1_1?ie=UTF8&qid=1398274203&sr=8-1"))));
}

TEST(UrlTokenizerTest, TrailingBackslashIsNotASplitToken) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://www.google.com/");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "www"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
          EqualsToken(Token::Type::URL_SUFFIX, "http://www.google.com/"),
          EqualsToken(Token::Type::URL_SUFFIX, "www.google.com/"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "google.com/"))));
}

TEST(UrlTokenizerTest, SingleCommonHostIsSignificant) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://www");

  EXPECT_THAT(url_tokenizer.TokenizeAll(s),
              IsOkAndHolds(ElementsAre(
                  EqualsToken(Token::Type::URL_SCHEME, "http"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "www"),
                  EqualsToken(Token::Type::URL_SUFFIX, "http://www"),
                  EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "www"))));
}

TEST(UrlTokenizerTest, DomainWithNoSlashesAndManyDots) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http:domain_without_slashes.but...many.dots");

  EXPECT_THAT(url_tokenizer.TokenizeAll(s),
              IsOkAndHolds(ElementsAre(
                  EqualsToken(Token::Type::URL_SCHEME, "http"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART,
                              "domain_without_slashes"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "but"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "many"),
                  EqualsToken(Token::Type::URL_HOST_COMMON_PART, "dots"),
                  EqualsToken(Token::Type::URL_SUFFIX,
                              "http:domain_without_slashes.but...many.dots"),
                  EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                              "domain_without_slashes.but...many.dots"))));
}

TEST(UrlTokenizerTest, FtpScheme) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("ftp://123.456.789");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "ftp"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "123"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "456"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "789"),
          EqualsToken(Token::Type::URL_SUFFIX, "ftp://123.456.789"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "123.456.789"))));
}

TEST(UrlTokenizerTest, ContentScheme) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("content://authority/path/id");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "content"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "authority"),
          EqualsToken(Token::Type::URL_PATH_PART, "path"),
          EqualsToken(Token::Type::URL_PATH_PART, "id"),
          EqualsToken(Token::Type::URL_SUFFIX, "content://authority/path/id"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                      "authority/path/id"))));
}

TEST(UrlTokenizerTest, UrlWithAllComponents) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://user:pass@host:99/foo?bar#baz");

  EXPECT_THAT(url_tokenizer.TokenizeAll(s),
              IsOkAndHolds(ElementsAre(
                  EqualsToken(Token::Type::URL_SCHEME, "http"),
                  EqualsToken(Token::Type::URL_USERNAME, "user"),
                  EqualsToken(Token::Type::URL_PASSWORD, "pass"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "host"),
                  EqualsToken(Token::Type::URL_PORT, "99"),
                  EqualsToken(Token::Type::URL_PATH_PART, "foo"),
                  EqualsToken(Token::Type::URL_QUERY, "bar"),
                  EqualsToken(Token::Type::URL_REF, "baz"),
                  EqualsToken(Token::Type::URL_SUFFIX,
                              "http://user:pass@host:99/foo?bar#baz"),
                  EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                              "host:99/foo?bar#baz"))));
}

TEST(UrlTokenizerTest, UrlWithAllComponentsAndComplexQuery) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://user:pass@host:99/foo?bar1&bar2=def#baz");

  EXPECT_THAT(url_tokenizer.TokenizeAll(s),
              IsOkAndHolds(ElementsAre(
                  EqualsToken(Token::Type::URL_SCHEME, "http"),
                  EqualsToken(Token::Type::URL_USERNAME, "user"),
                  EqualsToken(Token::Type::URL_PASSWORD, "pass"),
                  EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "host"),
                  EqualsToken(Token::Type::URL_PORT, "99"),
                  EqualsToken(Token::Type::URL_PATH_PART, "foo"),
                  EqualsToken(Token::Type::URL_QUERY, "bar1&bar2=def"),
                  EqualsToken(Token::Type::URL_REF, "baz"),
                  EqualsToken(Token::Type::URL_SUFFIX,
                              "http://user:pass@host:99/foo?bar1&bar2=def#baz"),
                  EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                              "host:99/foo?bar1&bar2=def#baz"))));
}

TEST(UrlTokenizerTest, UrlWithAllTokenTypes) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s(
      "http://user:pass@www.google.com:99/path/"
      "sub_path?param1=abc&param2=def#ref");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_USERNAME, "user"),
          EqualsToken(Token::Type::URL_PASSWORD, "pass"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "www"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "com"),
          EqualsToken(Token::Type::URL_PORT, "99"),
          EqualsToken(Token::Type::URL_PATH_PART, "path"),
          EqualsToken(Token::Type::URL_PATH_PART, "sub_path"),
          EqualsToken(Token::Type::URL_QUERY, "param1=abc&param2=def"),
          EqualsToken(Token::Type::URL_REF, "ref"),
          EqualsToken(Token::Type::URL_SUFFIX,
                      "http://user:pass@www.google.com:99/path/"
                      "sub_path?param1=abc&param2=def#ref"),
          EqualsToken(
              Token::Type::URL_SUFFIX,
              "www.google.com:99/path/sub_path?param1=abc&param2=def#ref"),
          EqualsToken(
              Token::Type::URL_SUFFIX_INNERMOST,
              "google.com:99/path/sub_path?param1=abc&param2=def#ref"))));
}

TEST(UrlTokenizerTest, ComponentsWithSpecialDelimiters) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://foo/path;a?asd?e#f#g");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "foo"),
          EqualsToken(Token::Type::URL_PATH_PART, "path;a"),
          EqualsToken(Token::Type::URL_QUERY, "asd?e"),
          EqualsToken(Token::Type::URL_REF, "f#g"),
          EqualsToken(Token::Type::URL_SUFFIX, "http://foo/path;a?asd?e#f#g"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST,
                      "foo/path;a?asd?e#f#g"))));
}

TEST(UrlTokenizerTest, AnyTopLevelDomainIsCommon) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://www.google.se/");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "www"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "se"),
          EqualsToken(Token::Type::URL_SUFFIX, "http://www.google.se/"),
          EqualsToken(Token::Type::URL_SUFFIX, "www.google.se/"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "google.se/"))));
}

TEST(UrlTokenizerTest, RareCommonSubdomain) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view s("http://m.google.nu/");

  EXPECT_THAT(
      url_tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(
          EqualsToken(Token::Type::URL_SCHEME, "http"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "m"),
          EqualsToken(Token::Type::URL_HOST_SIGNIFICANT_PART, "google"),
          EqualsToken(Token::Type::URL_HOST_COMMON_PART, "nu"),
          EqualsToken(Token::Type::URL_SUFFIX, "http://m.google.nu/"),
          EqualsToken(Token::Type::URL_SUFFIX, "m.google.nu/"),
          EqualsToken(Token::Type::URL_SUFFIX_INNERMOST, "google.nu/"))));
}

TEST(UrlTokenizerTest, InvalidUrls) {
  UrlTokenizer url_tokenizer = UrlTokenizer();

  std::string_view invalid_url("invalid_url.I");
  std::string_view empty_host("http://");
  std::string_view invalid_scheme("sch://www");
  std::string_view empty_scheme("://www");

  EXPECT_THAT(url_tokenizer.TokenizeAll(invalid_url), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(url_tokenizer.TokenizeAll(empty_host), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(url_tokenizer.TokenizeAll(invalid_scheme), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(url_tokenizer.TokenizeAll(empty_scheme), IsOkAndHolds(IsEmpty()));
}

}  // namespace
}  // namespace lib
}  // namespace icing
