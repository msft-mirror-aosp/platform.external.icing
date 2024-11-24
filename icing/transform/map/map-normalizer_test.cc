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

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/icu-i18n-test-utils.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "icing/util/character-iterator.h"

namespace icing {
namespace lib {

namespace {
using ::testing::Eq;

TEST(MapNormalizerTest, Creation) {
  EXPECT_THAT(normalizer_factory::Create(
                  /*max_term_byte_size=*/5),
              IsOk());
  EXPECT_THAT(normalizer_factory::Create(
                  /*max_term_byte_size=*/0),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(normalizer_factory::Create(
                  /*max_term_byte_size=*/-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

// Strings that are already normalized won't change if normalized again.
TEST(MapNormalizerTest, AlreadyNormalized) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  EXPECT_THAT(normalizer->NormalizeTerm(""), EqualsNormalizedTerm(""));
  EXPECT_THAT(normalizer->NormalizeTerm("hello world"),
              EqualsNormalizedTerm("hello world"));
  EXPECT_THAT(normalizer->NormalizeTerm("你好"), EqualsNormalizedTerm("你好"));
  EXPECT_THAT(normalizer->NormalizeTerm("キャンパス"),
              EqualsNormalizedTerm("キャンパス"));
  EXPECT_THAT(normalizer->NormalizeTerm("안녕하세요"),
              EqualsNormalizedTerm("안녕하세요"));
}

TEST(MapNormalizerTest, UppercaseToLowercase) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  EXPECT_THAT(normalizer->NormalizeTerm("MDI"), EqualsNormalizedTerm("mdi"));
  EXPECT_THAT(normalizer->NormalizeTerm("Icing"),
              EqualsNormalizedTerm("icing"));
}

TEST(MapNormalizerTest, LatinLetterRemoveAccent) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  EXPECT_THAT(normalizer->NormalizeTerm("Zürich"),
              EqualsNormalizedTerm("zurich"));
  EXPECT_THAT(normalizer->NormalizeTerm("après-midi"),
              EqualsNormalizedTerm("apres-midi"));
  EXPECT_THAT(normalizer->NormalizeTerm("Buenos días"),
              EqualsNormalizedTerm("buenos dias"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÀÁÂÃÄÅĀĂĄḀḁàáâãäåāăą"),
              EqualsNormalizedTerm("aaaaaaaaaaaaaaaaaaaa"));
  EXPECT_THAT(normalizer->NormalizeTerm("ḂḄḆḃḅḇ"),
              EqualsNormalizedTerm("bbbbbb"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÇĆĈĊČḈḉćĉċčç"),
              EqualsNormalizedTerm("cccccccccccc"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÐĎĐḊḌḎḐḒḋḍḏḑḓďđ"),
              EqualsNormalizedTerm("ddddddddddddddd"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÈÉÊËĒĔĖĘḔḖḘḚḜḕḗḙḛḝèéêëēĕėęě"),
              EqualsNormalizedTerm("eeeeeeeeeeeeeeeeeeeeeeeeeee"));
  EXPECT_THAT(normalizer->NormalizeTerm("Ḟḟ"), EqualsNormalizedTerm("ff"));
  EXPECT_THAT(normalizer->NormalizeTerm("ĜĞĠĢḠḡĝğġģ"),
              EqualsNormalizedTerm("gggggggggg"));
  EXPECT_THAT(normalizer->NormalizeTerm("ĤḢḤḦḨḪḣḥḧḩḫĥẖ"),
              EqualsNormalizedTerm("hhhhhhhhhhhhh"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÌÍÎÏĨĪĬḬḭḯìíîïĩīĭ"),
              EqualsNormalizedTerm("iiiiiiiiiiiiiiiii"));
  EXPECT_THAT(normalizer->NormalizeTerm("Ĵĵ"), EqualsNormalizedTerm("jj"));
  EXPECT_THAT(normalizer->NormalizeTerm("ĶḰḲḴḵḱḳķ"),
              EqualsNormalizedTerm("kkkkkkkk"));
  EXPECT_THAT(normalizer->NormalizeTerm("ĹĻĽḶḸḼḷḹḻḽĺļľ"),
              EqualsNormalizedTerm("lllllllllllll"));
  EXPECT_THAT(normalizer->NormalizeTerm("ḾṀṂḿṁṃ"),
              EqualsNormalizedTerm("mmmmmm"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÑŃŅŇṄṆṈṊṅṇṉṋñńņň"),
              EqualsNormalizedTerm("nnnnnnnnnnnnnnnn"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŌŎŐÒÓÔÕÖṌṎṐṒṍṏṑṓòóôõöōŏő"),
              EqualsNormalizedTerm("oooooooooooooooooooooooo"));
  EXPECT_THAT(normalizer->NormalizeTerm("ṔṖṕṗ"), EqualsNormalizedTerm("pppp"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŔŖŘṘṚṜṞṙṛṝṟŕŗř"),
              EqualsNormalizedTerm("rrrrrrrrrrrrrr"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŚŜŞŠȘṠṢṤṦṨṡṣṥṧṩșśŝşš"),
              EqualsNormalizedTerm("ssssssssssssssssssss"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŢŤȚṪṬṮṰṫṭṯṱțţť"),
              EqualsNormalizedTerm("tttttttttttttt"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŨŪŬÙÚÛÜṲṴṶṸṺṳṵṷṹṻùúûüũūŭ"),
              EqualsNormalizedTerm("uuuuuuuuuuuuuuuuuuuuuuuu"));
  EXPECT_THAT(normalizer->NormalizeTerm("ṼṾṽṿ"), EqualsNormalizedTerm("vvvv"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŴẀẂẄẆẈẁẃẅẇẉŵ"),
              EqualsNormalizedTerm("wwwwwwwwwwww"));
  EXPECT_THAT(normalizer->NormalizeTerm("ẊẌẋẍ"), EqualsNormalizedTerm("xxxx"));
  EXPECT_THAT(normalizer->NormalizeTerm("ÝŶŸẎẏŷýÿ"),
              EqualsNormalizedTerm("yyyyyyyy"));
  EXPECT_THAT(normalizer->NormalizeTerm("ŹŻŽẐẒẔẑẓẕźżž"),
              EqualsNormalizedTerm("zzzzzzzzzzzz"));
}

// Accent / diacritic marks won't be removed in non-latin chars, e.g. in
// Japanese and Greek
TEST(MapNormalizerTest, NonLatinLetterNotRemoveAccent) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  // Katakana
  EXPECT_THAT(normalizer->NormalizeTerm("ダヂヅデド"),
              EqualsNormalizedTerm("ダヂヅデド"));
  // Greek
  EXPECT_THAT(normalizer->NormalizeTerm("kαλημέρα"),
              EqualsNormalizedTerm("kαλημέρα"));
  EXPECT_THAT(normalizer->NormalizeTerm("εγγραφή"),
              EqualsNormalizedTerm("εγγραφή"));
  // Hebrew
  EXPECT_THAT(normalizer->NormalizeTerm("אָלֶף־בֵּית עִבְרִי"),
              EqualsNormalizedTerm("אָלֶף־בֵּית עִבְרִי"));
}

TEST(MapNormalizerTest, FullWidthCharsToASCII) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  // Full-width punctuation to ASCII punctuation
  EXPECT_THAT(normalizer->NormalizeTerm("‘’．，！？：“”"),
              EqualsNormalizedTerm("''.,!?:\"\""));
  // Full-width 0-9
  EXPECT_THAT(normalizer->NormalizeTerm("０１２３４５６７８９"),
              EqualsNormalizedTerm("0123456789"));
  // Full-width A-Z
  EXPECT_THAT(normalizer->NormalizeTerm(
                  "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"),
              EqualsNormalizedTerm("abcdefghijklmnopqrstuvwxyz"));
  // Full-width a-z
  EXPECT_THAT(normalizer->NormalizeTerm(
                  "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"),
              EqualsNormalizedTerm("abcdefghijklmnopqrstuvwxyz"));
}

TEST(MapNormalizerTest, IdeographicToASCII) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  EXPECT_THAT(normalizer->NormalizeTerm("，。"), EqualsNormalizedTerm(",."));
}

TEST(MapNormalizerTest, HiraganaToKatakana) {
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  EXPECT_THAT(normalizer->NormalizeTerm("あいうえお"),
              EqualsNormalizedTerm("アイウエオ"));
  EXPECT_THAT(normalizer->NormalizeTerm("かきくけこ"),
              EqualsNormalizedTerm("カキクケコ"));
  EXPECT_THAT(normalizer->NormalizeTerm("さしすせそ"),
              EqualsNormalizedTerm("サシスセソ"));
  EXPECT_THAT(normalizer->NormalizeTerm("たちつてと"),
              EqualsNormalizedTerm("タチツテト"));
  EXPECT_THAT(normalizer->NormalizeTerm("なにぬねの"),
              EqualsNormalizedTerm("ナニヌネノ"));
  EXPECT_THAT(normalizer->NormalizeTerm("はひふへほ"),
              EqualsNormalizedTerm("ハヒフヘホ"));
  EXPECT_THAT(normalizer->NormalizeTerm("まみむめも"),
              EqualsNormalizedTerm("マミムメモ"));
  EXPECT_THAT(normalizer->NormalizeTerm("やゆよ"),
              EqualsNormalizedTerm("ヤユヨ"));
  EXPECT_THAT(normalizer->NormalizeTerm("らりるれろ"),
              EqualsNormalizedTerm("ラリルレロ"));
  EXPECT_THAT(normalizer->NormalizeTerm("わゐゑを"),
              EqualsNormalizedTerm("ワヰヱヲ"));
  EXPECT_THAT(normalizer->NormalizeTerm("ん"), EqualsNormalizedTerm("ン"));
  EXPECT_THAT(normalizer->NormalizeTerm("がぎぐげご"),
              EqualsNormalizedTerm("ガギグゲゴ"));
  EXPECT_THAT(normalizer->NormalizeTerm("ざじずぜぞ"),
              EqualsNormalizedTerm("ザジズゼゾ"));
  EXPECT_THAT(normalizer->NormalizeTerm("だぢづでど"),
              EqualsNormalizedTerm("ダヂヅデド"));
  EXPECT_THAT(normalizer->NormalizeTerm("ばびぶべぼ"),
              EqualsNormalizedTerm("バビブベボ"));
  EXPECT_THAT(normalizer->NormalizeTerm("ぱぴぷぺぽ"),
              EqualsNormalizedTerm("パピプペポ"));
}

TEST(MapNormalizerTest, Truncate) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                    /*max_term_byte_size=*/5));

    // Won't be truncated
    EXPECT_THAT(normalizer->NormalizeTerm("hi"), EqualsNormalizedTerm("hi"));
    EXPECT_THAT(normalizer->NormalizeTerm("hello"),
                EqualsNormalizedTerm("hello"));

    // Truncated to length 5.
    EXPECT_THAT(normalizer->NormalizeTerm("hello!"),
                EqualsNormalizedTerm("hello"));

    // Each Japanese character has 3 bytes, so truncating to length 5 results in
    // only 1 character.
    EXPECT_THAT(normalizer->NormalizeTerm("キャンパス"),
                EqualsNormalizedTerm("キ"));

    // Each Greek character has 2 bytes, so truncating to length 5 results in 2
    // character.
    EXPECT_THAT(normalizer->NormalizeTerm("αβγδε"), EqualsNormalizedTerm("αβ"));
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                    /*max_term_byte_size=*/2));
    // The Japanese character has 3 bytes, truncating it results in an empty
    // string.
    EXPECT_THAT(normalizer->NormalizeTerm("キ"), EqualsNormalizedTerm(""));
  }
}

TEST(MapNormalizerTest, PrefixMatchLength) {
  // Verify that FindNormalizedMatchEndPosition will properly find the length of
  // the prefix match when given a non-normalized term and a normalized term
  // is a prefix of the non-normalized one.
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  // Upper to lower
  std::string term = "MDI";
  CharacterIterator match_end =
      normalizer->FindNormalizedMatchEndPosition(term, "md");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("MD"));

  term = "Icing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "icin");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Icin"));

  // Full-width
  term = "５２５６００";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "525");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("５２５"));

  term = "ＦＵＬＬＷＩＤＴＨ";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "full");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("ＦＵＬＬ"));

  // Hiragana to Katakana
  term = "あいうえお";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "アイ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("あい"));

  term = "かきくけこ";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "カ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("か"));

  // Latin accents
  term = "Zürich";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "zur");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Zür"));

  term = "après-midi";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "apre");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("aprè"));

  term = "Buenos días";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "buenos di");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Buenos dí"));
}

TEST(MapNormalizerTest, SharedPrefixMatchLength) {
  // Verify that FindNormalizedMatchEndPosition will properly find the length of
  // the prefix match when given a non-normalized term and a normalized term
  // that share a common prefix.
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
                                                  /*max_term_byte_size=*/1000));

  // Upper to lower
  std::string term = "MDI";
  CharacterIterator match_end =
      normalizer->FindNormalizedMatchEndPosition(term, "mgm");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("M"));

  term = "Icing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "icky");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Ic"));

  // Full-width
  term = "５２５６００";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "525788");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("５２５"));

  term = "ＦＵＬＬＷＩＤＴＨ";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "fully");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("ＦＵＬＬ"));

  // Hiragana to Katakana
  term = "あいうえお";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "アイエオ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("あい"));

  term = "かきくけこ";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "カケコ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("か"));

  // Latin accents
  term = "Zürich";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "zurg");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Zür"));

  term = "après-midi";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "apreciate");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("aprè"));

  term = "días";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "diamond");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("día"));
}

}  // namespace

}  // namespace lib
}  // namespace icing
