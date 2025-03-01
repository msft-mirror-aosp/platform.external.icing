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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-data.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer-options.h"
#include "icing/transform/normalizer.h"
#include "icing/util/icu-data-file-helper.h"

namespace icing {
namespace lib {
namespace {
using ::testing::Eq;

class IcuNormalizerTest : public testing::Test {
 protected:
  void SetUp() override {
    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        icu_data_file_helper::SetUpIcuDataFile(
            GetTestFilePath("icing/icu.dat")));

    NormalizerOptions options(/*max_term_byte_size=*/1024);
    ICING_ASSERT_OK_AND_ASSIGN(normalizer_,
                               normalizer_factory::Create(options));
  }

  std::unique_ptr<Normalizer> normalizer_;
};

TEST_F(IcuNormalizerTest, Creation) {
  NormalizerOptions options1(/*max_term_byte_size=*/5);
  EXPECT_THAT(normalizer_factory::Create(options1), IsOk());

  NormalizerOptions options2(/*max_term_byte_size=*/0);
  EXPECT_THAT(normalizer_factory::Create(options2),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  NormalizerOptions options3(/*max_term_byte_size=*/-1);
  EXPECT_THAT(normalizer_factory::Create(options3),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

// Strings that are already normalized won't change if normalized again.
TEST_F(IcuNormalizerTest, AlreadyNormalized) {
  EXPECT_THAT(normalizer_->NormalizeTerm(""), EqualsNormalizedTerm(""));
  EXPECT_THAT(normalizer_->NormalizeTerm("hello world"),
              EqualsNormalizedTerm("hello world"));
  EXPECT_THAT(normalizer_->NormalizeTerm("キャンパス"),
              EqualsNormalizedTerm("キャンパス"));
  EXPECT_THAT(normalizer_->NormalizeTerm("안녕하세요"),
              EqualsNormalizedTerm("안녕하세요"));
}

TEST_F(IcuNormalizerTest, UppercaseToLowercase) {
  EXPECT_THAT(normalizer_->NormalizeTerm("MDI"), EqualsNormalizedTerm("mdi"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Icing"),
              EqualsNormalizedTerm("icing"));
}

TEST_F(IcuNormalizerTest, LatinLetterRemoveAccent) {
  EXPECT_THAT(normalizer_->NormalizeTerm("Zürich"),
              EqualsNormalizedTerm("zurich"));
  EXPECT_THAT(normalizer_->NormalizeTerm("après-midi"),
              EqualsNormalizedTerm("apres-midi"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Buenos días"),
              EqualsNormalizedTerm("buenos dias"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÀÁÂÃÄÅĀĂĄḀḁàáâãäåāăą"),
              EqualsNormalizedTerm("aaaaaaaaaaaaaaaaaaaa"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ḂḄḆḃḅḇ"),
              EqualsNormalizedTerm("bbbbbb"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÇĆĈĊČḈḉćĉċčç"),
              EqualsNormalizedTerm("cccccccccccc"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÐĎĐḊḌḎḐḒḋḍḏḑḓďđ"),
              EqualsNormalizedTerm("ddddddddddddddd"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÈÉÊËĒĔĖĘḔḖḘḚḜḕḗḙḛḝèéêëēĕėęě"),
              EqualsNormalizedTerm("eeeeeeeeeeeeeeeeeeeeeeeeeee"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Ḟḟ"), EqualsNormalizedTerm("ff"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ĜĞĠĢḠḡĝğġģ"),
              EqualsNormalizedTerm("gggggggggg"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ĤḢḤḦḨḪḣḥḧḩḫĥẖ"),
              EqualsNormalizedTerm("hhhhhhhhhhhhh"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÌÍÎÏĨĪĬḬḭḯìíîïĩīĭ"),
              EqualsNormalizedTerm("iiiiiiiiiiiiiiiii"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Ĵĵ"), EqualsNormalizedTerm("jj"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ĶḰḲḴḵḱḳķ"),
              EqualsNormalizedTerm("kkkkkkkk"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ĹĻĽḶḸḼḷḹḻḽĺļľ"),
              EqualsNormalizedTerm("lllllllllllll"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ḾṀṂḿṁṃ"),
              EqualsNormalizedTerm("mmmmmm"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÑŃŅŇṄṆṈṊṅṇṉṋñńņň"),
              EqualsNormalizedTerm("nnnnnnnnnnnnnnnn"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŌŎŐÒÓÔÕÖṌṎṐṒṍṏṑṓòóôõöōŏő"),
              EqualsNormalizedTerm("oooooooooooooooooooooooo"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ṔṖṕṗ"), EqualsNormalizedTerm("pppp"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŔŖŘṘṚṜṞṙṛṝṟŕŗř"),
              EqualsNormalizedTerm("rrrrrrrrrrrrrr"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŚŜŞŠȘṠṢṤṦṨṡṣṥṧṩșśŝşš"),
              EqualsNormalizedTerm("ssssssssssssssssssss"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŢŤȚṪṬṮṰṫṭṯṱțţť"),
              EqualsNormalizedTerm("tttttttttttttt"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŨŪŬÙÚÛÜṲṴṶṸṺṳṵṷṹṻùúûüũūŭ"),
              EqualsNormalizedTerm("uuuuuuuuuuuuuuuuuuuuuuuu"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ṼṾṽṿ"), EqualsNormalizedTerm("vvvv"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŴẀẂẄẆẈẁẃẅẇẉŵ"),
              EqualsNormalizedTerm("wwwwwwwwwwww"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ẊẌẋẍ"), EqualsNormalizedTerm("xxxx"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ÝŶŸẎẏŷýÿ"),
              EqualsNormalizedTerm("yyyyyyyy"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ŹŻŽẐẒẔẑẓẕźżž"),
              EqualsNormalizedTerm("zzzzzzzzzzzz"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Barış"),
              EqualsNormalizedTerm("baris"));
}

TEST_F(IcuNormalizerTest, GreekLetterRemoveAccent) {
  EXPECT_THAT(normalizer_->NormalizeTerm("kαλημέρα"),
              EqualsNormalizedTerm("kαλημερα"));
  EXPECT_THAT(normalizer_->NormalizeTerm("εγγραφή"),
              EqualsNormalizedTerm("εγγραφη"));
  EXPECT_THAT(
      normalizer_->NormalizeTerm(
          "ἈἉἊἋἌἍἎἏᾈᾉᾊᾋᾌᾍᾎᾏᾸᾹᾺΆᾼἀἁἂἃἄἅἆἇὰάᾀᾁᾂᾃᾄᾅᾆᾇᾰᾱᾲᾳᾴᾶᾷ"),
      EqualsNormalizedTerm("αααααααααααααααααααααααααααααααααααααααααααααα"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ἘἙἚἛἜἝῈΈἐἑἒἓἔἕὲέ"),
              EqualsNormalizedTerm("εεεεεεεεεεεεεεεε"));
  EXPECT_THAT(
      normalizer_->NormalizeTerm("ἨἩἪἫἬἭἮἯᾘᾙᾚᾛᾜᾝᾞᾟῊΉῌἠἡἢἣἤἥἦἧὴήᾐᾑᾒᾓᾔᾕᾖᾗῂῃῄῆῇ"),
      EqualsNormalizedTerm("ηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηηη"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ἸἹἺἻἼἽἾἿῘῙῚΊἰἱἲἳἴἵἶἷὶίῐῑῒΐῖῗ"),
              EqualsNormalizedTerm("ιιιιιιιιιιιιιιιιιιιιιιιιιιιι"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ὈὉὊὋὌὍῸΌὀὁὂὃὄὅὸό"),
              EqualsNormalizedTerm("οοοοοοοοοοοοοοοο"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ὙὛὝὟῨῩῪΎὐὑὒὓὔὕὖὗὺύῠῡῢΰῦῧ"),
              EqualsNormalizedTerm("υυυυυυυυυυυυυυυυυυυυυυυυ"));
  EXPECT_THAT(
      normalizer_->NormalizeTerm("ὨὩὪὫὬὭὮὯᾨᾩᾪᾫᾬᾭᾮᾯῺΏῼὠὡὢὣὤὥὦὧὼώᾠᾡᾢᾣᾤᾥᾦᾧῲῳῴῶῷ"),
      EqualsNormalizedTerm("ωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωωω"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Ῥῤῥ"), EqualsNormalizedTerm("ρρρ"));
}

// Accent / diacritic marks won't be removed in non-latin chars, e.g. in
// Japanese
TEST_F(IcuNormalizerTest, NonLatinLetterNotRemoveAccent) {
  // Katakana
  EXPECT_THAT(normalizer_->NormalizeTerm("ダヂヅデド"),
              EqualsNormalizedTerm("ダヂヅデド"));

  // Our current ICU rules can't handle Hebrew properly, e.g. the accents in
  // "אָלֶף־בֵּית עִבְרִי"
  // will be removed.
}

TEST_F(IcuNormalizerTest, FullWidthCharsToASCII) {
  // Full-width punctuation to ASCII punctuation
  EXPECT_THAT(normalizer_->NormalizeTerm("‘’．，！？：“”"),
              EqualsNormalizedTerm("''.,!?:\"\""));
  // Full-width 0-9
  EXPECT_THAT(normalizer_->NormalizeTerm("０１２３４５６７８９"),
              EqualsNormalizedTerm("0123456789"));
  // Full-width A-Z
  EXPECT_THAT(normalizer_->NormalizeTerm(
                  "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"),
              EqualsNormalizedTerm("abcdefghijklmnopqrstuvwxyz"));
  // Full-width a-z
  EXPECT_THAT(normalizer_->NormalizeTerm(
                  "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"),
              EqualsNormalizedTerm("abcdefghijklmnopqrstuvwxyz"));
}

TEST_F(IcuNormalizerTest, IdeographicToASCII) {
  NormalizerOptions options(/*max_term_byte_size=*/1000);
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
      options));

  EXPECT_THAT(normalizer->NormalizeTerm("，。"), EqualsNormalizedTerm(",."));
}

// For Katakana, each character is normalized to its full-width version.
TEST_F(IcuNormalizerTest, KatakanaHalfWidthToFullWidth) {
  EXPECT_THAT(normalizer_->NormalizeTerm("ｶ"), EqualsNormalizedTerm("カ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ｫ"), EqualsNormalizedTerm("ォ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ｻ"), EqualsNormalizedTerm("サ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ﾎ"), EqualsNormalizedTerm("ホ"));
}

TEST_F(IcuNormalizerTest, HiraganaToKatakana) {
  EXPECT_THAT(normalizer_->NormalizeTerm("あいうえお"),
              EqualsNormalizedTerm("アイウエオ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("かきくけこ"),
              EqualsNormalizedTerm("カキクケコ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("さしすせそ"),
              EqualsNormalizedTerm("サシスセソ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("たちつてと"),
              EqualsNormalizedTerm("タチツテト"));
  EXPECT_THAT(normalizer_->NormalizeTerm("なにぬねの"),
              EqualsNormalizedTerm("ナニヌネノ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("はひふへほ"),
              EqualsNormalizedTerm("ハヒフヘホ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("まみむめも"),
              EqualsNormalizedTerm("マミムメモ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("やゆよ"),
              EqualsNormalizedTerm("ヤユヨ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("らりるれろ"),
              EqualsNormalizedTerm("ラリルレロ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("わゐゑを"),
              EqualsNormalizedTerm("ワヰヱヲ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ん"), EqualsNormalizedTerm("ン"));
  EXPECT_THAT(normalizer_->NormalizeTerm("がぎぐげご"),
              EqualsNormalizedTerm("ガギグゲゴ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ざじずぜぞ"),
              EqualsNormalizedTerm("ザジズゼゾ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("だぢづでど"),
              EqualsNormalizedTerm("ダヂヅデド"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ばびぶべぼ"),
              EqualsNormalizedTerm("バビブベボ"));
  EXPECT_THAT(normalizer_->NormalizeTerm("ぱぴぷぺぽ"),
              EqualsNormalizedTerm("パピプペポ"));
}

TEST_F(IcuNormalizerTest, HanToPinyin) {
  EXPECT_THAT(normalizer_->NormalizeTerm("戴"), EqualsNormalizedTerm("戴"));
  EXPECT_THAT(normalizer_->NormalizeTerm("慧"), EqualsNormalizedTerm("慧"));
}

TEST_F(IcuNormalizerTest, SuperscriptAndSubscriptToASCII) {
  EXPECT_THAT(normalizer_->NormalizeTerm("⁹"), EqualsNormalizedTerm("9"));
  EXPECT_THAT(normalizer_->NormalizeTerm("₉"), EqualsNormalizedTerm("9"));
}

TEST_F(IcuNormalizerTest, CircledCharsToASCII) {
  EXPECT_THAT(normalizer_->NormalizeTerm("①"), EqualsNormalizedTerm("1"));
  EXPECT_THAT(normalizer_->NormalizeTerm("Ⓐ"), EqualsNormalizedTerm("a"));
}

TEST_F(IcuNormalizerTest, RotatedCharsToASCII) {
  EXPECT_THAT(normalizer_->NormalizeTerm("︷"), EqualsNormalizedTerm("{"));
  EXPECT_THAT(normalizer_->NormalizeTerm("︸"), EqualsNormalizedTerm("}"));
}

TEST_F(IcuNormalizerTest, SquaredCharsToASCII) {
  EXPECT_THAT(normalizer_->NormalizeTerm("㌀"),
              EqualsNormalizedTerm("アパート"));
}

TEST_F(IcuNormalizerTest, FractionsToASCII) {
  EXPECT_THAT(normalizer_->NormalizeTerm("¼"), EqualsNormalizedTerm(" 1/4"));
  EXPECT_THAT(normalizer_->NormalizeTerm("⅚"), EqualsNormalizedTerm(" 5/6"));
}

TEST_F(IcuNormalizerTest, CorruptTerm) {
  std::string_view empty_term = "";
  EXPECT_THAT(normalizer_->NormalizeTerm(empty_term), EqualsNormalizedTerm(""));
  std::string_view CGJ_term = "\u034F";
  EXPECT_THAT(normalizer_->NormalizeTerm(CGJ_term), EqualsNormalizedTerm(""));
  std::string_view null_term = "\0";
  EXPECT_THAT(normalizer_->NormalizeTerm(null_term), EqualsNormalizedTerm(""));
}

TEST_F(IcuNormalizerTest, Truncate) {
  {
    NormalizerOptions options(/*max_term_byte_size=*/5);
    ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
        options));

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
    NormalizerOptions options(/*max_term_byte_size=*/2);
    ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
        options));
    // The Japanese character has 3 bytes, truncating it results in an empty
    // string.
    EXPECT_THAT(normalizer->NormalizeTerm("キ"), EqualsNormalizedTerm(""));
  }
}

TEST_F(IcuNormalizerTest, PrefixMatchLength) {
  // Verify that FindNormalizedMatchEndPosition will properly find the length of
  // the prefix match when given a non-normalized term and a normalized term
  // is a prefix of the non-normalized one.
  NormalizerOptions options(/*max_term_byte_size=*/1000);
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
      options));

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

  term = "BarışIcing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "baris");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Barış"));

  term = "ÀĄḁáIcing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "aaaa");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("ÀĄḁá"));

  // Greek accents
  term = "άνθρωπος";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "ανθ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("άνθ"));

  term = "καλημέρα";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "καλημε");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("καλημέ"));

  term = "όχι";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "οχ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("όχ"));

  term = "πότε";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "ποτ");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("πότ"));

  term = "ἈἉἊἋIcing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "αααα");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("ἈἉἊἋ"));
}

TEST_F(IcuNormalizerTest, SharedPrefixMatchLength) {
  // Verify that FindNormalizedMatchEndPosition will properly find the length of
  // the prefix match when given a non-normalized term and a normalized term
  // that share a common prefix.
  NormalizerOptions options(/*max_term_byte_size=*/1000);
  ICING_ASSERT_OK_AND_ASSIGN(auto normalizer, normalizer_factory::Create(
      options));

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

  term = "BarışIcing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "barismdi");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("Barış"));

  // Greek accents
  term = "άνθρωπος";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "ανθν");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("άνθ"));

  term = "καλημέρα";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "καλημεος");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("καλημέ"));

  term = "όχι";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "οχκα");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("όχ"));

  term = "πότε";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "ποτρα");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("πότ"));

  term = "ἈἉἊἋIcing";
  match_end = normalizer->FindNormalizedMatchEndPosition(term, "ααααmdi");
  EXPECT_THAT(term.substr(0, match_end.utf8_index()), Eq("ἈἉἊἋ"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
