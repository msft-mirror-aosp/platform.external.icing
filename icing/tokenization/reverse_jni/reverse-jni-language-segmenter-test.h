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

#ifndef ICING_TOKENIZATION_REVERSE_JNI_REVERSE_JNI_LANGUAGE_SEGMENTER_TEST_H_
#define ICING_TOKENIZATION_REVERSE_JNI_REVERSE_JNI_LANGUAGE_SEGMENTER_TEST_H_

#include <jni.h>

#include "icing/jni/jni-cache.h"
#include "gtest/gtest.h"

extern JNIEnv* g_jenv;

namespace icing {
namespace lib {

namespace test_internal {

class ReverseJniLanguageSegmenterTest
    : public testing::TestWithParam<const char*> {
 protected:
  ReverseJniLanguageSegmenterTest()
      : jni_cache_(std::move(JniCache::Create(g_jenv)).ValueOrDie()) {}

  static std::string GetLocale() { return GetParam(); }

  std::unique_ptr<JniCache> jni_cache_;
};

}  // namespace test_internal

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_REVERSE_JNI_REVERSE_JNI_LANGUAGE_SEGMENTER_TEST_H_
