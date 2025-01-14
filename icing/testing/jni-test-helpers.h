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

#ifndef ICING_TESTING_JNI_TEST_HELPERS_H_
#define ICING_TESTING_JNI_TEST_HELPERS_H_

#include <memory>

#include "icing/jni/jni-cache.h"

#ifdef ICING_REVERSE_JNI_SEGMENTATION

#include <jni.h>

extern JNIEnv* g_jenv;
extern char* g_icu_data_file_path;

#define ICING_TEST_JNI_CACHE JniCache::Create(g_jenv).ValueOrDie()
#define ICING_TEST_ICU_DATA_FILE_PATH g_icu_data_file_path

#else

#define ICING_TEST_JNI_CACHE nullptr
#define ICING_TEST_ICU_DATA_FILE_PATH nullptr

#endif  // ICING_REVERSE_JNI_SEGMENTATION

namespace icing {
namespace lib {

inline std::unique_ptr<JniCache> GetTestJniCache() {
  return ICING_TEST_JNI_CACHE;
}

inline char* GetTestIcuDataFilePath() { return ICING_TEST_ICU_DATA_FILE_PATH; }

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_JNI_TEST_HELPERS_H_
