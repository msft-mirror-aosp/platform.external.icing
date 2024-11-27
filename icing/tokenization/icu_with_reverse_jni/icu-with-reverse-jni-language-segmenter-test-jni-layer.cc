// Copyright (C) 2024 Google LLC
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

#include <jni.h>
#include <cstring>
#include <vector>

#include "gtest/gtest.h"
#include "icing/testing/logging-event-listener.h"
#include "icing/jni/scoped-utf-chars.h"

// Global variable used so that the test implementation can access the JNIEnv.
JNIEnv* g_jenv = nullptr;

// Global variable used so that the test implementation can access the absolute
// path of the ICU data file.
char* g_icu_data_file_path = nullptr;

extern "C" JNIEXPORT jboolean JNICALL
Java_icing_jni_IcuWithReverseJniLanguageSegmenterJniTest_testsMain(
    JNIEnv* env, jclass ignored, jstring icu_data_file_path) {
  g_jenv = env;

  // Copy the ICU data file path from a Java String to a C++ char*.
  icing::lib::ScopedUtfChars scoped_chars(env, icu_data_file_path);
  g_icu_data_file_path = new char[scoped_chars.size() + 1];
  std::strncpy(g_icu_data_file_path, scoped_chars.c_str(), scoped_chars.size());

  std::vector<char*> my_argv;
  char arg[] = "jni-test-lib";
  my_argv.push_back(arg);
  int argc = 1;
  char** argv = &(my_argv[0]);
  testing::InitGoogleTest(&argc, argv);
  testing::UnitTest::GetInstance()->listeners().Append(
      new icing::lib::LoggingEventListener());
  return RUN_ALL_TESTS() == 0;
}
