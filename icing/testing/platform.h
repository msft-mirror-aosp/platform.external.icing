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

#ifndef ICING_TESTING_PLATFORM_H_
#define ICING_TESTING_PLATFORM_H_

// This file is meant to hold util functions for tests that help the test
// determine which platform-specific configuration it may be running in.
namespace icing {
namespace lib {

// Returns true if the test was built with the CFStringTokenizer as the
// implementation of LanguageSegmenter.
inline bool IsCfStringTokenization() {
#if defined(__APPLE__) && !defined(ICING_IOS_ICU4C_SEGMENTATION)
  return true;
#endif  // defined(__APPLE__) && !defined(ICING_IOS_ICU4C_SEGMENTATION)
  return false;
}

inline bool IsReverseJniTokenization() {
#ifdef ICING_REVERSE_JNI_SEGMENTATION
  return true;
#endif  // ICING_REVERSE_JNI_SEGMENTATION
  return false;
}

// Whether the running test is an Android test.
inline bool IsAndroidPlatform() {
#if defined(__ANDROID__)
  return true;
#endif  // defined(__ANDROID__)
  return false;
}

// Whether the running test is an iOS test.
inline bool IsIosPlatform() {
#if defined(__APPLE__)
  return true;
#endif  // defined(__APPLE__)
  return false;
}

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_PLATFORM_H_
