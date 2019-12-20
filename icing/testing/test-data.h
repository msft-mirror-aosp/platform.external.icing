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

#ifndef ICING_TESTING_TEST_DATA_H_
#define ICING_TESTING_TEST_DATA_H_

#include <string>

#include "utils/base/status.h"

// This file provides functions for getting / setting up absolute test file
// paths. They are specific to Blaze and Google3 and should be changed when used
// in AOSP / Gerrit.
namespace icing {
namespace lib {

// The input path should be a relative path under google3. The function returns
// an absolute path to the file during unit testing. Before calling this
// function, please make sure the test file is added in "data" attribute in
// portable_cc_test or any other test build rules.
std::string GetTestFilePath(const std::string& google3_relative_file_path);

// Returns the latest LangId model in Google3.
std::string GetLangIdModelPath();

// This is for unit testing in Google3. The library binary doesn't contain any
// ICU data files, so we generate a .dat file at compile time and here make ICU
// use that file.
//
// Returns:
//   Ok on success
//   INTERNAL_ERROR if failed on any errors
libtextclassifier3::Status SetUpICUDataFile(
    const std::string& icu_data_file_relative_path);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_TEST_DATA_H_
