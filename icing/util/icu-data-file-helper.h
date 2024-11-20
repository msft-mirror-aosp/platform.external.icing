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

#ifndef ICING_UTIL_ICU_DATA_FILE_HELPER
#define ICING_UTIL_ICU_DATA_FILE_HELPER

#include "icing/text_classifier/lib3/utils/base/status.h"

namespace icing {
namespace lib {

namespace icu_data_file_helper {

// Initializes ICU using the specified absolute path to the ICU data file.
// The data file can either be downloaded via MDD or generated at compile time.
//
// NOTE: This target does NOT contain the ICU .dat file. To use this helper
// function, the calling target must include a data dependency on the .dat file
// and provide a valid path to that file.
//
// Returns:
//   Ok on success
//   INTERNAL_ERROR if failed on any errors
libtextclassifier3::Status SetUpIcuDataFile(
    const std::string& icu_data_file_absolute_path);

}  // namespace icu_data_file_helper

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_ICU_DATA_FILE_HELPER