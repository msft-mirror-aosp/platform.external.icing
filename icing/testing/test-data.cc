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

#include "icing/testing/test-data.h"

#include <sys/mman.h>

#include <cstdint>

#include "devtools/build/runtime/get_runfiles_dir.h"
#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "unicode/udata.h"
#include "unicode/utypes.h"

namespace icing {
namespace lib {

std::string GetTestFilePath(const std::string& google3_relative_file_path) {
  return absl_ports::StrCat(devtools_build::testonly::GetTestSrcdir(),
                            "/google3/", google3_relative_file_path);
}

// ICU data file needs to be set up only once every test run, it can be shared
// between different test cases. Setting up the file too many times may cause
// out-of-memory / segmentation fault errors in Android emulator.
bool has_set_up_icu_data_file = false;

libtextclassifier3::Status SetUpICUDataFile(
    const std::string& icu_data_file_relative_path) {
  if (has_set_up_icu_data_file) {
    return libtextclassifier3::Status::OK;
  }

  const std::string& file_path = GetTestFilePath(icu_data_file_relative_path);

  Filesystem filesystem;
  int64_t file_size = filesystem.GetFileSize(file_path.c_str());
  ScopedFd fd(filesystem.OpenForRead(file_path.c_str()));

  // TODO(samzheng): figure out why icing::MemoryMappedFile causes
  // segmentation fault here.
  const void* data =
      mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd.get(), 0);

  UErrorCode status = U_ZERO_ERROR;
  udata_setCommonData(data, &status);

  if (U_FAILURE(status)) {
    return absl_ports::InternalError(
        "Failed to set up ICU data, please check if you have the data file at "
        "the given path.");
  }

  has_set_up_icu_data_file = true;

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
