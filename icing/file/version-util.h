// Copyright (C) 2023 Google LLC
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

#ifndef ICING_FILE_VERSION_UTIL_H_
#define ICING_FILE_VERSION_UTIL_H_

#include <cstdint>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"

namespace icing {
namespace lib {

namespace version_util {

// - Version 0: Android T base. Can be identified only by flash index magic.
// - Version 1: Android U base and M-2023-08.
// - Version 2: M-2023-09, M-2023-11, M-2024-01. Schema is compatible with v1.
//   (There were no M-2023-10, M-2023-12).
// - Version 3: M-2024-02. Schema is compatible with v1 and v2.
//
// LINT.IfChange(kVersion)
inline static constexpr int32_t kVersion = 3;
// LINT.ThenChange(//depot/google3/icing/schema/schema-store.cc:min_overlay_version_compatibility)
inline static constexpr int32_t kVersionOne = 1;
inline static constexpr int32_t kVersionTwo = 2;
inline static constexpr int32_t kVersionThree = 3;

inline static constexpr int kVersionZeroFlashIndexMagic = 0x6dfba6ae;

struct VersionInfo {
  int32_t version;
  int32_t max_version;

  explicit VersionInfo(int32_t version_in, int32_t max_version_in)
      : version(version_in), max_version(max_version_in) {}

  bool IsValid() const { return version >= 0 && max_version >= 0; }

  bool operator==(const VersionInfo& other) const {
    return version == other.version && max_version == other.max_version;
  }
} __attribute__((packed));
static_assert(sizeof(VersionInfo) == 8, "");

enum class StateChange {
  kUndetermined,
  kCompatible,
  kRollForward,
  kRollBack,
  kUpgrade,
  kVersionZeroUpgrade,
  kVersionZeroRollForward,
};

// Helper method to read version info (using version file and flash index header
// magic) from the existing data. If the state is invalid (e.g. flash index
// header file is missing), then return an invalid VersionInfo.
//
// RETURNS:
//   - Existing data's VersionInfo on success
//   - INTERNAL_ERROR on I/O errors
libtextclassifier3::StatusOr<VersionInfo> ReadVersion(
    const Filesystem& filesystem, const std::string& version_file_path,
    const std::string& index_base_dir);

// Helper method to write version file.
//
// RETURNS:
//   - OK on success
//   - INTERNAL_ERROR on I/O errors
libtextclassifier3::Status WriteVersion(const Filesystem& filesystem,
                                        const std::string& version_file_path,
                                        const VersionInfo& version_info);

// Helper method to determine the change state between the existing data version
// and the current code version.
//
// REQUIRES: curr_version > 0. We implement version checking in version 1, so
//   the callers (except unit tests) will always use a version # greater than 0.
//
// RETURNS: StateChange
StateChange GetVersionStateChange(const VersionInfo& existing_version_info,
                                  int32_t curr_version = kVersion);

// Helper method to determine whether Icing should rebuild all derived files.
// Sometimes it is not required to rebuild derived files when
// roll-forward/upgrading. This function "encodes" upgrade paths and checks if
// the roll-forward/upgrading requires derived files to be rebuilt or not.
//
// REQUIRES: curr_version > 0. We implement version checking in version 1, so
//   the callers (except unit tests) will always use a version # greater than 0.
bool ShouldRebuildDerivedFiles(const VersionInfo& existing_version_info,
                               int32_t curr_version = kVersion);

}  // namespace version_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_VERSION_UTIL_H_
