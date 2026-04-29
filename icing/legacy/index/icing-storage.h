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

// Author: sbanacho@google.com (Scott Banachowski)
//         vmarko@google.com (Vladimir Marko)
//
// Interface class for disk-backed storage.

#ifndef ICING_LEGACY_INDEX_ICING_STORAGE_H_
#define ICING_LEGACY_INDEX_ICING_STORAGE_H_

#include <cstdint>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

// Abstract base class for interface.
class IIcingStorage {
 public:
  // Any resource that is not removed in the Close() function should
  // be removed in the child's destructor.
  virtual ~IIcingStorage() = default;

  // This is called to upgrade to a new version.
  // Returns true if the data store can be upgraded successfully.
  virtual bool UpgradeTo(int new_version) = 0;

  // This must be called before the object is usable.
  // Returns true if the storage is in a usable state.
  virtual libtextclassifier3::Status Init() = 0;

  // Attempts to init the given IIcingStorage. On failure, clears the underlying
  // data and tries again. Returns the failure status if the second init also
  // fails.
  static libtextclassifier3::Status InitWithRetry(IIcingStorage* file_in) {
    libtextclassifier3::Status status = file_in->Init();
    if (status.ok()) {
      return status;
    }
    ICING_LOG(WARNING) << "Init failed, clearing underlying data and retrying."
                       << status.error_message();
    if (!file_in->Remove()) {
      return absl_ports::InternalError(
          "Failed to remove underlying file after init failed");
    }
    return file_in->Init();
  }

  // Closes all files and system resources.
  // Init() must be called before the object is used again.
  virtual void Close() = 0;

  // Closes all system resources, then removes the backing file.
  // Init() is required before the object is used again.
  // Returns true on success.
  virtual bool Remove() = 0;

  // Syncs any unwritten data to disk.
  virtual libtextclassifier3::Status Sync() = 0;

  // Gets the total amount of disk usage for the object (i.e. the sum of the
  // bytes of all underlying files).
  // Note: reported values are estimated via the number of blocks the file takes
  // up on disk. Sparse files are reported as their physical disk usage, as
  // opposed to the logical size when read.
  // Returns kBadFileSize on error.
  virtual uint64_t GetDiskUsage() const = 0;

  // Updates any checksums that this storage maintains.
  // By default, does nothing.
  virtual Crc32 UpdateCrc() { return Crc32(); }

  virtual void GetDebugInfo(int verbosity, std::string* out) const = 0;

 protected:
  IIcingStorage() = default;

 private:
  // Document stores are non-copyable.
  IIcingStorage(const IIcingStorage&);
  IIcingStorage& operator=(const IIcingStorage&);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_LEGACY_INDEX_ICING_STORAGE_H_
