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

#include "icing/legacy/index/icing-storage-file.h"

#include <unistd.h>

#include <cinttypes>
#include <cstdint>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/legacy/core/icing-timer.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/legacy/index/icing-storage.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

IcingStorageFile::IcingStorageFile(const std::string &filename,
                                   const IcingFilesystem *filesystem)
    : IIcingStorage(), filesystem_(filesystem), filename_(filename) {}

libtextclassifier3::Status IcingStorageFile::Init() {
  if (!is_initialized_) {
    // Ensure the storage directory exists
    std::string storage_dir = filesystem_->GetDirname(filename_.c_str());
    if (!filesystem_->CreateDirectoryRecursively(storage_dir.c_str())) {
      return absl_ports::InternalError("Failed to create storage directory");
    }

    is_initialized_ = OnInit();
    if (!is_initialized_) {
      return absl_ports::InternalError("Failed to initialize storage file");
    }

    if (fd_.get() < 0) {  // if initialized, fd better be set
      return absl_ports::InternalError(
          "Storage file descriptor not set after initialization");
    }
  }
  return libtextclassifier3::Status::OK;
}

void IcingStorageFile::Close() {
  if (is_initialized_) {
    OnClose();
    fd_.reset();
    is_initialized_ = false;
  }
}

bool IcingStorageFile::Remove() {
  Close();
  return filesystem_->DeleteFile(filename_.c_str());
}

libtextclassifier3::Status IcingStorageFile::Sync() {
  if (!is_initialized_) {
    return absl_ports::InternalError("Storage file not initialized");
  }

  IcingTimer timer;
  if (!PreSync()) {
    return absl_ports::InternalError("Pre-sync failed");
  }
  if (!filesystem_->DataSync(fd_.get())) {
    return absl_ports::InternalError("Sync failed");
  }
  if (!PostSync()) {
    return absl_ports::InternalError("Post-sync failed");
  }
  ICING_VLOG(1) << "Syncing " << filename_ << " took " << timer.Elapsed() * 1000
                << "ms";
  return libtextclassifier3::Status::OK;
}

uint64_t IcingStorageFile::GetDiskUsage() const {
  return filesystem_->GetDiskUsage(fd_.get());
}

bool IcingStorageFile::PreSync() {
  // Default implementation is a no-op.
  return true;
}

bool IcingStorageFile::PostSync() {
  // Default implementation is a no-op.
  return true;
}

void IcingStorageFile::GetDebugInfo(int verbosity, std::string *out) const {
  if (!is_initialized_) {
    ICING_LOG(FATAL) << "Storage file not initialized";
  }

  if (verbosity >= 0) {  // Always
    uint64_t size = filesystem_->GetFileSize(fd_.get());
    IcingStringUtil::SStringAppendF(
        out, 1000, "Filename: %s Size: %" PRIu64 "\n", filename_.c_str(), size);
  }
}

}  // namespace lib
}  // namespace icing
