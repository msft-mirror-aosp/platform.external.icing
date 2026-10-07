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

#include "icing/legacy/index/icing-storage-collection.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/legacy/index/icing-storage.h"
#include "icing/util/crc32.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

IcingStorageCollection::~IcingStorageCollection() {
  // TODO(b/75960589): fix loop styling throughout
  for (size_t i = 0; i < files_.size(); ++i) {
    delete files_[i].file;
  }
}

void IcingStorageCollection::Add(IIcingStorage *file,
                                 bool remove_if_corrupted) {
  files_.push_back(FileInfo(file, remove_if_corrupted));
}

void IcingStorageCollection::Swap(const IIcingStorage *current_file,
                                  IIcingStorage *new_file) {
  for (size_t i = 0; i < files_.size(); ++i) {
    if (files_[i].file == current_file) {
      delete files_[i].file;
      files_[i] = FileInfo(new_file, files_[i].remove_if_corrupted);
    }
  }
}

bool IcingStorageCollection::UpgradeTo(int new_version) {
  size_t count = 0;
  for (size_t i = 0; i < files_.size(); ++i) {
    if (files_[i].file->UpgradeTo(new_version)) {
      ++count;
    }
  }
  return count == files_.size();
}

libtextclassifier3::Status IcingStorageCollection::Init() {
  for (size_t i = 0; i < files_.size(); ++i) {
    if (files_[i].remove_if_corrupted) {
      ICING_RETURN_IF_ERROR(IIcingStorage::InitWithRetry(files_[i].file));
    } else {
      ICING_RETURN_IF_ERROR(files_[i].file->Init());
    }
  }
  return libtextclassifier3::Status::OK;
}

void IcingStorageCollection::Close() {
  for (size_t i = 0; i < files_.size(); ++i) {
    files_[i].file->Close();
  }
}

bool IcingStorageCollection::Remove() {
  size_t count = 0;
  for (size_t i = 0; i < files_.size(); ++i) {
    if (files_[i].file->Remove()) {
      ++count;
    }
  }
  return count == files_.size();
}

libtextclassifier3::Status IcingStorageCollection::Sync() {
  for (size_t i = 0; i < files_.size(); ++i) {
    ICING_RETURN_IF_ERROR(files_[i].file->Sync());
  }
  return libtextclassifier3::Status::OK;
}

uint64_t IcingStorageCollection::GetDiskUsage() const {
  uint64_t total = 0;
  for (auto &file_info : files_) {
    IcingFilesystem::IncrementByOrSetInvalid(file_info.file->GetDiskUsage(),
                                             &total);
  }
  return total;
}

libtextclassifier3::StatusOr<Crc32> IcingStorageCollection::UpdateCrc() {
  Crc32 crc32;
  for (size_t i = 0; i < files_.size(); ++i) {
    ICING_ASSIGN_OR_RETURN(Crc32 this_crc, files_[i].file->UpdateCrc());
    crc32.Append(std::to_string(this_crc.Get()));
  }
  return crc32;
}

void IcingStorageCollection::GetDebugInfo(int verbosity,
                                          std::string *out) const {
  for (size_t i = 0; i < files_.size(); ++i) {
    files_[i].file->GetDebugInfo(verbosity, out);
  }
}

}  // namespace lib
}  // namespace icing
