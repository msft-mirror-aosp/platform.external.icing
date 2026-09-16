// Copyright (C) 2025 Google LLC
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

#ifndef ICING_FILE_DATABASE_STABLENESS_LOG_H_
#define ICING_FILE_DATABASE_STABLENESS_LOG_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
#include "icing/file/filesystem.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/persist.pb.h"

namespace icing {
namespace lib {

// A thread-safe class to update and read/write IcingDatabaseStablenessProto.
// See IcingDatabaseStablenessProto for more details about what information is
// logged and how it will be analyzed and used to help the developer.
class DatabaseStablenessLog {
 public:
  // Creates an DatabaseStablenessLog object.
  //
  // Returns:
  //   - On success, a DatabaseStablenessLog instance.
  //   - FAILED_PRECONDITION_ERROR if any of the pointer is null.
  //   - INTERNAL_ERROR on I/O errors.
  //   - Any FileBackedProto errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<DatabaseStablenessLog>>
  Create(const Filesystem* filesystem, std::string file_path);

  // Writes the API call history to the database stableness log. Overwrites the
  // existing history if the call type already exists.
  //
  // Note: PersistToDisk is not allowed to be updated by this method. Instead,
  //   use UpdatePersistToDiskHistory() below.
  //
  // Returns:
  //   - OK on success.
  //   - INVALID_ARGUMENT_ERROR if call_type is IcingApiCallType::Code::UNKNOWN
  //     or IcingApiCallType::Code::PERSIST_TO_DISK.
  //   - INTERNAL_ERROR on I/O errors.
  //   - Any FileBackedProto errors.
  libtextclassifier3::Status UpdateApiHistory(IcingApiCallType::Code call_type,
                                              int64_t timestamp_ms)
      ICING_LOCKS_EXCLUDED(mutex_);

  // Updates the history for PersistToDisk.
  //
  // Returns:
  //   - OK on success.
  //   - INTERNAL_ERROR on I/O errors.
  //   - INVALID_ARGUMENT_ERROR for invalid persist_type (e.g. UNKNOWN).
  //   - Any FileBackedProto errors.
  libtextclassifier3::Status UpdatePersistToDiskHistory(
      PersistType::Code persist_type, int64_t timestamp_ms)
      ICING_LOCKS_EXCLUDED(mutex_);

  // Returns a copy of the cached proto.
  IcingDatabaseStablenessProto GetCachedProto() const
      ICING_LOCKS_EXCLUDED(mutex_) {
    absl_ports::shared_lock lock(&mutex_);
    return cached_proto_;
  }

 private:
  explicit DatabaseStablenessLog(const Filesystem& filesystem,
                                 std::string&& file_path, ScopedFd&& sfd,
                                 IcingDatabaseStablenessProto&& cached_proto)
      : filesystem_(filesystem),
        file_path_(std::move(file_path)),
        sfd_(std::move(sfd)),
        cached_proto_(std::move(cached_proto)) {}

  const Filesystem& filesystem_;
  std::string file_path_;

  // Scoped file descriptor for the database stableness log file.
  //
  // Note: we don't use FileBackedProto here for several reasons:
  // - This file is not considered as a ground truth, so there is no need to
  //   handle checksum.
  // - FileBackedProto class does not cache the open file descriptor, so it will
  //   keep opening and closing the fd for every read and write operation, which
  //   is a bad pattern we would like to fix later.
  // - FileBackedProto wraps the proto with std::unique_ptr, which may introduce
  //   unnecessary allocation on heap memory.
  ScopedFd sfd_ ICING_GUARDED_BY(mutex_);

  IcingDatabaseStablenessProto cached_proto_ ICING_GUARDED_BY(mutex_);

  mutable absl_ports::shared_mutex mutex_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_DATABASE_STABLENESS_LOG_H_
