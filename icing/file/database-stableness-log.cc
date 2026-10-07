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

#include "icing/file/database-stableness-log.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/file/filesystem.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/persist.pb.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

libtextclassifier3::StatusOr<IcingDatabaseStablenessProto> ReadProtoFromFile(
    const Filesystem& filesystem, const ScopedFd& sfd) {
  int64_t file_size = filesystem.GetFileSize(sfd.get());
  if (file_size == Filesystem::kBadFileSize) {
    return absl_ports::InternalError("Failed to get file size.");
  }

  if (file_size == 0) {
    return absl_ports::NotFoundError("File is empty.");
  }

  auto buffer = std::make_unique<uint8_t[]>(file_size);
  if (filesystem.PRead(sfd.get(), buffer.get(), file_size, /*offset=*/0) !=
      file_size) {
    return absl_ports::InternalError("Failed to read proto from file.");
  }

  IcingDatabaseStablenessProto proto;
  if (!proto.ParseFromArray(buffer.get(), static_cast<int>(file_size))) {
    return absl_ports::InternalError("Failed to parse proto");
  }
  return proto;
}

libtextclassifier3::Status WriteProtoIntoFile(
    const Filesystem& filesystem, const ScopedFd& sfd,
    const IcingDatabaseStablenessProto& new_proto) {
  if (!filesystem.Truncate(sfd.get(), 0)) {
    return absl_ports::InternalError(
        "Failed to truncate file before writing proto.");
  }

  std::string new_proto_str = new_proto.SerializeAsString();
  if (!filesystem.PWrite(sfd.get(), /*offset=*/0, new_proto_str.data(),
                         new_proto_str.size())) {
    return absl_ports::InternalError("Failed to write proto into the file.");
  }

  return libtextclassifier3::Status::OK;
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<DatabaseStablenessLog>>
DatabaseStablenessLog::Create(const Filesystem* filesystem,
                              std::string file_path) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);

  bool is_new = !filesystem->FileExists(file_path.c_str());
  ScopedFd sfd(filesystem->OpenForWrite(file_path.c_str()));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError("Failed to open file for write.");
  }

  IcingDatabaseStablenessProto proto;
  if (is_new) {
    // If new file, write a default proto to disk. This ensures the file is
    // created.
    ICING_RETURN_IF_ERROR(WriteProtoIntoFile(*filesystem, sfd, proto));
  } else {
    // Otherwise, attempt to read and parse the proto from the disk. Don't
    // return error if we fail to read or parse the proto. Instead, assign a
    // default proto and overwrite it into the disk.
    auto proto_or = ReadProtoFromFile(*filesystem, sfd);
    if (!proto_or.ok()) {
      ICING_LOG(WARNING) << "Failed to analyze existing DatabaseStablenessLog "
                            "file. Overwrite the content with a default proto.";
      ICING_RETURN_IF_ERROR(WriteProtoIntoFile(*filesystem, sfd, proto));
    } else {
      proto = std::move(proto_or).ValueOrDie();
    }
  }

  return std::unique_ptr<DatabaseStablenessLog>(new DatabaseStablenessLog(
      *filesystem, std::move(file_path), std::move(sfd), std::move(proto)));
}

libtextclassifier3::Status DatabaseStablenessLog::UpdateApiHistory(
    IcingApiCallType::Code call_type, int64_t timestamp_ms) {
  if (call_type == IcingApiCallType::UNKNOWN) {
    return absl_ports::InvalidArgumentError("Invalid API call type.");
  }

  if (call_type == IcingApiCallType::PERSIST_TO_DISK) {
    return absl_ports::InvalidArgumentError(
        "Should call UpdatePersistToDiskHistory instead.");
  }

  absl_ports::unique_lock lock(&mutex_);

  // Step 1: write the history into the proto. If the call type already exists,
  //   overwrite the existing history.
  bool found = false;
  for (ApiHistoryProto& api_history : *cached_proto_.mutable_api_history()) {
    if (api_history.call_type() == call_type) {
      api_history.set_last_call_timestamp_ms(timestamp_ms);
      found = true;
      break;
    }
  }
  if (!found) {
    ApiHistoryProto* new_api_history = cached_proto_.add_api_history();
    new_api_history->set_call_type(call_type);
    new_api_history->set_last_call_timestamp_ms(timestamp_ms);
  }

  // Step 2: write the proto into the disk.
  ICING_RETURN_IF_ERROR(WriteProtoIntoFile(filesystem_, sfd_, cached_proto_));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DatabaseStablenessLog::UpdatePersistToDiskHistory(
    PersistType::Code persist_type, int64_t timestamp_ms) {
  absl_ports::unique_lock lock(&mutex_);

  // Step 1: write the history into the proto.
  switch (persist_type) {
    case PersistType::LITE:
      cached_proto_.set_last_flush_lite_timestamp_ms(timestamp_ms);
      break;
    case PersistType::FULL:
      cached_proto_.set_last_flush_full_timestamp_ms(timestamp_ms);
      break;
    case PersistType::RECOVERY_PROOF:
      cached_proto_.set_last_flush_recovery_proof_timestamp_ms(timestamp_ms);
      break;
    case PersistType::SHUTDOWN:
      cached_proto_.set_last_flush_shutdown_timestamp_ms(timestamp_ms);
      break;
    case PersistType::DESTRUCTOR:
      cached_proto_.set_last_flush_destructor_timestamp_ms(timestamp_ms);
      break;
    case PersistType::UNKNOWN:
      return absl_ports::InvalidArgumentError("Invalid persist type.");
  }

  // Step 2: write the proto into the disk.
  ICING_RETURN_IF_ERROR(WriteProtoIntoFile(filesystem_, sfd_, cached_proto_));

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
