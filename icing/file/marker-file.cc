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

#include "icing/file/marker-file.h"

#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/file-backed-proto.h"
#include "icing/file/filesystem.h"
#include "icing/proto/initialize.pb.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<MarkerFile>>
MarkerFile::Create(
    const Filesystem* filesystem, std::string filepath,
    IcingSearchEngineMarkerProto::OperationType::Code operation_type) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);

  if (operation_type == IcingSearchEngineMarkerProto::OperationType::UNKNOWN) {
    return absl_ports::InvalidArgumentError(
        "Cannot create marker file with UNKNOWN operation type.");
  }

  if (filesystem->FileExists(filepath.c_str())) {
    return absl_ports::FailedPreconditionError(
        absl_ports::StrCat("Marker file already exists: ", filepath));
  }

  // Create the instance.
  auto file_backed_proto =
      std::make_unique<FileBackedProto<IcingSearchEngineMarkerProto>>(
          *filesystem, filepath);
  auto marker_file = std::unique_ptr<MarkerFile>(new MarkerFile(
      filesystem, std::move(filepath), std::move(file_backed_proto)));

  // Initialize and write the proto with the given operation type.
  auto marker_proto = std::make_unique<IcingSearchEngineMarkerProto>();
  marker_proto->set_operation_type(operation_type);
  ICING_RETURN_IF_ERROR(
      marker_file->file_backed_proto_->Write(std::move(marker_proto)));

  return marker_file;
}

/* static */ std::unique_ptr<IcingSearchEngineMarkerProto>
MarkerFile::Postmortem(const Filesystem& filesystem,
                       const std::string& filepath) {
  if (!filesystem.FileExists(filepath.c_str())) {
    return nullptr;
  }

  FileBackedProto<IcingSearchEngineMarkerProto> marker_file(filesystem,
                                                            filepath);
  libtextclassifier3::StatusOr<const IcingSearchEngineMarkerProto*>
      marker_proto_or = marker_file.Read();
  if (!marker_proto_or.ok()) {
    // Since postmortem failure doesn't affect the correctness of the logic, we
    // just log the error message without passing it up. But we should  still
    // return a valid IcingSearchEngineMarkerProto with default values (with
    // UNKNOWN previous operation) indicating that the general marker file is
    // present on disk.
    ICING_LOG(ERROR) << "Failed to read existing marker file: "
                     << marker_proto_or.status().error_message();
    return std::make_unique<IcingSearchEngineMarkerProto>();
  }
  return std::make_unique<IcingSearchEngineMarkerProto>(
      *marker_proto_or.ValueOrDie());
}

}  // namespace lib
}  // namespace icing
