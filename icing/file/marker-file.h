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

#ifndef ICING_FILE_MARKER_FILE_H_
#define ICING_FILE_MARKER_FILE_H_

#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/file-backed-proto.h"
#include "icing/file/filesystem.h"
#include "icing/proto/initialize.pb.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

class MarkerFile {
 public:
  // Creates a destructible marker file object with the given operation type.
  // - If instantiation succeeds, then the marker file is created and a marker
  //   proto with the given operation type is written to the file.
  // - The marker file is deleted upon destruction of the marker file object.
  //
  // Returns:
  //   - On success, a unique pointer to the marker file object.
  //   - FAILED_PRECONDITION_ERROR if any of the pointer is null, or the marker
  //     file already exists.
  //   - INVALID_ARGUMENT_ERROR if the operation type is UNKNOWN.
  //   - INTERNAL_ERROR on I/O errors.
  //   - Any FileBackedProto errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<MarkerFile>> Create(
      const Filesystem* filesystem, std::string filepath,
      IcingSearchEngineMarkerProto::OperationType::Code operation_type);

  // Attempts to read the marker file and returns the content (in proto format).
  // - If the file exists, reads the content and returns the proto. If any error
  //   occurs when reading, returns a default proto with UNKNOWN operation type.
  // - If the file does not exist, returns nullptr.
  //
  // Returns:
  //   - A unique pointer to IcingSearchEngineMarkerProto if the file exists.
  //   - nullptr otherwise.
  static std::unique_ptr<IcingSearchEngineMarkerProto> Postmortem(
      const Filesystem& filesystem, const std::string& filepath);

  ~MarkerFile() {
    if (!filesystem_.DeleteFile(filepath_.c_str())) {
      ICING_VLOG(1) << "Failed to delete marker file " << filepath_
                    << " during destruction.";
    }
  }

 private:
  explicit MarkerFile(
      const Filesystem* filesystem, std::string filepath,
      std::unique_ptr<FileBackedProto<IcingSearchEngineMarkerProto>>
          file_backed_proto)
      : filesystem_(*filesystem),
        filepath_(std::move(filepath)),
        file_backed_proto_(std::move(file_backed_proto)) {}

  const Filesystem& filesystem_;
  std::string filepath_;

  // The file-backed proto object that is used to read/write the marker file.
  // Keep this instance alive, so in the future we can provide more methods to
  // write new fields to the marker file.
  std::unique_ptr<FileBackedProto<IcingSearchEngineMarkerProto>>
      file_backed_proto_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_MARKER_FILE_H_
