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

#ifndef ICING_LEGACY_INDEX_ICING_MOCK_FILESYSTEM_H_
#define ICING_LEGACY_INDEX_ICING_MOCK_FILESYSTEM_H_

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <memory>
#include <string>
#include <vector>

#include "icing/legacy/index/icing-filesystem.h"
#include "gmock/gmock.h"

namespace icing {
namespace lib {

class IcingMockFilesystem : public IcingFilesystem {
 public:
  MOCK_CONST_METHOD1(DeleteFile, bool(const char *file_name));

  MOCK_CONST_METHOD1(DeleteDirectory, bool(const char *dir_name));

  MOCK_CONST_METHOD1(DeleteDirectoryRecursively, bool(const char *dir_name));

  MOCK_CONST_METHOD1(FileExists, bool(const char *file_name));

  MOCK_CONST_METHOD1(DirectoryExists, bool(const char *dir_name));

  MOCK_CONST_METHOD1(GetBasenameIndex, int(const char *file_name));

  MOCK_CONST_METHOD1(GetBasename, std::string(const char *file_name));

  MOCK_CONST_METHOD1(GetDirname, std::string(const char *file_name));

  MOCK_CONST_METHOD2(ListDirectory, bool(const char *dir_name,
                                         std::vector<std::string> *entries));

  MOCK_CONST_METHOD2(GetMatchingFiles,
                     bool(const char *glob, std::vector<std::string> *matches));

  MOCK_CONST_METHOD1(OpenForWrite, int(const char *file_name));

  MOCK_CONST_METHOD1(OpenForAppend, int(const char *file_name));

  MOCK_CONST_METHOD1(OpenForRead, int(const char *file_name));

  MOCK_CONST_METHOD1(GetFileSize, uint64_t(int fd));

  MOCK_CONST_METHOD1(GetFileSize, uint64_t(const char *filename));

  MOCK_CONST_METHOD2(Truncate, bool(int fd, uint64_t new_size));

  MOCK_CONST_METHOD2(Truncate, bool(const char *filename, uint64_t new_size));

  MOCK_CONST_METHOD2(Grow, bool(int fd, uint64_t new_size));

  MOCK_CONST_METHOD3(Write, bool(int fd, const void *data, size_t data_size));
  MOCK_CONST_METHOD4(PWrite, bool(int fd, off_t offset, const void *data,
                                  size_t data_size));

  MOCK_CONST_METHOD1(DataSync, bool(int fd));

  MOCK_CONST_METHOD2(RenameFile,
                     bool(const char *old_name, const char *new_name));

  MOCK_CONST_METHOD2(SwapFiles, bool(const char *one, const char *two));

  MOCK_CONST_METHOD1(CreateDirectory, bool(const char *dir_name));

  MOCK_CONST_METHOD1(CreateDirectoryRecursively, bool(const char *dir_name));

  MOCK_CONST_METHOD2(CopyFile, bool(const char *src, const char *dst));

  MOCK_CONST_METHOD4(ComputeChecksum, bool(int fd, uint32_t *checksum,
                                           uint64_t offset, uint64_t length));

  MOCK_CONST_METHOD1(GetDiskUsage, uint64_t(const char *path));
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_LEGACY_INDEX_ICING_MOCK_FILESYSTEM_H_
