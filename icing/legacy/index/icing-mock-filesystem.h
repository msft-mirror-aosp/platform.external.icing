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
  MOCK_METHOD(bool, DeleteFile, (const char *file_name), (const, override));

  MOCK_METHOD(bool, DeleteDirectory, (const char *dir_name), (const, override));

  MOCK_METHOD(bool, DeleteDirectoryRecursively, (const char *dir_name),
              (const, override));

  MOCK_METHOD(bool, FileExists, (const char *file_name), (const, override));

  MOCK_METHOD(bool, DirectoryExists, (const char *dir_name), (const, override));

  MOCK_METHOD(int, GetBasenameIndex, (const char *file_name),
              (const, override));

  MOCK_METHOD(std::string, GetBasename, (const char *file_name),
              (const, override));

  MOCK_METHOD(std::string, GetDirname, (const char *file_name),
              (const, override));

  MOCK_METHOD(bool, ListDirectory,
              (const char *dir_name, std::vector<std::string> *entries),
              (const, override));

  MOCK_METHOD(bool, GetMatchingFiles,
              (const char *glob, std::vector<std::string> *matches),
              (const, override));

  MOCK_METHOD(int, OpenForWrite, (const char *file_name), (const, override));

  MOCK_METHOD(int, OpenForAppend, (const char *file_name), (const, override));

  MOCK_METHOD(int, OpenForRead, (const char *file_name), (const, override));

  MOCK_METHOD(uint64_t, GetFileSize, (int fd), (const, override));

  MOCK_METHOD(uint64_t, GetFileSize, (const char *filename), (const, override));

  MOCK_METHOD(bool, Truncate, (int fd, uint64_t new_size), (const, override));

  MOCK_METHOD(bool, Truncate, (const char *filename, uint64_t new_size),
              (const, override));

  MOCK_METHOD(bool, Grow, (int fd, uint64_t new_size), (const, override));

  MOCK_METHOD(bool, Write, (int fd, const void *data, size_t data_size),
              (const, override));
  MOCK_METHOD(bool, PWrite,
              (int fd, off_t offset, const void *data, size_t data_size),
              (const, override));

  MOCK_METHOD(bool, DataSync, (int fd), (const, override));

  MOCK_METHOD(bool, RenameFile, (const char *old_name, const char *new_name),
              (const, override));

  MOCK_METHOD(bool, SwapFiles, (const char *one, const char *two),
              (const, override));

  MOCK_METHOD(bool, CreateDirectory, (const char *dir_name), (const, override));

  MOCK_METHOD(bool, CreateDirectoryRecursively, (const char *dir_name),
              (const, override));

  MOCK_METHOD(bool, CopyFile, (const char *src, const char *dst),
              (const, override));

  MOCK_METHOD(bool, ComputeChecksum,
              (int fd, uint32_t *checksum, uint64_t offset, uint64_t length),
              (const, override));

  MOCK_METHOD(uint64_t, GetDiskUsage, (const char *path), (const, override));
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_LEGACY_INDEX_ICING_MOCK_FILESYSTEM_H_
