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

#include <cstdint>
#include <random>

#include "testing/base/public/benchmark.h"
#include "gmock/gmock.h"
#include "icing/document-builder.h"
#include "icing/file/file-backed-proto-log.h"
#include "icing/file/filesystem.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/document.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/random-string.h"
#include "icing/testing/tmp-directory.h"

// go/microbenchmarks
//
// To build and run on a local machine:
//   $ blaze build -c opt --dynamic_mode=off --copt=-gmlt
//   icing/file:file-backed-proto-log_benchmark
//
//   $ blaze-bin/icing/file/file-backed-proto-log_benchmark
//   --benchmarks=all
//
//
// To build and run on an Android device (must be connected and rooted):
//   $ blaze build --copt="-DGOOGLE_COMMANDLINEFLAGS_FULL_API=1"
//   --config=android_arm64 -c opt --dynamic_mode=off --copt=-gmlt
//   icing/file:file-backed-proto-log_benchmark
//
//   $ adb root
//
//   $ adb push
//   blaze-bin/icing/file/file-backed-proto-log_benchmark
//   /data/local/tmp/
//
//   $ adb shell /data/local/tmp/file-backed-proto-log-benchmark
//   --benchmarks=all

namespace icing {
namespace lib {

namespace {

static void BM_Write(benchmark::State& state) {
  const Filesystem filesystem;
  int string_length = state.range(0);
  const std::string file_path = IcingStringUtil::StringPrintf(
      "%s%s%d%s", GetTestTempDir().c_str(), "/proto_", string_length, ".log");
  int max_proto_size = (1 << 24) - 1;  // 16 MiB
  bool compress = true;

  // Make sure it doesn't already exist.
  filesystem.DeleteFile(file_path.c_str());

  auto proto_log =
      FileBackedProtoLog<DocumentProto>::Create(
          &filesystem, file_path,
          FileBackedProtoLog<DocumentProto>::Options(compress, max_proto_size))
          .ValueOrDie()
          .proto_log;

  DocumentProto document = DocumentBuilder().SetKey("namespace", "uri").Build();

  std::default_random_engine random;
  const std::string rand_str =
      RandomString(kAlNumAlphabet, string_length, &random);

  auto document_properties = document.add_properties();
  document_properties->set_name("string property");
  document_properties->add_string_values(rand_str);

  for (auto _ : state) {
    testing::DoNotOptimize(proto_log->WriteProto(document));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          string_length);

  // Cleanup after ourselves
  filesystem.DeleteFile(file_path.c_str());
}
BENCHMARK(BM_Write)
    ->Arg(1)
    ->Arg(32)
    ->Arg(512)
    ->Arg(1024)
    ->Arg(4 * 1024)
    ->Arg(8 * 1024)
    ->Arg(16 * 1024)
    ->Arg(32 * 1024)
    ->Arg(256 * 1024)
    ->Arg(2 * 1024 * 1024)
    ->Arg(8 * 1024 * 1024)
    ->Arg(15 * 1024 * 1024);  // We do 15MiB here since our max proto size is
                              // 16MiB, and we need some extra space for the
                              // rest of the document properties

static void BM_Read(benchmark::State& state) {
  const Filesystem filesystem;
  int string_length = state.range(0);
  const std::string file_path = IcingStringUtil::StringPrintf(
      "%s%s%d%s", GetTestTempDir().c_str(), "/proto_", string_length, ".log");
  int max_proto_size = (1 << 24) - 1;  // 16 MiB
  bool compress = true;

  // Make sure it doesn't already exist.
  filesystem.DeleteFile(file_path.c_str());

  auto proto_log =
      FileBackedProtoLog<DocumentProto>::Create(
          &filesystem, file_path,
          FileBackedProtoLog<DocumentProto>::Options(compress, max_proto_size))
          .ValueOrDie()
          .proto_log;

  DocumentProto document = DocumentBuilder().SetKey("namespace", "uri").Build();

  std::default_random_engine random;
  const std::string rand_str =
      RandomString(kAlNumAlphabet, string_length, &random);

  auto document_properties = document.add_properties();
  document_properties->set_name("string property");
  document_properties->add_string_values(rand_str);

  ICING_ASSERT_OK_AND_ASSIGN(int64_t write_offset,
                             proto_log->WriteProto(document));

  for (auto _ : state) {
    testing::DoNotOptimize(proto_log->ReadProto(write_offset));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          string_length);

  // Cleanup after ourselves
  filesystem.DeleteFile(file_path.c_str());
}
BENCHMARK(BM_Read)
    ->Arg(1)
    ->Arg(32)
    ->Arg(512)
    ->Arg(1024)
    ->Arg(4 * 1024)
    ->Arg(8 * 1024)
    ->Arg(16 * 1024)
    ->Arg(32 * 1024)
    ->Arg(256 * 1024)
    ->Arg(2 * 1024 * 1024)
    ->Arg(8 * 1024 * 1024)
    ->Arg(15 * 1024 * 1024);  // We do 15MiB here since our max proto size is
                              // 16MiB, and we need some extra space for the
                              // rest of the document properties

}  // namespace
}  // namespace lib
}  // namespace icing
