// Copyright (C) 2022 Google LLC
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

#ifndef THIRD_PARTY_ICING_MONKEY_TEST_ICING_MONKEY_TEST_RUNNER_H_
#define THIRD_PARTY_ICING_MONKEY_TEST_ICING_MONKEY_TEST_RUNNER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "third_party/icing/file/filesystem.h"
#include "third_party/icing/icing-search-engine.h"
#include "third_party/icing/monkey_test/in-memory-icing-search-engine.h"
#include "third_party/icing/monkey_test/monkey-test-generators.h"
#include "third_party/icing/monkey_test/monkey-test-util.h"
#include "third_party/icing/proto/schema.proto.h"

namespace icing {
namespace lib {

class IcingMonkeyTestRunner {
 public:
  IcingMonkeyTestRunner(IcingMonkeyTestRunnerConfiguration config);
  IcingMonkeyTestRunner(const IcingMonkeyTestRunner&) = delete;
  IcingMonkeyTestRunner& operator=(const IcingMonkeyTestRunner&) = delete;

  SetSchemaResultProto SetSchema(SchemaProto&& schema);

  // This function must and should only be called before running the monkey
  // test.
  void Initialize();

  // Run the monkey test with num operations.
  void Run(uint32_t num);

  // APIs supported in icing search engine.
  void DoUpdateSchema();
  void DoGet();
  void DoGetAllNamespaces();
  void DoPut();
  void DoDelete();
  void DoDeleteByNamespace();
  void DoDeleteBySchemaType();
  void DoDeleteByQuery();
  void DoSearch();

  // Operations with no observable side-effects.
  void ReloadFromDisk();
  void DoOptimize();

 private:
  IcingMonkeyTestRunnerConfiguration config_;
  MonkeyTestRandomEngine random_;
  Filesystem filesystem_;
  std::string icing_dir_;
  std::unique_ptr<InMemoryIcingSearchEngine> in_memory_icing_;
  std::unique_ptr<IcingSearchEngine> icing_;

  std::unique_ptr<MonkeySchemaGenerator> schema_generator_;
  std::unique_ptr<MonkeyDocumentGenerator> document_generator_;

  void CreateIcingSearchEngine();
  // Reloads the in-memory icing from the disk.
  void ReloadInMemoryIcing();
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_MONKEY_TEST_ICING_MONKEY_TEST_RUNNER_H_
