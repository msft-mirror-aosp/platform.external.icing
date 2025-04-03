// Copyright (C) 2023 Google LLC
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

#ifndef ICING_SCHEMA_BACKUP_SCHEMA_PRODUCER_H_
#define ICING_SCHEMA_BACKUP_SCHEMA_PRODUCER_H_

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/feature-flags.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/section-manager.h"

namespace icing {
namespace lib {

class BackupSchemaProducer {
 public:
  explicit BackupSchemaProducer(const FeatureFlags* feature_flags)
      : feature_flags_(*feature_flags) {}

  // Creates a BackupSchemaResult based off of schema.
  // If schema doesn't require a backup schema (because it is fully
  // rollback-proof) then `BackupSchemaResult::backup_schema_produced` will be
  // false. No guarantee is made about the state of
  // `BackupSchemaResult::backup_schema` in this case.
  // If schema *does* require a backup schema, then
  //`BackupSchemaResult::backup_schema_produced` will be true and
  // `BackupSchemaResult::backup_schema` will be populated accordingly.
  // Returns:
  //   - On success, a BackupSchemaResult
  //   - INTERNAL_ERROR if the schema is inconsistent with the type_manager.
  struct BackupSchemaResult {
    BackupSchemaResult() : backup_schema(), backup_schema_produced(false) {}
    explicit BackupSchemaResult(SchemaProto backup_schema_in)
        : backup_schema(backup_schema_in), backup_schema_produced(true) {}

    SchemaProto backup_schema;
    bool backup_schema_produced;
  };
  libtextclassifier3::StatusOr<BackupSchemaProducer::BackupSchemaResult>
  Produce(const SchemaProto& schema, const SectionManager& type_manager);

 private:
  const FeatureFlags& feature_flags_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCHEMA_BACKUP_SCHEMA_PRODUCER_H_
