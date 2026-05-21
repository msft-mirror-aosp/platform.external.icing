// Copyright (C) 2024 Google LLC
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

#include "icing/testing/test-feature-flags.h"

#include "icing/feature-flags.h"

namespace icing {
namespace lib {

FeatureFlags GetTestFeatureFlags() {
  return FeatureFlags(
      /*enable_circular_schema_definitions=*/true,
      /*enable_repeated_field_joins=*/true,
      /*enable_embedding_backup_generation=*/true,
      /*enable_optimize_improvements=*/true,
      /*expired_document_purge_threshold_ms=*/0,
      /*enable_non_existent_qualified_id_join=*/true,
      /*enable_skip_set_schema_type_equality_check=*/true,
      /*enable_schema_definition_deduping=*/true,
      /*enable_delete_propagation_from=*/true);
}

}  // namespace lib
}  // namespace icing
