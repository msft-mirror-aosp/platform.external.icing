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

#ifndef ICING_UTIL_SCORABLE_PROPERTY_SET_H_
#define ICING_UTIL_SCORABLE_PROPERTY_SET_H_

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/proto/internal/scorable_property_set.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"

namespace icing {
namespace lib {

// A class that interprets and represents the data from the proto of
// ScorablePropertySetProto.
//
// This class serves as an utility to access the scorable property data.
class ScorablePropertySet {
 public:
  // Creates a class that represents the data from the proto of
  // ScorablePropertySetProto.
  //
  // This function is light weight, and it doesn't perform any proto copy.
  //
  // Returns:
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid, or the proto data is
  //     inconsistent with the schema config.
  //   - FAILED_PRECONDITION if the schema hasn't been set yet
  static libtextclassifier3::StatusOr<std::unique_ptr<ScorablePropertySet>>
  Create(ScorablePropertySetProto&& scorable_property_set_proto,
         SchemaTypeId schema_type_id, const SchemaStore* schema_store);

  // Creates a class that represents the data from the proto of
  // ScorablePropertySetProto.
  //
  // This function converts the input |document| to the ScorablePropertySetProto
  // under the hood.
  //
  // Returns:
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid, or that no scorable
  //     property is found under the schema config of |schema_type_id|.
  //   - FAILED_PRECONDITION if the schema hasn't been set yet
  static libtextclassifier3::StatusOr<std::unique_ptr<ScorablePropertySet>>
  Create(const DocumentProto& document, SchemaTypeId schema_type_id,
         const SchemaStore* schema_store);

  // Delete copy constructor and assignment operator.
  ScorablePropertySet(const ScorablePropertySet&) = delete;
  ScorablePropertySet& operator=(const ScorablePropertySet&) = delete;

  // Returns the scorable property proto for the given property path.
  //
  // Return:
  //   - ScorablePropertyProto on success
  //   - nullptr if the property path is not found in the schema config
  //     as a scorable property.
  const ScorablePropertyProto* GetScorablePropertyProto(
      const std::string& property_path) const;

  // Returns the reference to the underlying ScorablePropertySetProto.
  const ScorablePropertySetProto& GetScorablePropertySetProto() const {
    return scorable_property_set_proto_;
  }

 private:
  explicit ScorablePropertySet(
      ScorablePropertySetProto&& scorable_property_set_proto,
      SchemaTypeId schema_type_id, const SchemaStore* schema_store);

  const ScorablePropertySetProto scorable_property_set_proto_;
  const SchemaTypeId schema_type_id_;
  const SchemaStore* schema_store_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_SCORABLE_PROPERTY_SET_H_
