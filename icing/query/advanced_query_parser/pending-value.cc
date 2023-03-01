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
#include "icing/query/advanced_query_parser/pending-value.h"

#include "icing/absl_ports/canonical_errors.h"

namespace icing {
namespace lib {

libtextclassifier3::Status PendingValue::ParseInt() {
  if (data_type_ == DataType::kLong) {
    return libtextclassifier3::Status::OK;
  } else if (data_type_ != DataType::kText) {
    return absl_ports::InvalidArgumentError("Cannot parse value as LONG");
  }
  char* value_end;
  long_val_ = std::strtoll(string_vals_.at(0).c_str(), &value_end, /*base=*/10);
  if (value_end != string_vals_.at(0).c_str() + string_vals_.at(0).length()) {
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Unable to parse \"", string_vals_.at(0), "\" as number."));
  }
  data_type_ = DataType::kLong;
  string_vals_.clear();
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
