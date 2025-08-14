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

#ifndef THIRD_PARTY_ICING_QUERY_QUERY_UTILS_H_
#define THIRD_PARTY_ICING_QUERY_QUERY_UTILS_H_

#include <cstdint>
#include <memory>

#include "third_party/icing/index/iterator/document-filter-predicate.h"
#include "third_party/icing/proto/search.proto.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/store/document-store.h"

namespace icing {
namespace lib {

std::unique_ptr<DocumentFilterPredicate> GetFilterPredicateBySchemaAndNamespace(
    const SearchSpecProto& search_spec, const DocumentStore& document_store,
    const SchemaStore& schema_store, int64_t current_time_ms);

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_QUERY_QUERY_UTILS_H_
