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

#ifndef ICING_QUERY_ADVANCED_QUERY_PARSER_OPTIMIZER_QUERY_OPTIMIZATION_UTIL_H_
#define ICING_QUERY_ADVANCED_QUERY_PARSER_OPTIMIZER_QUERY_OPTIMIZATION_UTIL_H_

#include <memory>
#include <vector>

#include "icing/feature-flags.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"

namespace icing {
namespace lib {
namespace query_optimization_util {

// Optimizes the given iterators that are all intended to be AND'd together.
// If an optimization is possible, then this node will be rewritten to an
// equivalent but more efficient iterator sub-tree. Otherwise, the iterators
// will be returned as a normal AND iterator.
std::unique_ptr<DocHitInfoIterator> OptimizeAndIteratorsIfPossible(
    std::vector<std::unique_ptr<DocHitInfoIterator>>&& iterators);

}  // namespace query_optimization_util
}  // namespace lib
}  // namespace icing

#endif  // ICING_QUERY_ADVANCED_QUERY_PARSER_OPTIMIZER_QUERY_OPTIMIZATION_UTIL_H_
