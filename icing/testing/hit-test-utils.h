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

#ifndef ICING_TESTING_HIT_TEST_UTILS_H_
#define ICING_TESTING_HIT_TEST_UTILS_H_

#include <vector>

#include "icing/index/hit/hit.h"
#include "icing/legacy/index/icing-bit-util.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Returns a hit that has a delta of desired_byte_length from last_hit.
Hit CreateHit(Hit last_hit, int desired_byte_length);

// Returns a vector of num_hits Hits with the first hit starting at start_docid
// and with desired_byte_length deltas.
std::vector<Hit> CreateHits(DocumentId start_docid, int num_hits,
                            int desired_byte_length);

// Returns a vector of num_hits Hits with the first hit starting at 0 and each
// with desired_byte_length deltas.
std::vector<Hit> CreateHits(int num_hits, int desired_byte_length);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_HIT_TEST_UTILS_H_
