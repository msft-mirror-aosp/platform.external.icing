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

#include <cstdint>
#include <vector>

#include "icing/index/embed/embedding-hit.h"
#include "icing/index/hit/hit.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Returns a hit that has a delta of desired_byte_length from last_hit.
Hit CreateHit(const Hit& last_hit, int desired_byte_length);

// Returns a hit that has a delta of desired_byte_length from last_hit, with
// the desired term_frequency and flags
Hit CreateHit(const Hit& last_hit, int desired_byte_length,
              Hit::TermFrequency term_frequency, bool is_in_prefix_section,
              bool is_prefix_hit);

// Returns a vector of num_hits Hits with the first hit starting at start_docid
// and with desired_byte_length deltas.
std::vector<Hit> CreateHits(DocumentId start_docid, int num_hits,
                            int desired_byte_length);

// Returns a vector of num_hits Hits with the first hit being the desired byte
// length from last_hit, and with deltas of the same desired byte length.
std::vector<Hit> CreateHits(const Hit& last_hit, int num_hits,
                            int desired_byte_length);

// Returns a vector of num_hits Hits with the first hit starting at 0 and each
// with desired_byte_length deltas.
std::vector<Hit> CreateHits(int num_hits, int desired_byte_length);

// Returns a hit that has a delta of desired_byte_length from last_hit after
// VarInt encoding.
// Requires that 0 < desired_byte_length <= VarInt::kMaxEncodedLen64.
EmbeddingHit CreateEmbeddingHit(const EmbeddingHit& last_hit,
                                uint32_t desired_byte_length);

// Returns a vector of num_hits Hits with the first hit starting at document 0
// and with a delta of desired_byte_length between each subsequent hit after
// VarInt encoding.
std::vector<EmbeddingHit> CreateEmbeddingHits(int num_hits,
                                              int desired_byte_length);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_HIT_TEST_UTILS_H_
