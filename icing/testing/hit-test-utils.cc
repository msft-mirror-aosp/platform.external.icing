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

#include "icing/testing/hit-test-utils.h"

#include <cstdint>
#include <vector>

#include "icing/index/embed/embedding-hit.h"
#include "icing/index/hit/hit.h"
#include "icing/index/main/posting-list-hit-serializer.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Returns a hit that has a delta of desired_byte_length from last_hit.
Hit CreateHit(const Hit& last_hit, int desired_byte_length) {
  return CreateHit(last_hit, desired_byte_length, last_hit.term_frequency(),
                   /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                   /*is_stemmed_hit=*/false);
}

// Returns a hit that has a delta of desired_byte_length from last_hit, with
// the desired term_frequency and flags
Hit CreateHit(const Hit& last_hit, int desired_byte_length,
              Hit::TermFrequency term_frequency, bool is_in_prefix_section,
              bool is_prefix_hit, bool is_stemmed_hit) {
  Hit hit = last_hit;
  uint8_t buf[5];
  do {
    hit = (hit.section_id() == kMinSectionId)
              ? Hit(kMaxSectionId, hit.document_id() + 1, term_frequency,
                    is_in_prefix_section, is_prefix_hit, is_stemmed_hit)
              : Hit(hit.section_id() - 1, hit.document_id(), term_frequency,
                    is_in_prefix_section, is_prefix_hit, is_stemmed_hit);
  } while (PostingListHitSerializer::EncodeNextHitValue(
               /*next_hit_value=*/hit.value(),
               /*curr_hit_value=*/last_hit.value(), buf) < desired_byte_length);
  return hit;
}

// Returns a vector of num_hits Hits with the first hit starting at start_docid
// and with deltas of the desired byte length.
std::vector<Hit> CreateHits(DocumentId start_docid, int num_hits,
                            int desired_byte_length) {
  std::vector<Hit> hits;
  if (num_hits < 1) {
    return hits;
  }
  hits.push_back(Hit(/*section_id=*/1, /*document_id=*/start_docid,
                     Hit::kDefaultTermFrequency, /*is_in_prefix_section=*/false,
                     /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false));
  while (hits.size() < num_hits) {
    hits.push_back(CreateHit(hits.back(), desired_byte_length));
  }
  return hits;
}

// Returns a vector of num_hits Hits with the first hit being the desired byte
// length from last_hit, and with deltas of the same desired byte length.
std::vector<Hit> CreateHits(const Hit& last_hit, int num_hits,
                            int desired_byte_length) {
  std::vector<Hit> hits;
  if (num_hits < 1) {
    return hits;
  }
  hits.reserve(num_hits);
  for (int i = 0; i < num_hits; ++i) {
    hits.push_back(
        CreateHit(hits.empty() ? last_hit : hits.back(), desired_byte_length));
  }
  return hits;
}

std::vector<Hit> CreateHits(int num_hits, int desired_byte_length) {
  return CreateHits(/*start_docid=*/0, num_hits, desired_byte_length);
}

EmbeddingHit CreateEmbeddingHit(const EmbeddingHit& last_hit,
                                uint32_t desired_byte_length) {
  // Create a delta that has (desired_byte_length - 1) * 7 + 1 bits, so that it
  // can be encoded in desired_byte_length bytes.
  uint64_t delta = UINT64_C(1) << ((desired_byte_length - 1) * 7);
  return EmbeddingHit(last_hit.value() - delta);
}

std::vector<EmbeddingHit> CreateEmbeddingHits(int num_hits,
                                              int desired_byte_length) {
  std::vector<EmbeddingHit> hits;
  if (num_hits == 0) {
    return hits;
  }
  hits.reserve(num_hits);
  hits.push_back(EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                              /*location=*/0));
  for (int i = 1; i < num_hits; ++i) {
    hits.push_back(CreateEmbeddingHit(hits.back(), desired_byte_length));
  }
  return hits;
}

}  // namespace lib
}  // namespace icing
