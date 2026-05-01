// Copyright (C) 2026 Google LLC
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

#ifndef ICING_INDEX_EMBED_MINI_BATCH_K_MEANS_H_
#define ICING_INDEX_EMBED_MINI_BATCH_K_MEANS_H_

#include <cstdint>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/proto/ann.pb.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {

class MiniBatchKMeans {
 public:
  struct ClusteringResult {
    // The centroids of the clusters.
    std::vector<std::vector<float>> centroids;

    // The cluster assignment for each input embedding.
    // Indices correspond to the input vector order.
    std::vector<int> partition_assignments;

    // The actual number of iterations performed.
    uint32_t actual_iterations;
  };

  // Computes the K-Means clustering for the given embeddings.
  //
  // Returns:
  //   ClusteringResult on success
  //   Error on failure (e.g. invalid arguments)
  static libtextclassifier3::StatusOr<ClusteringResult> Compute(
      const std::vector<EmbeddingReference>& embeddings, int dimension,
      const MiniBatchKMeansOptions& options, const Clock* clock);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_MINI_BATCH_K_MEANS_H_
