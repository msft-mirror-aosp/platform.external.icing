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

#include "icing/index/embed/mini-batch-k-means.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/index/embed/quantizer.h"
#include "icing/util/clock.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

float ComputeDistance(const EmbeddingReference& embedding,
                      const std::vector<float>& centroid,
                      const EmbeddingScorer* scorer, int dimension) {
  if (embedding.float_vector != nullptr) {
    return scorer->EigenScore(dimension, embedding.float_vector,
                              centroid.data());
  } else {
    // Quantized
    Quantizer quantizer(embedding.quantized_vector);
    const uint8_t* q_vec = reinterpret_cast<const uint8_t*>(
        embedding.quantized_vector + sizeof(Quantizer));
    return scorer->EigenScore(dimension, centroid.data(), q_vec, quantizer);
  }
}

void Dequantize(const EmbeddingReference& embedding, std::vector<float>& out,
                int dimension) {
  out.resize(dimension);
  if (embedding.float_vector != nullptr) {
    std::copy(embedding.float_vector, embedding.float_vector + dimension,
              out.begin());
  } else {
    Quantizer quantizer(embedding.quantized_vector);
    const uint8_t* q_vec = reinterpret_cast<const uint8_t*>(
        embedding.quantized_vector + sizeof(Quantizer));
    for (int i = 0; i < dimension; ++i) {
      out[i] = quantizer.Dequantize(q_vec[i]);
    }
  }
}

int FindNearestCluster(const EmbeddingReference& embedding,
                       const std::vector<std::vector<float>>& centroids,
                       const EmbeddingScorer* scorer, int dimension,
                       float balance_penalty = 0.0f,
                       const std::vector<int>* cluster_counts = nullptr) {
  int best_cluster = -1;
  float best_score = std::numeric_limits<float>::max();

  for (int c = 0; c < centroids.size(); ++c) {
    float score = ComputeDistance(embedding, centroids[c], scorer, dimension);
    if (cluster_counts != nullptr) {
      score += balance_penalty * static_cast<float>((*cluster_counts)[c]);
    }
    if (score < best_score) {
      best_score = score;
      best_cluster = c;
    }
  }
  return best_cluster;
}

}  // namespace

// static
libtextclassifier3::StatusOr<MiniBatchKMeans::ClusteringResult>
MiniBatchKMeans::Compute(const std::vector<EmbeddingReference>& embeddings,
                         int dimension, const MiniBatchKMeansOptions& options,
                         const Clock* clock) {
  ICING_RETURN_ERROR_IF_NULL(clock);

  int num_embeddings = static_cast<int>(embeddings.size());
  if (num_embeddings == 0) {
    return absl_ports::InvalidArgumentError("Embeddings cannot be empty");
  }
  if (dimension <= 0) {
    return absl_ports::InvalidArgumentError("Dimension must be positive");
  }
  for (const auto& embedding : embeddings) {
    ICING_RETURN_IF_ERROR(embedding.Validate());
  }
  if (options.target_cluster_size() == 0) {
    return absl_ports::InvalidArgumentError(
        "Target cluster size must be positive");
  }
  if (options.mini_batch_size() == 0) {
    return absl_ports::InvalidArgumentError("Mini batch size must be positive");
  }

  // NOTE: Standard K-Means (and this Mini-Batch formulation) mathematically
  // requires Euclidean distance. The centroid update step `c <- (1 - eta) * c +
  // eta * x` calculates a strictly linear moving average. The arithmetic mean
  // is the true minimizer ONLY for the sum of squared Euclidean distances.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<EmbeddingScorer> scorer,
      EmbeddingScorer::Create(
          SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN));

  int k = static_cast<int>(
      std::max(1u, num_embeddings / options.target_cluster_size()));
  std::mt19937 random(options.random_seed());

  // Initialize centroids
  std::vector<std::vector<float>> centroids(k);
  std::vector<int> centroid_indices(k);
  std::iota(centroid_indices.begin(), centroid_indices.end(), 0);
  // Reservoir sampling (Algorithm R) uniformly picks k random indices without
  // replacement. It operates in O(N) time and O(k) space.
  for (int i = k; i < num_embeddings; ++i) {
    std::uniform_int_distribution<int> dist(0, i);
    int j = dist(random);
    if (j < k) {
      centroid_indices[j] = i;
    }
  }
  for (int i = 0; i < k; ++i) {
    Dequantize(embeddings[centroid_indices[i]], centroids[i], dimension);
  }

  // Training iterations
  std::vector<int> cluster_counts(k, 0);
  double expected_iterations = static_cast<double>(num_embeddings) *
                               options.sampling_ratio() /
                               options.mini_batch_size();
  expected_iterations = std::ceil(
      std::max<double>(expected_iterations, options.min_num_iterations()));

  std::unique_ptr<Timer> timer = clock->GetNewTimer();
  uint32_t actual_iterations = 0;

  // Pre-allocate memory outside the loop to avoid repeated allocations.
  std::vector<int> mini_batch_indices(options.mini_batch_size());
  std::vector<int> batch_assignments(options.mini_batch_size());
  std::vector<int> current_counts;
  std::vector<float> temp_vec(dimension);
  std::uniform_int_distribution<int> uniform_idx_dist(0, num_embeddings - 1);

  for (; actual_iterations < expected_iterations; ++actual_iterations) {
    if (options.timeout_ms() > 0 &&
        timer->GetElapsedMilliseconds() > options.timeout_ms()) {
      break;
    }

    // Mini-batch sampling
    for (int i = 0; i < options.mini_batch_size(); ++i) {
      mini_batch_indices[i] = uniform_idx_dist(random);
    }

    // Step 1: Assignment with constraints
    // Local assignments for this batch
    current_counts = cluster_counts;
    for (int i = 0; i < mini_batch_indices.size(); ++i) {
      int embedding_idx = mini_batch_indices[i];
      int c = FindNearestCluster(embeddings[embedding_idx], centroids,
                                 scorer.get(), dimension,
                                 options.balance_penalty(), &current_counts);
      batch_assignments[i] = c;
      current_counts[c]++;
    }

    // Step 2: Update centroids
    for (int i = 0; i < mini_batch_indices.size(); ++i) {
      int c = batch_assignments[i];
      if (c == -1) {
        return absl_ports::InternalError(
            "No cluster assigned to embedding. This should not happen.");
      }

      int embedding_idx = mini_batch_indices[i];
      cluster_counts[c]++;
      float eta = 1.0f / static_cast<float>(cluster_counts[c]);

      Dequantize(embeddings[embedding_idx], temp_vec, dimension);

      // c <- (1 - eta) * c + eta * x
      for (int d = 0; d < dimension; ++d) {
        centroids[c][d] = (1.0f - eta) * centroids[c][d] + eta * temp_vec[d];
      }
    }
  }

  // Final assignment
  std::vector<int> partition_assignments(num_embeddings);
  for (int i = 0; i < num_embeddings; ++i) {
    partition_assignments[i] =
        FindNearestCluster(embeddings[i], centroids, scorer.get(), dimension);
  }

  return ClusteringResult{std::move(centroids),
                          std::move(partition_assignments), actual_iterations};
}

}  // namespace lib
}  // namespace icing
