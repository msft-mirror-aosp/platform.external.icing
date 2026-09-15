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
#include <random>
#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/embed/quantizer.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {
namespace {

using ::testing::HasSubstr;
using ::testing::SizeIs;

class MiniBatchKMeansTest : public ::testing::Test {
 protected:
  Clock clock_;
};

// Helper to generate random points around centers
std::vector<std::vector<float>> GenerateData(int num_points, int dimension,
                                             int num_clusters, int seed) {
  std::mt19937 rng(seed);
  std::uniform_real_distribution<float> center_dist(-10.0f, 10.0f);
  std::normal_distribution<float> noise_dist(0.0f, 0.5f);

  std::vector<std::vector<float>> centers(num_clusters,
                                          std::vector<float>(dimension));
  for (int i = 0; i < num_clusters; ++i) {
    for (int d = 0; d < dimension; ++d) {
      centers[i][d] = center_dist(rng);
    }
  }

  std::vector<std::vector<float>> data(num_points,
                                       std::vector<float>(dimension));
  for (int i = 0; i < num_points; ++i) {
    int cluster = i % num_clusters;
    for (int d = 0; d < dimension; ++d) {
      data[i][d] = centers[cluster][d] + noise_dist(rng);
    }
  }
  return data;
}

TEST_F(MiniBatchKMeansTest, InvalidArguments) {
  std::vector<EmbeddingReference> embeddings(1);
  float vec[] = {1.0f};
  embeddings[0].float_vector = vec;
  MiniBatchKMeansOptions options;

  // Empty embeddings
  EXPECT_THAT(MiniBatchKMeans::Compute({}, 1, options, &clock_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Embeddings cannot be empty")));

  // Invalid dimension
  EXPECT_THAT(MiniBatchKMeans::Compute(embeddings, 0, options, &clock_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Dimension must be positive")));

  // Invalid Options
  options.set_target_cluster_size(0);
  EXPECT_THAT(MiniBatchKMeans::Compute(embeddings, 1, options, &clock_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Target cluster size must be positive")));
  options.set_target_cluster_size(100);

  options.set_mini_batch_size(0);
  EXPECT_THAT(MiniBatchKMeans::Compute(embeddings, 1, options, &clock_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Mini batch size must be positive")));
  options.set_mini_batch_size(100);

  // Partial/Invalid Embedding
  std::vector<EmbeddingReference> invalid_embeddings(1);
  // Both null
  EXPECT_THAT(MiniBatchKMeans::Compute(invalid_embeddings, 1, options, &clock_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("must have exactly one")));

  // Both set (float and quantized)
  char q_vec[] = {0};
  invalid_embeddings[0].quantized_vector = q_vec;
  invalid_embeddings[0].float_vector = vec;
  EXPECT_THAT(MiniBatchKMeans::Compute(invalid_embeddings, 1, options, &clock_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("must have exactly one")));
}

TEST_F(MiniBatchKMeansTest, SimpleClusteringFloat) {
  std::vector<float> data = {
      0.1f,  0.1f,   // Cluster 1
      0.2f,  0.2f,   // Cluster 1
      -0.1f, -0.1f,  // Cluster 1
      9.9f,  9.9f,   // Cluster 2
      10.1f, 10.1f,  // Cluster 2
      10.0f, 10.0f   // Cluster 2
  };
  int dimension = 2;
  int num_points = 6;

  std::vector<EmbeddingReference> embeddings(num_points);
  for (int i = 0; i < num_points; ++i) {
    embeddings[i].float_vector = &data[i * dimension];
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(3);
  options.set_mini_batch_size(2);
  options.set_min_num_iterations(10);
  options.set_random_seed(123);

  ICING_ASSERT_OK_AND_ASSIGN(
      auto result,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  EXPECT_THAT(result.centroids, SizeIs(2));
  EXPECT_THAT(result.partition_assignments, SizeIs(num_points));

  int cluster1 = result.partition_assignments[0];
  EXPECT_EQ(result.partition_assignments[1], cluster1);
  EXPECT_EQ(result.partition_assignments[2], cluster1);

  int cluster2 = result.partition_assignments[3];
  EXPECT_EQ(result.partition_assignments[4], cluster2);
  EXPECT_EQ(result.partition_assignments[5], cluster2);

  EXPECT_NE(cluster1, cluster2);
}

TEST_F(MiniBatchKMeansTest, SimpleClusteringQuantized) {
  // Same data as SimpleClusteringFloat
  std::vector<float> data = {0.1f, 0.1f, 0.2f,  0.2f,  -0.1f, -0.1f,
                             9.9f, 9.9f, 10.1f, 10.1f, 10.0f, 10.0f};
  int dimension = 2;
  int num_points = 6;

  // Create Quantizer
  // Range -1 to 11 to cover all points
  ICING_ASSERT_OK_AND_ASSIGN(auto quantizer, Quantizer::Create(-1.0f, 11.0f));

  std::vector<std::string> quantized_data_strings(num_points);
  std::vector<EmbeddingReference> embeddings(num_points);

  for (int i = 0; i < num_points; ++i) {
    quantized_data_strings[i].append(reinterpret_cast<const char*>(&quantizer),
                                     sizeof(Quantizer));
    for (int d = 0; d < dimension; ++d) {
      uint8_t q_val = quantizer.Quantize(data[i * dimension + d]);
      quantized_data_strings[i].push_back(static_cast<char>(q_val));
    }
    embeddings[i].quantized_vector = quantized_data_strings[i].data();
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(3);
  options.set_mini_batch_size(2);
  options.set_min_num_iterations(10);
  options.set_random_seed(123);

  ICING_ASSERT_OK_AND_ASSIGN(
      auto result,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  ASSERT_THAT(result.centroids, SizeIs(2));

  // Check assignments
  int cluster1 = result.partition_assignments[0];
  EXPECT_EQ(result.partition_assignments[1], cluster1);
  EXPECT_EQ(result.partition_assignments[2], cluster1);

  int cluster2 = result.partition_assignments[3];
  EXPECT_EQ(result.partition_assignments[4], cluster2);
  EXPECT_EQ(result.partition_assignments[5], cluster2);

  EXPECT_NE(cluster1, cluster2);
}

TEST_F(MiniBatchKMeansTest, BalanceConstraintEffect) {
  // Use a continuous distribution with variable density to allow centroids
  // to shift naturally.
  // 80 points in [0, 2] (High density)
  // 20 points in [2, 10] (Low density)
  // Total 100 points. Target 50 per cluster.
  int num_dense = 80;
  int num_sparse = 20;
  int num_points = num_dense + num_sparse;
  int dimension = 1;

  std::vector<std::vector<float>> data(num_points,
                                       std::vector<float>(dimension));
  std::vector<EmbeddingReference> embeddings(num_points);

  std::mt19937 rng(12345);
  std::uniform_real_distribution<float> dense_dist(0.0f, 2.0f);
  std::uniform_real_distribution<float> sparse_dist(2.0f, 10.0f);

  for (int i = 0; i < num_dense; ++i) {
    data[i][0] = dense_dist(rng);
    embeddings[i].float_vector = data[i].data();
  }
  for (int i = num_dense; i < num_points; ++i) {
    data[i][0] = sparse_dist(rng);
    embeddings[i].float_vector = data[i].data();
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(num_points / 2);  // Target 50
  options.set_mini_batch_size(20);
  options.set_min_num_iterations(50);
  options.set_random_seed(1234);

  auto get_cluster_counts = [](const std::vector<int>& assignments,
                               int k) -> std::vector<int> {
    std::vector<int> counts(k, 0);
    for (int c : assignments) {
      if (c >= 0 && c < k) {
        counts[c]++;
      }
    }
    return counts;
  };

  auto get_imbalance = [](const std::vector<int>& counts, int target) -> int {
    int imbalance = 0;
    for (int c : counts) {
      imbalance += std::abs(c - target);
    }
    return imbalance;
  };

  // 1. Run WITHOUT balance penalty
  options.set_balance_penalty(0.0f);
  ICING_ASSERT_OK_AND_ASSIGN(
      auto result_unbalanced,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  // Verify unbalanced behavior
  // Dense region [0,2] mean ~1. Sparse [2,10] mean ~6.
  // Split ~3.5.
  // Points < 3.5: All 80 dense + ~4 sparse = ~84.
  // Points > 3.5: ~16 sparse.
  std::vector<int> counts_unbalanced =
      get_cluster_counts(result_unbalanced.partition_assignments, 2);
  int max_count =
      *std::max_element(counts_unbalanced.begin(), counts_unbalanced.end());

  // Expect significant imbalance (one cluster > 70)
  EXPECT_GT(max_count, 70);

  int imbalance_unbalanced = get_imbalance(
      counts_unbalanced, static_cast<int>(options.target_cluster_size()));

  // 2. Run WITH balance penalty
  options.set_balance_penalty(1000.0f);
  ICING_ASSERT_OK_AND_ASSIGN(
      auto result_balanced,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  std::vector<int> counts_balanced =
      get_cluster_counts(result_balanced.partition_assignments, 2);
  int imbalance_balanced = get_imbalance(
      counts_balanced, static_cast<int>(options.target_cluster_size()));

  // The balanced version should reduce the count of the largest cluster
  // by shifting the boundary into the dense region.
  int max_count_balanced =
      *std::max_element(counts_balanced.begin(), counts_balanced.end());

  EXPECT_LT(max_count_balanced, max_count);
  EXPECT_LT(imbalance_balanced, imbalance_unbalanced);
}

TEST_F(MiniBatchKMeansTest, LargeScaleClustering) {
  int num_points = 1000;
  int dimension = 10;
  int num_clusters = 5;
  auto data = GenerateData(num_points, dimension, num_clusters, /*seed=*/999);

  std::vector<EmbeddingReference> embeddings(num_points);
  for (int i = 0; i < num_points; ++i) {
    embeddings[i].float_vector = data[i].data();
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(num_points / num_clusters);
  options.set_mini_batch_size(100);
  options.set_min_num_iterations(50);
  options.set_random_seed(999);
  options.set_balance_penalty(0.0f);

  ICING_ASSERT_OK_AND_ASSIGN(
      auto result,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  EXPECT_THAT(result.centroids, SizeIs(num_clusters));

  // Verify structural validity of the assignments. Although we do not validate
  // against ground truth labels, we ensure basic sanity: valid cluster indices
  // and no empty clusters.
  std::vector<int> counts(num_clusters, 0);
  for (int c : result.partition_assignments) {
    ASSERT_GE(c, 0);
    ASSERT_LT(c, num_clusters);
    counts[c]++;
  }

  for (int c = 0; c < num_clusters; ++c) {
    EXPECT_GT(counts[c], 0);  // Should have at least some points
  }
}

TEST_F(MiniBatchKMeansTest, SingleCluster) {
  int num_points = 10;
  int dimension = 2;
  auto data =
      GenerateData(num_points, dimension, /*num_clusters=*/1, /*seed=*/111);

  std::vector<EmbeddingReference> embeddings(num_points);
  for (int i = 0; i < num_points; ++i) {
    embeddings[i].float_vector = data[i].data();
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(100);  // > num_points, so k=1
  options.set_mini_batch_size(5);
  options.set_min_num_iterations(5);

  ICING_ASSERT_OK_AND_ASSIGN(
      auto result,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  EXPECT_THAT(result.centroids, SizeIs(1));
  for (int c : result.partition_assignments) {
    EXPECT_EQ(c, 0);
  }
}

TEST_F(MiniBatchKMeansTest, WithSamplingRatio) {
  std::vector<float> data = {0.1f, 0.1f, 0.2f,  0.2f,  -0.1f, -0.1f,
                             9.9f, 9.9f, 10.1f, 10.1f, 10.0f, 10.0f};
  int dimension = 2;
  int num_points = 6;

  std::vector<EmbeddingReference> embeddings(num_points);
  for (int i = 0; i < num_points; ++i) {
    embeddings[i].float_vector = &data[i * dimension];
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(3);
  options.set_mini_batch_size(2);
  options.set_min_num_iterations(5);
  options.set_sampling_ratio(2.0f);  // 6 * 2 / 2 = 6 iterations > 5
  options.set_random_seed(123);

  ICING_ASSERT_OK_AND_ASSIGN(
      auto result,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &clock_));

  EXPECT_THAT(result.centroids, SizeIs(2));
  EXPECT_THAT(result.partition_assignments, SizeIs(num_points));
}

TEST_F(MiniBatchKMeansTest, TimeoutTest) {
  std::vector<float> data = {0.1f, 0.1f, 0.2f,  0.2f,  -0.1f, -0.1f,
                             9.9f, 9.9f, 10.1f, 10.1f, 10.0f, 10.0f};
  int dimension = 2;
  int num_points = 6;

  std::vector<EmbeddingReference> embeddings(num_points);
  for (int i = 0; i < num_points; ++i) {
    embeddings[i].float_vector = &data[i * dimension];
  }

  MiniBatchKMeansOptions options;
  options.set_target_cluster_size(3);
  options.set_mini_batch_size(2);
  options.set_min_num_iterations(10);
  options.set_timeout_ms(5);
  options.set_random_seed(123);

  FakeClock fake_clock;
  fake_clock.SetTimerElapsedMilliseconds(10);

  ICING_ASSERT_OK_AND_ASSIGN(
      auto result,
      MiniBatchKMeans::Compute(embeddings, dimension, options, &fake_clock));

  // We expect it to stop before starting any iterations because the timer
  // already exceeds the timeout.
  EXPECT_EQ(result.actual_iterations, 0);
}

}  // namespace
}  // namespace lib
}  // namespace icing
