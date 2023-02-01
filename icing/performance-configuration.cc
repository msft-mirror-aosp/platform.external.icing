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

#include "icing/performance-configuration.h"

#include "icing/result/result-state.h"

namespace icing {
namespace lib {

namespace {
// Search-related thresholds:
// Search performance mainly involves the following components:
// 1. QueryProcessor
//    Running time is O(query_length) according to results of
//    //icing/query:query-processor_benchmark.
// 2. ScoringProcessor and Ranker
//    Running time is O(num_to_score) according to results of
//    //icing/scoring:score-and-rank_benchmark. Note that the
//    process includes scoring, building a heap, and popping results from the
//    heap.
// 3. ResultRetriever
//    Running time is O(page_size). Assuming page_size is always a small number,
//    it's actually O(1).
//
// Overall Search performance goal: 33 ms. On a 60FPS screen it's the time of
// rendering 2 frames.
//
// With the information above, we then try to choose default values for
// query_length and num_to_score so that the overall time can comfortably fit
// in with our goal.
// 1. Set query_length to 23000 so that any query can be executed by
//    QueryProcessor within 15 ms on a Pixel 3 XL according to results of
//    //icing/query:query-processor_benchmark.
// 2. Set num_to_score to 30000 so that results can be scored and ranked within
//    3 ms on a Pixel 3 XL according to results of
//    //icing/scoring:score-and-rank_benchmark.
//
// In the worse-case scenario, we still have [33 ms - 15 ms - 3 ms] = 15 ms left
// for all the other things like proto parsing, document fetching, and even
// Android Binder calls if Icing search engine runs in a separate process.
constexpr int kMaxQueryLength = 23000;
constexpr int kDefaultNumToScore = 30000;

// New Android devices nowadays all allow more than 16 MB memory per app. Using
// that as a guideline, we set 16 MB as the safe memory threshold.
// TODO(b/150029642): Android apps / framework have better understanding of how
// much memory is allowed, so it would be better to let clients pass in this
// value.
constexpr int kSafeMemoryUsage = 16 * 1024 * 1024;  // 16MB

// This number is not determined by benchmarks. We just assume that returning
// the best 1000 scored document hits of a query is enough. To find the best
// 1000 scored document hits from a heap, we need roughly 0.7 ms on a Pixel 3 XL
// according to //icing/scoring:ranker_benchmark.
constexpr int kMaxNumHitsPerQuery = 1000;

// A rough estimation of the size of ResultState if it stores the maximum number
// of scored document hits.
constexpr int kMaxMemoryPerResult =
    sizeof(ResultState) + kMaxNumHitsPerQuery * sizeof(ScoredDocumentHit);

// To be safer, we assume that all the Results contain the maximum number of
// hits and only use half of the memory allowed.
constexpr int kDefaultNumResultsToCache =
    kSafeMemoryUsage / 2 / kMaxMemoryPerResult;

static_assert(
    kDefaultNumResultsToCache > 500,
    "Default number of results to cache has changed, please update and make "
    "sure it still meets our requirements.");
}  // namespace

PerformanceConfiguration::PerformanceConfiguration()
    : PerformanceConfiguration(kMaxQueryLength, kDefaultNumToScore,
                               kMaxNumHitsPerQuery, kDefaultNumResultsToCache) {
}

}  // namespace lib
}  // namespace icing
