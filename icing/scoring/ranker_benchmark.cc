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

#include <cstdlib>

#include "testing/base/public/benchmark.h"
#include "icing/scoring/ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {

namespace {
// Run on a Linux workstation:
//    $ blaze build -c opt --dynamic_mode=off --copt=-gmlt
//    //icing/scoring:ranker_benchmark
//
//    $ blaze-bin/icing/scoring/ranker_benchmark --benchmarks=all
//    --benchmark_memory_usage
//
// Run on an Android device:
//    $ blaze build --copt="-DGOOGLE_COMMANDLINEFLAGS_FULL_API=1"
//    --config=android_arm64 -c opt --dynamic_mode=off --copt=-gmlt
//    //icing/scoring:ranker_benchmark
//
//    $ adb push blaze-bin/icing/scoring/ranker_benchmark
//    /data/local/tmp/
//
//    $ adb shell /data/local/tmp/ranker_benchmark --benchmarks=all

void BM_GetTopN(benchmark::State& state) {
  int num_to_score = state.range(0);
  int num_to_return = state.range(1);

  std::vector<ScoredDocumentHit> scored_document_hits;
  uint seed = Clock().GetCurrentSeconds();
  for (int i = 0; i < num_to_score; i++) {
    int score = rand_r(&seed);
    scored_document_hits.emplace_back(/*document_id=*/0,
                                      /*hit_section_id_mask=*/0, score);
  }

  for (auto _ : state) {
    auto result =
        GetTopNFromScoredDocumentHits(scored_document_hits, num_to_return,
                                      /*is_descending=*/true);
  }
}
BENCHMARK(BM_GetTopN)
    ->ArgPair(1000, 10)  // (num_to_score, num_to_return)
    ->ArgPair(3000, 10)
    ->ArgPair(5000, 10)
    ->ArgPair(7000, 10)
    ->ArgPair(9000, 10)
    ->ArgPair(11000, 10)
    ->ArgPair(13000, 10)
    ->ArgPair(15000, 10)
    ->ArgPair(17000, 10)
    ->ArgPair(19000, 10)
    ->ArgPair(1000, 20)
    ->ArgPair(3000, 20)
    ->ArgPair(5000, 20)
    ->ArgPair(7000, 20)
    ->ArgPair(9000, 20)
    ->ArgPair(11000, 20)
    ->ArgPair(13000, 20)
    ->ArgPair(15000, 20)
    ->ArgPair(17000, 20)
    ->ArgPair(19000, 20)
    ->ArgPair(1000, 30)
    ->ArgPair(3000, 30)
    ->ArgPair(5000, 30)
    ->ArgPair(7000, 30)
    ->ArgPair(9000, 30)
    ->ArgPair(11000, 30)
    ->ArgPair(13000, 30)
    ->ArgPair(15000, 30)
    ->ArgPair(17000, 30)
    ->ArgPair(19000, 30);
}  // namespace

}  // namespace lib
}  // namespace icing
