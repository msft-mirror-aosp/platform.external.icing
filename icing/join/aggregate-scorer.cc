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

#include "icing/join/aggregate-scorer.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "icing/proto/search.pb.h"
#include "icing/scoring/scored-document-hit.h"

namespace icing {
namespace lib {

class MinAggregateScorer : public AggregateScorer {
 public:
  double GetScore(const ScoredDocumentHit& parent,
                  const std::vector<ScoredDocumentHit>& children) override {
    return std::min_element(children.begin(), children.end(),
                            [](const ScoredDocumentHit& lhs,
                               const ScoredDocumentHit& rhs) -> bool {
                              return lhs.score() < rhs.score();
                            })
        ->score();
  }
};

class MaxAggregateScorer : public AggregateScorer {
 public:
  double GetScore(const ScoredDocumentHit& parent,
                  const std::vector<ScoredDocumentHit>& children) override {
    return std::max_element(children.begin(), children.end(),
                            [](const ScoredDocumentHit& lhs,
                               const ScoredDocumentHit& rhs) -> bool {
                              return lhs.score() < rhs.score();
                            })
        ->score();
  }
};

class AverageAggregateScorer : public AggregateScorer {
 public:
  double GetScore(const ScoredDocumentHit& parent,
                  const std::vector<ScoredDocumentHit>& children) override {
    if (children.empty()) return 0.0;
    return std::reduce(
               children.begin(), children.end(), 0.0,
               [](const double& prev, const ScoredDocumentHit& item) -> double {
                 return prev + item.score();
               }) /
           children.size();
  }
};

class CountAggregateScorer : public AggregateScorer {
 public:
  double GetScore(const ScoredDocumentHit& parent,
                  const std::vector<ScoredDocumentHit>& children) override {
    return children.size();
  }
};

class SumAggregateScorer : public AggregateScorer {
 public:
  double GetScore(const ScoredDocumentHit& parent,
                  const std::vector<ScoredDocumentHit>& children) override {
    return std::reduce(
        children.begin(), children.end(), 0.0,
        [](const double& prev, const ScoredDocumentHit& item) -> double {
          return prev + item.score();
        });
  }
};

class DefaultAggregateScorer : public AggregateScorer {
 public:
  double GetScore(const ScoredDocumentHit& parent,
                  const std::vector<ScoredDocumentHit>& children) override {
    return parent.score();
  }
};

std::unique_ptr<AggregateScorer> AggregateScorer::Create(
    const JoinSpecProto& join_spec) {
  switch (join_spec.aggregation_score_strategy()) {
    case JoinSpecProto_AggregationScore_MIN:
      return std::make_unique<MinAggregateScorer>();
    case JoinSpecProto_AggregationScore_MAX:
      return std::make_unique<MaxAggregateScorer>();
    case JoinSpecProto_AggregationScore_COUNT:
      return std::make_unique<CountAggregateScorer>();
    case JoinSpecProto_AggregationScore_AVG:
      return std::make_unique<AverageAggregateScorer>();
    case JoinSpecProto_AggregationScore_SUM:
      return std::make_unique<SumAggregateScorer>();
    case JoinSpecProto_AggregationScore_UNDEFINED:
      [[fallthrough]];
    default:
      return std::make_unique<DefaultAggregateScorer>();
  }
}

}  // namespace lib
}  // namespace icing
