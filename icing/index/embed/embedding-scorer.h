// Copyright (C) 2024 Google LLC
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

#ifndef ICING_INDEX_EMBED_EMBEDDING_SCORER_H_
#define ICING_INDEX_EMBED_EMBEDDING_SCORER_H_

#include <memory>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

class EmbeddingScorer {
 public:
  static libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingScorer>> Create(
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type);
  virtual float Score(int dimension, const float* v1,
                      const float* v2) const = 0;

  virtual ~EmbeddingScorer() = default;
};

class CosineEmbeddingScorer : public EmbeddingScorer {
 public:
  float Score(int dimension, const float* v1, const float* v2) const override;
};

class DotProductEmbeddingScorer : public EmbeddingScorer {
 public:
  float Score(int dimension, const float* v1, const float* v2) const override;
};

class EuclideanDistanceEmbeddingScorer : public EmbeddingScorer {
 public:
  float Score(int dimension, const float* v1, const float* v2) const override;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_SCORER_H_
