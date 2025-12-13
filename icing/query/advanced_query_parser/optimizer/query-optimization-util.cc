// Copyright (C) 2025 Google LLC
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

#include "icing/query/advanced_query_parser/optimizer/query-optimization-util.h"

#include <memory>
#include <utility>
#include <vector>

#include "icing/feature-flags.h"
#include "icing/index/embed/doc-hit-info-iterator-embedding-v2.h"
#include "icing/index/iterator/doc-hit-info-iterator-and.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"

namespace icing {
namespace lib {
namespace query_optimization_util {

std::unique_ptr<DocHitInfoIterator> OptimizeAndIteratorsIfPossible(
    std::vector<std::unique_ptr<DocHitInfoIterator>>&& iterators,
    const FeatureFlags& feature_flags) {
  std::unique_ptr<DocHitInfoIterator> embed_iterator;
  DocHitInfoIteratorEmbeddingV2* embed_iterator_ptr = nullptr;
  bool delegate_node_is_right_most = true;
  if (feature_flags.enable_embed_query_optimization()) {
    // Find the first embedding iterator and remove it from the vector.
    int embed_iterator_index = 0;
    for (int i = embed_iterator_index; embed_iterator_index < iterators.size();
         ++embed_iterator_index) {
      if ((embed_iterator_ptr = dynamic_cast<DocHitInfoIteratorEmbeddingV2*>(
               iterators.at(i).get())) != nullptr) {
        embed_iterator = std::move(iterators.at(i));
        if (i == iterators.size() - 1) {
          // If this embedding iterator is the last iterator, then the node is
          // the right most node and the delegate that we're going to create
          // would not be right most.
          delegate_node_is_right_most = false;
        }
        iterators.erase(iterators.begin() + i);
        break;
      }
    }
  }
  // If we found an embedding iterator, then put all other iterators into an
  // AND iterator together and pass it to this embedding iterator as a delegate.
  // Otherwise, just return the original AND iterator.
  std::unique_ptr<DocHitInfoIterator> and_iterator =
      CreateAndIterator(std::move(iterators));
  if (embed_iterator == nullptr) {
    return and_iterator;
  }
  embed_iterator_ptr->AdoptDelegate(std::move(and_iterator),
                                    delegate_node_is_right_most);
  return embed_iterator;
}

}  // namespace query_optimization_util
}  // namespace lib
}  // namespace icing
