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

#ifndef ICING_REVERSE_VECTOR_NO_RANKER_H_
#define ICING_REVERSE_VECTOR_NO_RANKER_H_

#include <algorithm>
#include <memory>
#include <optional>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// ScoredDocumentHitsRanker interface implementation, based on std::vector.
// ReverseVectorNoRanker DOES NOT re-rank the data. Instead, it returns hits in
// the reverse order of the input vector (i.e. starts from the back).
//
// This class is used for:
// - No ranking/scoring use case.
// - ResultStateV2 cache eviction serialization: usually we use priority queue
//   as the ranker for retrieval, but when serializing the state for cache
//   eviction, we don't want to serialize the priority queue. Instead, we pop
//   all hits from the priority queue and serialize them into a list.
//   Afterwards, all hits have been ranked, so we only need to use
//   ReverseVectorNoRanker after deserialization.
// - ResultStateV2 optimization: same as above, we need to pop all hits from the
//   priority queue and convert the ids from old to new. After popping and
//   conversion, all hits have been ranked, so we only need to use
//   ReverseVectorNoRanker.
template <typename ScoredDataType,
          typename Converter = typename ScoredDataType::Converter>
class ReverseVectorNoRanker : public ScoredDocumentHitsRanker {
 public:
  // Constructs a ReverseVectorNoRanker.
  explicit ReverseVectorNoRanker(std::vector<ScoredDataType>&& scored_data);

  ~ReverseVectorNoRanker() override = default;

  void Pop() override;

  // Note: ranker may store ScoredDocumentHit or JoinedScoredDocumentHit, so we
  // have template for scored_data_.
  // - JoinedScoredDocumentHit is a superset of ScoredDocumentHit, so we unify
  //   the return type of Top to use the superset type JoinedScoredDocumentHit
  //   in order to make it simple, and rankers storing ScoredDocumentHit should
  //   convert it to JoinedScoredDocumentHit before returning. It makes the
  //   implementation simpler, especially for ResultRetriever, which now only
  //   needs to deal with one single return format.
  // - JoinedScoredDocumentHit has ~2x size of ScoredDocumentHit. Since we cache
  //   ranker (which contains a vector of data) in ResultState, if we store the
  //   scored hits in JoinedScoredDocumentHit format directly, then it doubles
  //   the memory usage. Therefore, we still keep the flexibility to store
  //   ScoredDocumentHit or any other types of data, but require Pop to convert
  //   it to JoinedScoredDocumentHit and cache it in curr_.
  const JoinedScoredDocumentHit& Top() const override { return *curr_; }

  // Truncates the remaining ScoredDocumentHits to the given size. The best
  // ScoredDocumentHits (according to the ranking policy) should be kept.
  // If new_size is invalid (< 0), or greater or equal to # of remaining
  // ScoredDocumentHits, then no action will be taken. Otherwise truncates the
  // the remaining ScoredDocumentHits to the given size.
  void TruncateHitsTo(int new_size) override;

  std::unique_ptr<ScoredDocumentHitsRanker> OptimizeAndTransfer(
      const std::vector<DocumentId>& document_id_old_to_new) &&
      override;

  // Returns DocumentIds of the top K documents according to the ranking policy.
  // - For ScoredDocumentHit, this returns the DocumentIds of the top K
  //   documents.
  // - For JoinedScoredDocumentHit, this returns the DocumentIds of the top K
  //   parent documents.
  std::unordered_set<DocumentId> GetTopKDocumentIds(int k) const override;

  // Returns the DocumentIds of the top K child documents for each
  // JoinedScoredDocumentHit.
  // - For ScoredDocumentHit, this returns an empty set.
  std::unordered_set<DocumentId> GetTopKChildDocumentIds(int k) const override;

  int size() const override { return scored_data_.size(); }

  bool empty() const override { return curr_ == nullptr; }

  void clear() override {
    scored_data_.clear();
    curr_ = nullptr;
  }

 private:
  // Helper function to refresh the current element (fetch the top element from
  // the back of the vector, convert it to JoinedScoredDocumentHit, and cache it
  // in curr_).
  void RefreshCurrent();

  // Use vector to store the hits and get top K hits in O(K) time.
  std::vector<ScoredDataType> scored_data_;

  Converter converter_;

  std::unique_ptr<JoinedScoredDocumentHit> curr_;
};

template <typename ScoredDataType, typename Converter>
ReverseVectorNoRanker<ScoredDataType, Converter>::ReverseVectorNoRanker(
    std::vector<ScoredDataType>&& scored_data)
    : scored_data_(std::move(scored_data)) {
  RefreshCurrent();
}

template <typename ScoredDataType, typename Converter>
void ReverseVectorNoRanker<ScoredDataType, Converter>::Pop() {
  scored_data_.pop_back();
  RefreshCurrent();
}

template <typename ScoredDataType, typename Converter>
void ReverseVectorNoRanker<ScoredDataType, Converter>::TruncateHitsTo(
    int new_size) {
  if (new_size < 0 || scored_data_.size() <= new_size) {
    return;
  }

  // Keep [scored_data_.end() - new_size, scored_data_.end()] to preserve the
  // last new_size elements, and erase the rest from the beginning.
  scored_data_.erase(scored_data_.begin(), scored_data_.end() - new_size);
  scored_data_.shrink_to_fit();
  RefreshCurrent();
}

template <typename ScoredDataType, typename Converter>
std::unique_ptr<ScoredDocumentHitsRanker>
ReverseVectorNoRanker<ScoredDataType, Converter>::OptimizeAndTransfer(
    const std::vector<DocumentId>& document_id_old_to_new) && {
  std::vector<ScoredDataType> optimized_scored_data_vec;
  optimized_scored_data_vec.reserve(scored_data_.size());
  for (ScoredDataType& scored_data : scored_data_) {
    std::optional<ScoredDataType> converted_scored_data =
        std::move(scored_data).Optimize(document_id_old_to_new);
    if (converted_scored_data.has_value()) {
      optimized_scored_data_vec.push_back(std::move(*converted_scored_data));
    }
  }
  optimized_scored_data_vec.shrink_to_fit();

  return std::make_unique<ReverseVectorNoRanker<ScoredDataType, Converter>>(
      std::move(optimized_scored_data_vec));
}

template <typename ScoredDataType, typename Converter>
std::unordered_set<DocumentId>
ReverseVectorNoRanker<ScoredDataType, Converter>::GetTopKDocumentIds(
    int k) const {
  std::unordered_set<DocumentId> top_k_document_ids;
  if (k <= 0) {
    return top_k_document_ids;
  }

  top_k_document_ids.reserve(k);
  for (int i = std::max(0, static_cast<int>(scored_data_.size()) - k);
       i < scored_data_.size(); ++i) {
    const ScoredDataType& next_scored_data = scored_data_[i];
    if constexpr (std::is_same_v<ScoredDataType, ScoredDocumentHit>) {
      top_k_document_ids.insert(next_scored_data.document_id());
    } else if constexpr (std::is_same_v<ScoredDataType,
                                        JoinedScoredDocumentHit>) {
      top_k_document_ids.insert(
          next_scored_data.parent_scored_document_hit().document_id());
    } else {
      // Returns an empty set if the ScoredDataType is not
      // JoinedScoredDocumentHit or ScoredDocumentHit.
      return top_k_document_ids;
    }
  }
  return top_k_document_ids;
}

template <typename ScoredDataType, typename Converter>
std::unordered_set<DocumentId>
ReverseVectorNoRanker<ScoredDataType, Converter>::GetTopKChildDocumentIds(
    int k) const {
  std::unordered_set<DocumentId> top_k_document_ids;
  if (k <= 0) {
    return top_k_document_ids;
  }

  if constexpr (std::is_same_v<ScoredDataType, ScoredDocumentHit>) {
    return top_k_document_ids;
  } else if constexpr (std::is_same_v<ScoredDataType,
                                      JoinedScoredDocumentHit>) {
    for (const ScoredDataType& scored_data : scored_data_) {
      const std::vector<ScoredDocumentHit>& child_scored_document_hits =
          scored_data.child_scored_document_hits();
      for (int i = 0; i < k && i < child_scored_document_hits.size(); ++i) {
        top_k_document_ids.insert(child_scored_document_hits[i].document_id());
      }
    }
  } else {
    // Returns an empty set if the ScoredDataType is not JoinedScoredDocumentHit
    // or ScoredDocumentHit.
    return top_k_document_ids;
  }
  return top_k_document_ids;
}

template <typename ScoredDataType, typename Converter>
void ReverseVectorNoRanker<ScoredDataType, Converter>::RefreshCurrent() {
  if (scored_data_.empty()) {
    curr_ = nullptr;
  } else {
    ScoredDataType scored_data = scored_data_.back();

    if (curr_ == nullptr) {
      curr_ = std::make_unique<JoinedScoredDocumentHit>(
          converter_(std::move(scored_data)));
    } else {
      *curr_ = converter_(std::move(scored_data));
    }
  }
}

}  // namespace lib
}  // namespace icing

#endif  // ICING_REVERSE_VECTOR_NO_RANKER_H_
