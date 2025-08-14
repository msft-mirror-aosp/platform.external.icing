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

#ifndef THIRD_PARTY_ICING_RESULT_RETRIEVER_V2_H_
#define THIRD_PARTY_ICING_RESULT_RETRIEVER_V2_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/absl_ports/thread_annotations.h"
#include "third_party/icing/feature-flags.h"
#include "third_party/icing/proto/search.proto.h"
#include "third_party/icing/result/page-result.h"
#include "third_party/icing/result/result-state-v2.h"
#include "third_party/icing/result/snippet-retriever.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/scoring/scored-document-hit.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/tokenization/language-segmenter.h"
#include "third_party/icing/transform/normalizer.h"

namespace icing {
namespace lib {

class GroupResultLimiterV2 {
 public:
  GroupResultLimiterV2() {}

  virtual ~GroupResultLimiterV2() = default;

  // Gets the index of the group result limits for the given scored document
  // hit.
  //
  // Returns:
  //   - A valid index of the group result limits.
  //   - -1 indicating that there is no limit for the result document.
  //   - std::nullopt if the result document, its namespace, or its schema type
  //     is not present. The caller should exclude the document from the page.
  virtual std::optional<int> GetGroupResultLimitsIndex(
      const ScoredDocumentHit& scored_document_hit,
      const std::unordered_map<int32_t, int>& entry_id_group_id_map,
      const DocumentStore& document_store,
      ResultSpecProto::ResultGroupingType result_group_type,
      int64_t current_time_ms) const;
};

class ResultRetrieverV2 {
 public:
  // Factory function to create a ResultRetrieverV2 which does not take
  // ownership of any input components, and all pointers must refer to valid
  // objects that outlive the created ResultRetrieverV2 instance.
  //
  // Returns:
  //   A ResultRetrieverV2 on success
  //   FAILED_PRECONDITION on any null pointer input
  static libtextclassifier3::StatusOr<std::unique_ptr<ResultRetrieverV2>>
  Create(const DocumentStore* doc_store, const SchemaStore* schema_store,
         const LanguageSegmenter* language_segmenter,
         const Normalizer* normalizer, const FeatureFlags* feature_flags,
         std::unique_ptr<const GroupResultLimiterV2> group_result_limiter =
             std::make_unique<const GroupResultLimiterV2>());

  // Retrieves results (pairs of DocumentProtos and SnippetProtos) with the
  // given ResultState which holds document and snippet information. It pulls
  // out the next top rank documents from ResultState, retrieves the documents
  // from storage, updates ResultState, and finally wraps the result + other
  // information into PageResult. The expected number of documents to return is
  // min(max_results, num_per_page, the number of all scored document hits)
  // inside ResultState.
  //
  // The number of snippets to return is based on the total number of snippets
  // needed and number of snippets that have already been returned previously
  // for the same query. The order of results returned will be sorted by
  // scored_document_hit_comparator inside ResultState.
  //
  // An additional boolean value will be returned, indicating if ResultState has
  // remaining documents to be retrieved next round.
  //
  // All errors will be ignored. It will keep retrieving the next document and
  // valid documents will be included in PageResult.
  //
  // Returns:
  //   std::pair<PageResult, bool>
  std::pair<PageResult, bool> RetrieveNextPage(ResultStateV2& result_state,
                                               int32_t max_results,
                                               int64_t current_time_ms) const
      ICING_LOCKS_EXCLUDED(result_state.mutex);

 private:
  explicit ResultRetrieverV2(
      const DocumentStore* doc_store,
      std::unique_ptr<SnippetRetriever> snippet_retriever,
      std::unique_ptr<const GroupResultLimiterV2> group_result_limiter,
      const FeatureFlags* feature_flags)
      : doc_store_(*doc_store),
        snippet_retriever_(std::move(snippet_retriever)),
        group_result_limiter_(std::move(group_result_limiter)),
        feature_flags_(*feature_flags) {}

  // Helper function to construct a ResultProto by the next best document hit
  // from the scored document hits ranker.
  //
  // REQUIRES: !result_state.scored_document_hits_ranker.empty()
  struct RetrieveResult {
    // The constructed result proto. If std::nullopt, then the document should
    // be skipped.
    std::optional<SearchResultProto::ResultProto> result_proto;

    // The index of the group result limits for the result. The caller should
    // decrement the corresponding result limit in
    // result_state.group_result_limits after deciding to include the result
    // document in the page.
    // - It is guaranteed to be -1 or in the range of [0,
    //   result_state.group_result_limits.size() - 1]. If it is -1, then it
    //   means there is no limit for the result document.
    // - Only used when the proto is not std::nullopt.
    int group_result_limits_index;

    // Whether the (parent) document of the result has snippets. Only used when
    // the proto is not std::nullopt.
    bool has_parent_snippets;
  };

  RetrieveResult Retrieve(ResultStateV2& result_state,
                          int64_t current_time_ms) const
      ICING_EXCLUSIVE_LOCKS_REQUIRED(result_state.mutex);

  const DocumentStore& doc_store_;
  std::unique_ptr<SnippetRetriever> snippet_retriever_;
  const std::unique_ptr<const GroupResultLimiterV2> group_result_limiter_;

  const FeatureFlags& feature_flags_;
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_RESULT_RETRIEVER_V2_H_
