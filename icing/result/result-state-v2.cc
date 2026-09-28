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

#include "icing/result/result-state-v2.h"

#include <atomic>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/proto/search.pb.h"
#include "icing/result/result-adjustment-info.h"
#include "icing/result/result-utils.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"

namespace icing {
namespace lib {

ResultStateV2::ResultStateV2(
    std::unique_ptr<ScoredDocumentHitsRanker> scored_document_hits_ranker_in,
    std::unique_ptr<ResultAdjustmentInfo> parent_adjustment_info_in,
    std::unique_ptr<ResultAdjustmentInfo> child_adjustment_info_in,
    const ResultSpecProto& result_spec, const SchemaStore& schema_store,
    const DocumentStore& document_store)
    : scored_document_hits_ranker(std::move(scored_document_hits_ranker_in)),
      parent_adjustment_info(std::move(parent_adjustment_info_in)),
      child_adjustment_info(std::move(child_adjustment_info_in)),
      num_returned(0),
      num_per_page_(result_spec.num_per_page()),
      num_total_bytes_per_page_threshold_(
          result_spec.num_total_bytes_per_page_threshold()),
      max_joined_children_per_parent_to_return_(
          result_spec.max_joined_children_per_parent_to_return()),
      result_group_type_(result_spec.result_group_type()),
      num_total_hits_(nullptr) {
  group_result_limits.reserve(result_spec.result_groupings().size());
  for (const ResultSpecProto::ResultGrouping& result_grouping :
       result_spec.result_groupings()) {
    int new_group_index = static_cast<int>(group_result_limits.size());
    group_result_limits.push_back(result_grouping.max_results());
    for (const ResultSpecProto::ResultGrouping::Entry& entry :
         result_grouping.entry_groupings()) {
      std::optional<result_utils::ResultGroupingEntryId> entry_id =
          result_utils::EncodeResultGroupingEntryId(
              schema_store, document_store, result_group_type_,
              entry.namespace_(), entry.schema());
      if (!entry_id.has_value()) {
        continue;
      }
      entry_id_group_index_map.insert({*entry_id, new_group_index});
    }
  }
}

ResultStateV2::~ResultStateV2() {
  IncrementNumTotalHits(-1 * scored_document_hits_ranker->size());
}

libtextclassifier3::Status ResultStateV2::Optimize(
    const DocumentStore::OptimizeResult& optimize_result) {
  // Component 1: scored_document_hits_ranker.
  // Step 1: unregister num_total_hits_ before optimizing the ranker, to
  //   subtract the size from the registered num_total_hits_.
  std::atomic<int>* original_num_total_hits_ptr = num_total_hits_;
  RegisterNumTotalHits(nullptr);
  // Step 2: optimize the ranker and assign the new optimized ranker back to the
  //   class member.
  std::unique_ptr<ScoredDocumentHitsRanker> old_ranker =
      std::move(scored_document_hits_ranker);
  scored_document_hits_ranker =
      std::move(*old_ranker)
          .OptimizeAndTransfer(optimize_result.document_id_old_to_new);
  // Step 3: finally, register the original num_total_hits_ for the new ranker
  //   to add the new size back to num_total_hits_.
  RegisterNumTotalHits(original_num_total_hits_ptr);

  // Component 2: parent_adjustment_info and child_adjustment_info.
  if (parent_adjustment_info != nullptr) {
    parent_adjustment_info->Optimize(optimize_result);
  }
  if (child_adjustment_info != nullptr) {
    child_adjustment_info->Optimize(optimize_result);
  }

  // Component 3: entry_id_group_index_map.
  // - Key: old entry id encoded by ResultGroupingType, NamespaceId and
  //   SchemaTypeId.
  // - Value: group index (should be unchanged).
  //
  // We need to convert the old entry id by remapping NamespaceId (note:
  // SchemaTypeId is unchanged). If an old NamespaceId is deleted, then we just
  // skip this entry id.
  // - Keep the same group index for the remapped entry id even if some of the
  //   old entry ids are deleted.
  // - This will leave group_result_limits vector sparse after Optimize because
  //   some group indices will not be used anymore, but:
  //   - It's fine since the "sparseness" only appears in the old result states,
  //     and result states will eventually be invalidated after all pages are
  //     returned OR expired.
  //   - It is more convenient since there is no need to optimize (change index
  //     and id) group_result_limits vector.
  std::unordered_map<result_utils::ResultGroupingEntryId, int>
      new_entry_id_group_index_map;
  new_entry_id_group_index_map.reserve(entry_id_group_index_map.size());
  for (const auto& [old_entry_id, group_idx] : entry_id_group_index_map) {
    // Decode the old entry id and remap the NamespaceId.
    std::pair<NamespaceId, SchemaTypeId> id_pair =
        result_utils::DecodeResultGroupingEntryId(old_entry_id,
                                                  result_group_type_);
    id_pair.first =
        (id_pair.first >= 0 &&
         id_pair.first < optimize_result.namespace_id_old_to_new.size())
            ? optimize_result.namespace_id_old_to_new[id_pair.first]
            : kInvalidNamespaceId;

    // Encode the remapped entry id and insert into the new map.
    std::optional<result_utils::ResultGroupingEntryId> new_entry_id =
        result_utils::EncodeResultGroupingEntryId(
            result_group_type_, id_pair.first, id_pair.second);
    if (!new_entry_id.has_value()) {
      continue;
    }
    new_entry_id_group_index_map.insert({*new_entry_id, group_idx});
  }
  entry_id_group_index_map = std::move(new_entry_id_group_index_map);

  return libtextclassifier3::Status::OK;
}

void ResultStateV2::Clear() {
  // Unregister num_total_hits_ before clearing scored_document_hits_ranker.
  // This will decrement the num_total_hits_ by the size of
  // scored_document_hits_ranker.
  RegisterNumTotalHits(nullptr);

  group_result_limits.clear();
  entry_id_group_index_map.clear();
  child_adjustment_info.reset();
  parent_adjustment_info.reset();
  scored_document_hits_ranker->clear();
}

void ResultStateV2::RegisterNumTotalHits(std::atomic<int>* num_total_hits) {
  // Decrement the original num_total_hits_ before registering a new one.
  IncrementNumTotalHits(-1 * scored_document_hits_ranker->size());
  num_total_hits_ = num_total_hits;
  IncrementNumTotalHits(scored_document_hits_ranker->size());
}

void ResultStateV2::IncrementNumTotalHits(int increment_by) {
  if (num_total_hits_ != nullptr) {
    *num_total_hits_ += increment_by;
  }
}

}  // namespace lib
}  // namespace icing
