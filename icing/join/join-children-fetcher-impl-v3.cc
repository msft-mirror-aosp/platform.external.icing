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

#include "icing/join/join-children-fetcher-impl-v3.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<JoinChildrenFetcherImplV3>>
JoinChildrenFetcherImplV3::Create(
    const JoinSpecProto& join_spec, const SchemaStore* schema_store,
    const DocumentStore* doc_store,
    const QualifiedIdJoinIndex* qualified_id_join_index,
    int64_t current_time_ms,
    std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(doc_store);
  ICING_RETURN_ERROR_IF_NULL(qualified_id_join_index);

  // TODO(b/256022027): So far we only support kQualifiedIdExpr for
  // parent_property_expression, we could support more.
  if (join_spec.parent_property_expression() != kQualifiedIdExpr) {
    return absl_ports::UnimplementedError(absl_ports::StrCat(
        "Parent property expression must be ", kQualifiedIdExpr));
  }

  if (qualified_id_join_index->version() !=
      QualifiedIdJoinIndex::Version::kV3) {
    return absl_ports::InvalidArgumentError(
        "QualifiedIdJoinIndex version must be v3 with "
        "JoinChildrenFetcherImplV3.");
  }

  // For each child scored document hit, we need to find the joinable property
  // id that matches the joinable property specified in the join spec.
  std::unordered_map<DocumentJoinIdPair, ScoredDocumentHit,
                     DocumentJoinIdPair::Hasher>
      child_join_id_pair_to_scored_document_hit_map;
  for (ScoredDocumentHit& child_scored_document_hit :
       child_scored_document_hits) {
    DocumentId child_doc_id = child_scored_document_hit.document_id();

    // Get the document filter data for the child document.
    std::optional<DocumentFilterData> child_doc_filter_data =
        doc_store->GetAliveDocumentFilterData(child_doc_id, current_time_ms);
    if (!child_doc_filter_data) {
      continue;
    }

    // Get the joinable property metadata.
    ICING_ASSIGN_OR_RETURN(const JoinablePropertyMetadata* metadata,
                           schema_store->GetJoinablePropertyMetadata(
                               child_doc_filter_data->schema_type_id(),
                               join_spec.child_property_expression()));
    if (metadata == nullptr) {
      continue;
    }

    child_join_id_pair_to_scored_document_hit_map.insert(
        {DocumentJoinIdPair(child_doc_id, metadata->id),
         std::move(child_scored_document_hit)});
  }

  return std::unique_ptr<JoinChildrenFetcherImplV3>(
      new JoinChildrenFetcherImplV3(
          join_spec, qualified_id_join_index,
          std::move(child_join_id_pair_to_scored_document_hit_map)));
}

libtextclassifier3::StatusOr<std::vector<ScoredDocumentHit>>
JoinChildrenFetcherImplV3::GetChildren(DocumentId parent_doc_id) const {
  // If the parent_doc_id and its children are already in the cache, return the
  // cached value.
  if (auto cache_itr = cached_parent_to_children_map_.find(parent_doc_id);
      cache_itr != cached_parent_to_children_map_.end()) {
    return cache_itr->second;
  }

  // Otherwise, fetch the children from the qualified id join index and cache
  // the result.
  ICING_ASSIGN_OR_RETURN(
      std::vector<DocumentJoinIdPair> child_document_join_id_pairs,
      qualified_id_join_index_.Get(parent_doc_id));

  // Filter and construct child_scored_document_hits for the given parent doc
  // id.
  // - child_join_id_pair_to_scored_document_hit_map_ contains (DocumentId,
  //   JoinablePropertyId) of the child documents in the child result set and
  //   the desired joinable property id which matches the joinable property
  //   specified in the join spec.
  // - The DocumentJoinIdPair list fetched from the join index may contain child
  //   documents that are not in the child result set, or the joinable property
  //   id is not the desired one. For example:
  //   - Parent document 1 may have 4 child DocumentJoinIdPairs:
  //     - (doc_id=100, joinable_property_id=1)
  //     - (doc_id=100, joinable_property_id=2)
  //     - (doc_id=101, joinable_property_id=1)
  //     - (doc_id=102, joinable_property_id=1)
  //   - All of these 4 pairs will be fetched from the join index.
  //   - Now, the child query obtains result doc ids = [100, 102, 103], and the
  //     client wants to join the child document by the joinable property "foo",
  //     which has joinable property id 1 in its schema.
  //   - In this case, when handling parent document 1, we should filter out
  //     children [(100, 2), (101, 1)] since they are not in the child result
  //     set, or the joinable property does not match the one specified in the
  //     join spec.
  std::vector<ScoredDocumentHit> child_scored_document_hits;
  for (const DocumentJoinIdPair& child_document_join_id_pair :
       child_document_join_id_pairs) {
    if (auto filter_itr = child_join_id_pair_to_scored_document_hit_map_.find(
            child_document_join_id_pair);
        filter_itr != child_join_id_pair_to_scored_document_hit_map_.end()) {
      child_scored_document_hits.push_back(filter_itr->second);
    }
  }

  // Sort child_scored_document_hits according to the child scoring spec.
  ScoredDocumentHitComparator score_comparator(
      /*is_descending=*/join_spec_.nested_spec().scoring_spec().order_by() ==
      ScoringSpecProto::Order::DESC);
  std::sort(child_scored_document_hits.begin(),
            child_scored_document_hits.end(), score_comparator);

  cached_parent_to_children_map_.insert(
      {parent_doc_id, child_scored_document_hits});

  return child_scored_document_hits;
}

}  // namespace lib
}  // namespace icing
