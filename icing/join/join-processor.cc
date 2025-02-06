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

#include "icing/join/join-processor.h"

#include <algorithm>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/join/aggregation-scorer.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/join-children-fetcher-impl-deprecated.h"
#include "icing/join/join-children-fetcher-impl-v3.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/join/qualified-id.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/joinable-property.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/namespace-id-fingerprint.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<JoinChildrenFetcher>>
JoinProcessor::GetChildrenFetcher(
    const JoinSpecProto& join_spec,
    std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
  if (join_spec.parent_property_expression() != kQualifiedIdExpr) {
    // TODO(b/256022027): So far we only support kQualifiedIdExpr for
    // parent_property_expression, we could support more.
    return absl_ports::UnimplementedError(absl_ports::StrCat(
        "Parent property expression must be ", kQualifiedIdExpr));
  }

  switch (qualified_id_join_index_->version()) {
    case QualifiedIdJoinIndex::Version::kV2:
      return GetChildrenFetcherV2(join_spec,
                                  std::move(child_scored_document_hits));
    case QualifiedIdJoinIndex::Version::kV3:
      return JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_, doc_store_, qualified_id_join_index_,
          current_time_ms_, std::move(child_scored_document_hits));
  }
}

libtextclassifier3::StatusOr<std::unique_ptr<JoinChildrenFetcher>>
JoinProcessor::GetChildrenFetcherV2(
    const JoinSpecProto& join_spec,
    std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
  // Step 1a: sort child ScoredDocumentHits in document id descending order.
  std::sort(child_scored_document_hits.begin(),
            child_scored_document_hits.end(),
            [](const ScoredDocumentHit& lhs, const ScoredDocumentHit& rhs) {
              return lhs.document_id() > rhs.document_id();
            });

  // Step 1b: group all child ScoredDocumentHits by the document's
  //          schema_type_id.
  std::unordered_map<SchemaTypeId, std::vector<ScoredDocumentHit>>
      schema_to_child_scored_doc_hits_map;
  for (const ScoredDocumentHit& child_scored_document_hit :
       child_scored_document_hits) {
    std::optional<DocumentFilterData> child_doc_filter_data =
        doc_store_->GetAliveDocumentFilterData(
            child_scored_document_hit.document_id(), current_time_ms_);
    if (!child_doc_filter_data) {
      continue;
    }

    schema_to_child_scored_doc_hits_map[child_doc_filter_data->schema_type_id()]
        .push_back(child_scored_document_hit);
  }

  // Step 1c: for each schema_type_id, lookup QualifiedIdJoinIndexImplV2 to
  //          fetch all child join data from posting list(s). Convert all
  //          child join data to referenced parent document ids and bucketize
  //          child ScoredDocumentHits by it.
  std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      parent_to_child_docs_map;
  for (auto& [schema_type_id, grouped_child_scored_doc_hits] :
       schema_to_child_scored_doc_hits_map) {
    // Get joinable_property_id of this schema.
    ICING_ASSIGN_OR_RETURN(
        const JoinablePropertyMetadata* metadata,
        schema_store_->GetJoinablePropertyMetadata(
            schema_type_id, join_spec.child_property_expression()));
    if (metadata == nullptr ||
        metadata->value_type != JoinableConfig::ValueType::QUALIFIED_ID) {
      // Currently we only support qualified id, so skip other types.
      continue;
    }

    // Lookup QualifiedIdJoinIndexImplV2.
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<QualifiedIdJoinIndex::JoinDataIteratorBase>
            join_index_iter,
        qualified_id_join_index_->GetIterator(
            schema_type_id, /*joinable_property_id=*/metadata->id));

    // - Join index contains all join data of schema_type_id and
    //   join_index_iter will return all of them in (child) document id
    //   descending order.
    // - But we only need join data of child document ids which appear in
    //   grouped_child_scored_doc_hits. Also grouped_child_scored_doc_hits
    //   contain ScoredDocumentHits in (child) document id descending order.
    // - Therefore, we advance 2 iterators to intersect them and get desired
    //   join data.
    auto child_scored_doc_hits_iter = grouped_child_scored_doc_hits.cbegin();
    while (join_index_iter->Advance().ok() &&
           child_scored_doc_hits_iter != grouped_child_scored_doc_hits.cend()) {
      // Advance child_scored_doc_hits_iter until it points to a
      // ScoredDocumentHit with document id <= the one pointed by
      // join_index_iter.
      while (child_scored_doc_hits_iter !=
                 grouped_child_scored_doc_hits.cend() &&
             child_scored_doc_hits_iter->document_id() >
                 join_index_iter->GetCurrent().document_id()) {
        ++child_scored_doc_hits_iter;
      }

      if (child_scored_doc_hits_iter != grouped_child_scored_doc_hits.cend() &&
          child_scored_doc_hits_iter->document_id() ==
              join_index_iter->GetCurrent().document_id()) {
        // We get a join data whose child document id exists in both join
        // index and grouped_child_scored_doc_hits. Convert its join info to
        // referenced parent document ids and bucketize ScoredDocumentHits by
        // it (putting into parent_to_child_docs_map).
        const NamespaceIdFingerprint& ref_doc_nsid_uri_fingerprint =
            join_index_iter->GetCurrent().join_info();
        libtextclassifier3::StatusOr<DocumentId> ref_parent_doc_id_or =
            doc_store_->GetDocumentId(ref_doc_nsid_uri_fingerprint);
        if (ref_parent_doc_id_or.ok()) {
          parent_to_child_docs_map[std::move(ref_parent_doc_id_or).ValueOrDie()]
              .push_back(*child_scored_doc_hits_iter);
        }
      }
    }
  }

  // Step 1d: finally, sort each parent's joined child ScoredDocumentHits by
  //          score.
  ScoredDocumentHitComparator score_comparator(
      /*is_descending=*/join_spec.nested_spec().scoring_spec().order_by() ==
      ScoringSpecProto::Order::DESC);
  for (auto& [parent_doc_id, bucketized_child_scored_hits] :
       parent_to_child_docs_map) {
    std::sort(bucketized_child_scored_hits.begin(),
              bucketized_child_scored_hits.end(), score_comparator);
  }

  return JoinChildrenFetcherImplDeprecated::Create(
      join_spec, std::move(parent_to_child_docs_map));
}

libtextclassifier3::StatusOr<std::vector<JoinedScoredDocumentHit>>
JoinProcessor::Join(
    const JoinSpecProto& join_spec,
    std::vector<ScoredDocumentHit>&& parent_scored_document_hits,
    const JoinChildrenFetcher& join_children_fetcher) {
  std::unique_ptr<AggregationScorer> aggregation_scorer =
      AggregationScorer::Create(join_spec);

  std::vector<JoinedScoredDocumentHit> joined_scored_document_hits;
  joined_scored_document_hits.reserve(parent_scored_document_hits.size());

  // Step 2: iterate through all parent documentIds and construct
  //         JoinedScoredDocumentHit for each by looking up
  //         join_children_fetcher.
  for (ScoredDocumentHit& parent : parent_scored_document_hits) {
    ICING_ASSIGN_OR_RETURN(
        std::vector<ScoredDocumentHit> children,
        join_children_fetcher.GetChildren(parent.document_id()));

    double final_score = aggregation_scorer->GetScore(parent, children);
    joined_scored_document_hits.emplace_back(final_score, std::move(parent),
                                             std::move(children));
  }

  return joined_scored_document_hits;
}

libtextclassifier3::StatusOr<std::unordered_set<DocumentId>>
JoinProcessor::GetPropagatedChildDocumentsToDelete(
    const std::unordered_set<DocumentId>& deleted_document_ids) {
  // Sanity check: join index should be V3.
  if (qualified_id_join_index_->version() !=
      QualifiedIdJoinIndex::Version::kV3) {
    return absl_ports::UnimplementedError(
        "QualifiedIdJoinIndex version must be V3 to support delete "
        "propagation.");
  }

  // BFS traverse to find all child documents to propagate delete.
  std::queue<DocumentId> que(
      std::deque(deleted_document_ids.begin(), deleted_document_ids.end()));
  std::unordered_set<DocumentId> child_documents_to_delete;
  while (!que.empty()) {
    DocumentId doc_id_to_expand = que.front();
    que.pop();

    ICING_ASSIGN_OR_RETURN(std::vector<DocumentJoinIdPair> child_join_id_pairs,
                           qualified_id_join_index_->Get(doc_id_to_expand));
    for (const DocumentJoinIdPair& child_join_id_pair : child_join_id_pairs) {
      if (child_documents_to_delete.find(child_join_id_pair.document_id()) !=
              child_documents_to_delete.end() ||
          deleted_document_ids.find(child_join_id_pair.document_id()) !=
              deleted_document_ids.end()) {
        // Already added into the set to delete or already deleted (happens only
        // when there is a cycle back to the deleted or traversed document in
        // the join relation). Skip it.
        continue;
      }

      // Get DocumentFilterData of the child document to look up its schema type
      // id.
      // - Skip if the child document has been deleted, since delete propagation
      //   should've been done to all its children when deleting it previously.
      // - Otherwise, we have to handle this child document and propagate delete
      //   to the grandchildren, even if it is expired.
      std::optional<DocumentFilterData> child_filter_data =
          doc_store_->GetNonDeletedDocumentFilterData(
              child_join_id_pair.document_id());
      if (!child_filter_data) {
        // The child document has been deleted. Skip.
        continue;
      }

      libtextclassifier3::StatusOr<const JoinablePropertyMetadata*>
          metadata_or = schema_store_->GetJoinablePropertyMetadata(
              child_filter_data->schema_type_id(),
              child_join_id_pair.joinable_property_id());
      if (!metadata_or.ok() || metadata_or.ValueOrDie() == nullptr) {
        // This shouldn't happen because we've validated it during indexing and
        // only put valid DocumentJoinIdPair into qualified id join index.
        // Log and skip it.
        ICING_LOG(ERROR) << "Failed to get metadata for schema type id "
                         << child_filter_data->schema_type_id()
                         << ", joinable property id "
                         << static_cast<int>(
                                child_join_id_pair.joinable_property_id());
        continue;
      }
      const JoinablePropertyMetadata* metadata = metadata_or.ValueOrDie();

      if (metadata->value_type == JoinableConfig::ValueType::QUALIFIED_ID &&
          metadata->delete_propagation_type ==
              JoinableConfig::DeletePropagationType::PROPAGATE_FROM) {
        child_documents_to_delete.insert(child_join_id_pair.document_id());
        que.push(child_join_id_pair.document_id());
      }
    }
  }

  return child_documents_to_delete;
}

}  // namespace lib
}  // namespace icing
