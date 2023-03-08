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
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/join/aggregation-scorer.h"
#include "icing/join/qualified-id.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/util/snippet-helpers.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<JoinChildrenFetcher>
JoinProcessor::GetChildrenFetcher(
    const JoinSpecProto& join_spec,
    std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
  if (join_spec.parent_property_expression() != kQualifiedIdExpr) {
    // TODO(b/256022027): So far we only support kQualifiedIdExpr for
    // parent_property_expression, we could support more.
    return absl_ports::UnimplementedError(absl_ports::StrCat(
        "Parent property expression must be ", kQualifiedIdExpr));
  }

  std::sort(
      child_scored_document_hits.begin(), child_scored_document_hits.end(),
      ScoredDocumentHitComparator(
          /*is_descending=*/join_spec.nested_spec().scoring_spec().order_by() ==
          ScoringSpecProto::Order::DESC));

  // TODO(b/256022027):
  // - Optimization
  //   - Cache property to speed up property retrieval.
  //   - If there is no cache, then we still have the flexibility to fetch it
  //     from actual docs via DocumentStore.

  // Step 1: group child documents by parent documentId. Currently we only
  //         support QualifiedId joining, so fetch the qualified id content of
  //         child_property_expression, break it down into namespace + uri, and
  //         lookup the DocumentId.
  // The keys of this map are the DocumentIds of the parent docs the child
  // ScoredDocumentHits refer to. The values in this map are vectors of child
  // ScoredDocumentHits that refer to a parent DocumentId.
  std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      map_joinable_qualified_id;
  for (const ScoredDocumentHit& child : child_scored_document_hits) {
    std::string property_content = FetchPropertyExpressionValue(
        child.document_id(), join_spec.child_property_expression());

    // Parse qualified id.
    libtextclassifier3::StatusOr<QualifiedId> qualified_id_or =
        QualifiedId::Parse(property_content);
    if (!qualified_id_or.ok()) {
      ICING_VLOG(2) << "Skip content with invalid format of QualifiedId";
      continue;
    }
    QualifiedId qualified_id = std::move(qualified_id_or).ValueOrDie();

    // Lookup parent DocumentId.
    libtextclassifier3::StatusOr<DocumentId> parent_doc_id_or =
        doc_store_->GetDocumentId(qualified_id.name_space(),
                                  qualified_id.uri());
    if (!parent_doc_id_or.ok()) {
      // Skip the document if getting errors.
      continue;
    }
    DocumentId parent_doc_id = std::move(parent_doc_id_or).ValueOrDie();

    // Since we've already sorted child_scored_document_hits, just simply omit
    // if the map_joinable_qualified_id[parent_doc_id].size() has reached max
    // joined child count.
    if (map_joinable_qualified_id[parent_doc_id].size() <
        join_spec.max_joined_child_count()) {
      map_joinable_qualified_id[parent_doc_id].push_back(child);
    }
  }
  return JoinChildrenFetcher(join_spec, std::move(map_joinable_qualified_id));
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

std::string JoinProcessor::FetchPropertyExpressionValue(
    const DocumentId& document_id,
    const std::string& property_expression) const {
  // TODO(b/256022027): Add caching of document_id -> {expression -> value}
  libtextclassifier3::StatusOr<DocumentProto> document_or =
      doc_store_->Get(document_id);
  if (!document_or.ok()) {
    // Skip the document if getting errors.
    return "";
  }

  DocumentProto document = std::move(document_or).ValueOrDie();

  return std::string(GetString(&document, property_expression));
}

}  // namespace lib
}  // namespace icing
