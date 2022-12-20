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
#include "icing/join/qualified-id.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/util/snippet-helpers.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::vector<JoinedScoredDocumentHit>>
JoinProcessor::Join(
    const JoinSpecProto& join_spec,
    std::vector<ScoredDocumentHit>&& parent_scored_document_hits,
    std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
  std::sort(
      child_scored_document_hits.begin(), child_scored_document_hits.end(),
      ScoredDocumentHitComparator(
          /*is_descending=*/join_spec.nested_spec().scoring_spec().order_by() ==
          ScoringSpecProto::Order::DESC));

  // TODO(b/256022027):
  // - Aggregate scoring
  //   - Calculate the aggregated score if strategy is AGGREGATION_SCORING.
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
      parent_id_to_child_map;
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
    // if the parent_id_to_child_map[parent_doc_id].size() has reached max
    // joined child count.
    if (parent_id_to_child_map[parent_doc_id].size() <
        join_spec.max_joined_child_count()) {
      parent_id_to_child_map[parent_doc_id].push_back(child);
    }
  }

  std::vector<JoinedScoredDocumentHit> joined_scored_document_hits;
  joined_scored_document_hits.reserve(parent_scored_document_hits.size());

  // Step 2: iterate through all parent documentIds and construct
  //         JoinedScoredDocumentHit for each by looking up
  //         parent_id_to_child_map.
  for (ScoredDocumentHit& parent : parent_scored_document_hits) {
    DocumentId parent_doc_id = kInvalidDocumentId;
    if (join_spec.parent_property_expression() == kQualifiedIdExpr) {
      parent_doc_id = parent.document_id();
    } else {
      // TODO(b/256022027): So far we only support kQualifiedIdExpr for
      // parent_property_expression, we could support more.
      return absl_ports::UnimplementedError(absl_ports::StrCat(
          "Parent property expression must be ", kQualifiedIdExpr));
    }

    // TODO(b/256022027): Derive final score from
    // parent_id_to_child_map[parent_doc_id] and
    // join_spec.aggregation_score_strategy()
    double final_score = parent.score();
    joined_scored_document_hits.emplace_back(
        final_score, std::move(parent),
        std::vector<ScoredDocumentHit>(
            std::move(parent_id_to_child_map[parent_doc_id])));
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
