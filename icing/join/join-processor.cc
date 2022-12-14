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
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
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

  // Break children down into maps. The keys of this map are the DocumentIds of
  // the parent docs the child ScoredDocumentHits refer to. The values in this
  // map are vectors of child ScoredDocumentHits that refer to a parent
  // DocumentId.
  std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      parent_to_child_map;
  for (const ScoredDocumentHit& child : child_scored_document_hits) {
    std::string property_content = FetchPropertyExpressionValue(
        child.document_id(), join_spec.child_property_expression());

    // Try to split the property content by separators.
    std::vector<int> separators_in_property_content =
        GetSeparatorLocations(property_content, "#");

    if (separators_in_property_content.size() != 1) {
      // Skip the document if the qualified id isn't made up of the namespace
      // and uri. StrSplit will return just the original string if there are no
      // spaces.
      continue;
    }

    std::string ns =
        property_content.substr(0, separators_in_property_content[0]);
    std::string uri =
        property_content.substr(separators_in_property_content[0] + 1);

    UnescapeSeparator(ns, "#");
    UnescapeSeparator(uri, "#");

    libtextclassifier3::StatusOr<DocumentId> doc_id_or =
        doc_store_->GetDocumentId(ns, uri);

    if (!doc_id_or.ok()) {
      // Skip the document if getting errors.
      continue;
    }

    DocumentId parent_doc_id = std::move(doc_id_or).ValueOrDie();

    // This assumes the child docs are already sorted.
    if (parent_to_child_map[parent_doc_id].size() <
        join_spec.max_joined_child_count()) {
      parent_to_child_map[parent_doc_id].push_back(std::move(child));
    }
  }

  std::vector<JoinedScoredDocumentHit> joined_scored_document_hits;
  joined_scored_document_hits.reserve(parent_scored_document_hits.size());

  // Then add use child maps to add to parent ScoredDocumentHits.
  for (ScoredDocumentHit& parent : parent_scored_document_hits) {
    DocumentId parent_doc_id = kInvalidDocumentId;
    if (join_spec.parent_property_expression() == kFullyQualifiedIdExpr) {
      parent_doc_id = parent.document_id();
    } else {
      // TODO(b/256022027): So far we only support kFullyQualifiedIdExpr for
      // parent_property_expression, we could support more.
      return absl_ports::UnimplementedError(
          join_spec.parent_property_expression() +
          " must be \"fullyQualifiedId(this)\"");
    }

    // TODO(b/256022027): Derive final score from
    // parent_to_child_map[parent_doc_id] and
    // join_spec.aggregation_score_strategy()
    double final_score = parent.score();
    joined_scored_document_hits.emplace_back(
        final_score, std::move(parent),
        std::vector<ScoredDocumentHit>(
            std::move(parent_to_child_map[parent_doc_id])));
  }

  return joined_scored_document_hits;
}

// This loads a document and uses a property expression to fetch the value of
// the property from the document. The property expression may refer to nested
// document properties. We do not allow for repeated values in this property
// path, as that would allow for a single document to join to multiple
// documents.
//
// Returns:
//   "" on document load error.
//   "" if the property path is not found in the document.
//   "" if part of the property path is a repeated value.
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

std::vector<int> JoinProcessor::GetSeparatorLocations(
    const std::string& content, const std::string& separator) const {
  std::vector<int> separators_in_property_content;

  for (int i = 0; i < content.length(); ++i) {
    if (content[i] == '\\') {
      // Skip the following character
      i++;
    } else if (content[i] == '#') {
      // Unescaped separator
      separators_in_property_content.push_back(i);
    }
  }
  return separators_in_property_content;
}

void JoinProcessor::UnescapeSeparator(std::string& property,
                                      const std::string& separator) {
  size_t start_pos = 0;
  while ((start_pos = property.find("\\" + separator, start_pos)) !=
         std::string::npos) {
    property.replace(start_pos, 2, "#");
    start_pos += 1;
  }
}

}  // namespace lib
}  // namespace icing
