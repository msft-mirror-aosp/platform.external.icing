// Copyright (C) 2019 Google LLC
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

#ifndef ICING_RESULT_SNIPPET_CONTEXT_H_
#define ICING_RESULT_SNIPPET_CONTEXT_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-terms.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Stores data needed for snippeting. With SnippetContext we can fetch snippets
// for queries with multiple pages.
struct SnippetContext {
  // A struct to store the cache entry for embedding match info.
  struct EmbeddingMatchInfoEntry {
    double score;
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type;
    // The position of the matched embedding vector in a section relative to
    // other vectors with the same (dimension, signature) combination. Note that
    // this is not the universal position of the vector in the section.
    //
    // E.g. If a repeated vector property contains the following vectors:
    // - vector1: [1, 2, 3] (signature = "signature1", dimension = 3)
    // - vector2: [7, 8, 9] (signature = "signature1", dimension = 3)
    // - vector3: [4, 5, 6, 8] (signature = "signature2", dimension = 4)
    // - vector4: [10, 11, 12] (signature = "signature1", dimension = 3)
    //
    // Then the position values for each vector would be:
    // - vector1: 0
    // - vector2: 1
    // - vector3: 0
    // - vector4: 2
    int position;
    int query_vector_index;
    SectionId section_id;

    explicit EmbeddingMatchInfoEntry(
        double score_in,
        SearchSpecProto::EmbeddingQueryMetricType::Code metric_type_in,
        int position_in, int query_vector_index_in, SectionId section_id_in) {
      score = score_in;
      metric_type = metric_type_in;
      position = position_in;
      query_vector_index = query_vector_index_in;
      section_id = section_id_in;
    }
  };

  // Maps from document_id to a vector of EmbeddingMatchInfoEntry. This
  // is used to retrieve the full embedding match info for a given document
  // during snippeting.
  using DocumentEmbeddingMatchInfoMap =
      std::unordered_map<DocumentId, std::vector<EmbeddingMatchInfoEntry>>;

  // Map of
  // (query_vector_dimension -> (model_signature -> set of query_vector_index))
  // for the embedding query vectors in the search spec.
  using EmbeddingQueryVectorMetadataMap = std::unordered_map<
      int, std::unordered_map<std::string, std::unordered_set<int>>>;

  explicit SnippetContext(
      SectionRestrictQueryTermsMap query_terms_in,
      EmbeddingQueryVectorMetadataMap embedding_query_vector_metadata_map_in,
      DocumentEmbeddingMatchInfoMap embedding_match_info_map_in,
      ResultSpecProto::SnippetSpecProto snippet_spec_in,
      TermMatchType::Code match_type_in)
      : query_terms(std::move(query_terms_in)),
        embedding_query_vector_metadata_map(
            std::move(embedding_query_vector_metadata_map_in)),
        embedding_match_info_map(std::move(embedding_match_info_map_in)),
        snippet_spec(std::move(snippet_spec_in)),
        match_type(match_type_in) {}

  // Query terms that are used to find snippets
  SectionRestrictQueryTermsMap query_terms;

  // Query vector metadata map for finding the global section positions for
  // each embedding match.
  //
  // Map of (query_vector_dimension -> (model_signature -> query_vector_index))
  // for the embedding query vectors in the search spec.
  EmbeddingQueryVectorMetadataMap embedding_query_vector_metadata_map;

  // Results retrieved from embedding queries.
  DocumentEmbeddingMatchInfoMap embedding_match_info_map;

  // Spec that defines some quantities of snippeting
  ResultSpecProto::SnippetSpecProto snippet_spec;

  // Defines how we match each term
  TermMatchType::Code match_type;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_RESULT_SNIPPET_CONTEXT_H_
