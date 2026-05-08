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

#include "icing/monkey_test/abstract_query_tree/monkey-semantic-query-node.h"

#include <iomanip>
#include <ios>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

MonkeySemanticQueryNode::MonkeySemanticQueryNode(
    int vector_index, double min_score, double max_score,
    SearchSpecProto::EmbeddingQueryMetricType::Code distance_metric,
    PropertyProto::VectorProto embedding_query_vector,
    std::unordered_set<std::string> property_restricts,
    std::vector<std::string> document_namespaces,
    std::vector<std::string> document_schema_types)
    : MonkeyAbstractRestrictableLeafQueryNode(std::move(property_restricts),
                                              std::move(document_namespaces),
                                              std::move(document_schema_types)),
      vector_index_(vector_index),
      min_score_(min_score),
      max_score_(max_score),
      distance_metric_(distance_metric),
      embedding_query_vector_(std::move(embedding_query_vector)) {}

MonkeySemanticQueryNode::MonkeySemanticQueryNode(
    int vector_index, double min_score, double max_score,
    SearchSpecProto::EmbeddingQueryMetricType::Code distance_metric,
    PropertyProto::VectorProto embedding_query_vector)
    : MonkeySemanticQueryNode(
          vector_index, min_score, max_score, distance_metric,
          std::move(embedding_query_vector),
          /*property_restricts=*/{},
          /*document_namespaces=*/{}, /*document_schema_types=*/{}) {}

libtextclassifier3::StatusOr<bool>
MonkeySemanticQueryNode::DoesDocumentMatchQuery(
    const InMemoryIcingSearchEngine* search_engine,
    const MonkeyTokenizedDocument& document) const {
  // distance_metric_ should never be UNKNOWN, but if it is, we'll default to
  // COSINE.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<EmbeddingScorer> embedding_scorer,
      EmbeddingScorer::Create(
          distance_metric_ == SearchSpecProto::EmbeddingQueryMetricType::UNKNOWN
              ? SearchSpecProto::EmbeddingQueryMetricType::COSINE
              : distance_metric_));

  for (const MonkeySection& section : document.sections) {
    if (!IsRestrictedSection(section)) {
      continue;
    }
    ICING_ASSIGN_OR_RETURN(
        InMemoryIcingSearchEngine::PropertyIndexInfo property_index_info,
        search_engine->GetPropertyIndexInfo(document.document.schema(),
                                            section.path));
    if (!property_index_info.indexable ||
        property_index_info.data_type !=
            PropertyConfigProto::DataType::VECTOR) {
      continue;
    }

    for (const PropertyProto::VectorProto& section_vector :
         section.vector_values) {
      ICING_ASSIGN_OR_RETURN(
          bool match,
          DoesVectorsMatch(embedding_scorer.get(), min_score_, max_score_,
                           property_index_info.quantization_type,
                           embedding_query_vector_, section_vector));
      if (match) {
        return true;
      }
    }
  }
  return false;
}

std::string MonkeySemanticQueryNode::GenerateQueryString() const {
  std::ostringstream stream;
  stream << std::fixed << std::setprecision(2);
  // TODO(b/491571627) - Handle multiple property restricts.
  stream << (property_restricts_.empty()
                 ? ""
                 : absl_ports::StrCat(*property_restricts_.begin(), ":"));
  stream << "semanticSearch(getEmbeddingParameter(" << vector_index_ << "), "
         << min_score_ << ", " << max_score_;
  stream << (distance_metric_ ==
                     SearchSpecProto::EmbeddingQueryMetricType::UNKNOWN
                 ? ", \"COSINE\""
                 : absl_ports::StrCat(
                       ", \"",
                       SearchSpecProto::EmbeddingQueryMetricType::Code_Name(
                           distance_metric_),
                       "\""));
  stream << ")";
  return stream.str();
}

}  // namespace lib
}  // namespace icing
