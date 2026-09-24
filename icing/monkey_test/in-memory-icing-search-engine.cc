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

#include "icing/monkey_test/in-memory-icing-search-engine.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <random>
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
#include "icing/absl_ports/str_join.h"
#include "icing/join/qualified-id.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-group-info.h"
#include "icing/store/document-id.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

static constexpr float kEps = 1e-6;

namespace {

template <typename T>
void SortResults(std::vector<T>& results,
                 ScoringSpecProto::Order::Code order_by) {
  struct ScoreExtractor {
    int operator()(const T& r) const { return r.scored_document_id.score; }
    int operator()(const T* r) const { return r->scored_document_id.score; }
  } get_score;

  if (order_by == ScoringSpecProto::Order::ASC) {
    std::sort(results.begin(), results.end(),
              [&get_score](const T& a, const T& b) {
                if (get_score(a) == get_score(b)) {
                  return a.scored_document_id.document_id <
                         b.scored_document_id.document_id;
                }
                return get_score(a) < get_score(b);
              });
  } else {
    std::sort(results.begin(), results.end(),
              [&get_score](const T& a, const T& b) {
                if (get_score(a) == get_score(b)) {
                  return a.scored_document_id.document_id >
                         b.scored_document_id.document_id;
                }
                return get_score(a) > get_score(b);
              });
  }
}

// Helper function to (BFS) traverse from doc_ids to all children by the given
// graph.
//
// Returns: the set of all document ids, INCLUDING the original doc_ids and the
//          traversed child doc ids.
std::unordered_set<DocumentId> TraverseToAllChildren(
    const std::unordered_map<DocumentId, std::unordered_set<DocumentId>>& graph,
    std::unordered_set<DocumentId>&& doc_ids) {
  // Use BFS to propagate doc_ids to children.
  std::queue<DocumentId> que(std::deque(doc_ids.cbegin(), doc_ids.cend()));
  while (!que.empty()) {
    DocumentId doc_id_to_expand = que.front();
    que.pop();

    auto itr = graph.find(doc_id_to_expand);
    if (itr == graph.end()) {
      // No child documents.
      continue;
    }
    for (DocumentId child_doc_id : itr->second) {
      if (doc_ids.find(child_doc_id) == doc_ids.end()) {
        doc_ids.insert(child_doc_id);
        que.push(child_doc_id);
      }
    }
  }
  return doc_ids;
}

}  // namespace

InMemoryIcingSearchEngine::InMemoryIcingSearchEngine(
    MonkeyTestRandomEngine* random, bool enable_delete_propagation)
    : random_(random), enable_delete_propagation_(enable_delete_propagation) {
  language_segmenter_factory::SegmenterOptions segmenter_options(
      ULOC_US, /*jni_cache=*/nullptr, /*enable_icu_segmenter=*/true);
  language_segmenter_ =
      language_segmenter_factory::Create(
          segmenter_options, /*icu_segmenter_creation_status=*/nullptr)
          .ValueOrDie();
}

libtextclassifier3::StatusOr<const PropertyConfigProto *>
InMemoryIcingSearchEngine::GetPropertyConfig(
    const std::string &schema_type, const std::string &property_name) const {
  auto schema_iter = property_config_map_.find(schema_type);
  if (schema_iter == property_config_map_.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Schema type: ", schema_type, " is not found."));
  }
  auto property_iter = schema_iter->second.find(property_name);
  if (property_iter == schema_iter->second.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Property: ", property_name, " is not found."));
  }
  return &property_iter->second;
}

libtextclassifier3::StatusOr<InMemoryIcingSearchEngine::PropertyIndexInfo>
InMemoryIcingSearchEngine::GetPropertyIndexInfo(
    const std::string& schema_type, std::string_view property_path) const {
  bool in_indexable_properties_list = false;
  bool all_indexable_from_top = true;

  std::vector<std::string_view> properties_in_path =
      absl_ports::StrSplit(property_path, ".");
  if (properties_in_path.empty()) {
    return absl_ports::InvalidArgumentError("Got empty path.");
  }
  std::string curr_schema_type = schema_type;
  for (int i = 0; i < properties_in_path.size(); ++i) {
    ICING_ASSIGN_OR_RETURN(
        const PropertyConfigProto *prop,
        GetPropertyConfig(curr_schema_type,
                          std::string(properties_in_path[i])));
    if (prop->data_type() == PropertyConfigProto::DataType::STRING) {
      TermMatchType::Code term_match_type =
          prop->string_indexing_config().term_match_type();
      StringIndexingConfig::TokenizerType::Code tokenizer_type =
          prop->string_indexing_config().tokenizer_type();
      bool indexable =
          term_match_type != TermMatchType::UNKNOWN &&
          tokenizer_type != StringIndexingConfig::TokenizerType::NONE;
      return PropertyIndexInfo{.data_type = prop->data_type(),
                               .term_match_type = term_match_type,
                               .tokenizer_type = tokenizer_type,
                               .indexable = indexable};
    }

    if (prop->data_type() == PropertyConfigProto::DataType::VECTOR) {
      EmbeddingIndexingConfig::EmbeddingIndexingType::Code
          embedding_indexing_type =
              prop->embedding_indexing_config().embedding_indexing_type();
      bool indexable = embedding_indexing_type !=
                       EmbeddingIndexingConfig::EmbeddingIndexingType::UNKNOWN;
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type =
          prop->embedding_indexing_config().quantization_type();
      return PropertyIndexInfo{
          .data_type = prop->data_type(),
          .quantization_type = quantization_type,
          .embedding_indexing_type = embedding_indexing_type,
          .indexable = indexable};
    }

    if (prop->data_type() == PropertyConfigProto::DataType::INT64) {
      IntegerIndexingConfig::NumericMatchType::Code numeric_match_type =
          prop->integer_indexing_config().numeric_match_type();
      bool indexable = numeric_match_type !=
                       IntegerIndexingConfig::NumericMatchType::UNKNOWN;
      return PropertyIndexInfo{.data_type = prop->data_type(),
                               .numeric_match_type = numeric_match_type,
                               .indexable = indexable};
    }

    if (prop->data_type() != PropertyConfigProto::DataType::DOCUMENT) {
      return PropertyIndexInfo{.data_type = prop->data_type()};
    }

    bool old_all_indexable_from_top = all_indexable_from_top;
    all_indexable_from_top &=
        prop->document_indexing_config().index_nested_properties();
    if (!all_indexable_from_top && !in_indexable_properties_list) {
      // Only try to update in_indexable_properties_list if this is the first
      // level with index_nested_properties=false.
      if (old_all_indexable_from_top) {
        auto &indexable_properties =
            prop->document_indexing_config().indexable_nested_properties_list();
        std::string relative_path =
            absl_ports::StrCatPieces(std::vector<std::string_view>(
                properties_in_path.begin() + i + 1, properties_in_path.end()));
        in_indexable_properties_list =
            std::find(indexable_properties.begin(), indexable_properties.end(),
                      relative_path) != indexable_properties.end();
      }
      // Check in_indexable_properties_list again.
      if (!in_indexable_properties_list) {
        return PropertyIndexInfo{.data_type = prop->data_type()};
      }
    }
    curr_schema_type = prop->document_indexing_config().GetTypeName();
  }

  return absl_ports::NotFoundError(
      absl_ports::StrCat("Property: ", property_path, " is not found."));
}

std::optional<InMemoryIcingSearchEngine::PropertyJoinableInfo>
InMemoryIcingSearchEngine::GetPropertyJoinableInfo(
    const std::string& schema_type, std::string_view property_path) const {
  std::vector<std::string_view> properties_in_path =
      absl_ports::StrSplit(property_path, ".");
  if (properties_in_path.empty()) {
    return std::nullopt;
  }

  std::string curr_schema_type = schema_type;
  for (int i = 0; i < properties_in_path.size(); ++i) {
    auto prop_or =
        GetPropertyConfig(curr_schema_type, std::string(properties_in_path[i]));
    if (!prop_or.ok()) {
      return std::nullopt;
    }
    const PropertyConfigProto* prop = prop_or.ValueOrDie();

    if (prop->data_type() != PropertyConfigProto::DataType::DOCUMENT &&
        i == properties_in_path.size() - 1) {
      return PropertyJoinableInfo{
          .data_type = prop->data_type(),
          .value_type = prop->joinable_config().value_type(),
          .delete_propagation_type =
              prop->joinable_config().delete_propagation_type()};
    }
    curr_schema_type = prop->schema_type();
  }
  return std::nullopt;
}

void InMemoryIcingSearchEngine::SetSchema(SchemaProto schema) {
  schema_ = std::make_unique<SchemaProto>(std::move(schema));
  property_config_map_.clear();
  for (const SchemaTypeConfigProto &type_config : schema_->types()) {
    auto &curr_property_map = property_config_map_[type_config.schema_type()];
    for (const PropertyConfigProto &property_config :
         type_config.properties()) {
      curr_property_map.insert(
          {property_config.property_name(), property_config});
    }
  }
}

InMemoryIcingSearchEngine::PickDocumentResult
InMemoryIcingSearchEngine::RandomPickDocument(float p_alive, float p_all,
                                              float p_other) const {
  // Normalizing p_alive, p_all and p_other, so that they sum to 1.
  if (abs(p_alive) < kEps && abs(p_all) < kEps && abs(p_other) < kEps) {
    p_alive = p_all = p_other = 1 / 3.;
  } else {
    float p_sum = p_alive + p_all + p_other;
    p_alive = p_alive / p_sum;
    p_all = p_all / p_sum;
    p_other = p_other / p_sum;
  }

  std::uniform_real_distribution<> real_dist(0, 1);
  float p = real_dist(*random_);
  if (p <= p_other || documents_.empty()) {
    // 20 is a fair number of non-existing namespaces and uris, enough for
    // monkey testing.
    std::uniform_int_distribution<> dist(0, 19);
    std::string name_space = absl_ports::StrCat("non_existing_namespace",
                                                std::to_string(dist(*random_)));
    std::string uri =
        absl_ports::StrCat("non_existing_uri", std::to_string(dist(*random_)));
    return {name_space, uri};
  }
  p -= p_other;
  DocumentId doc_id;
  if (p <= p_all || existing_doc_ids_.empty()) {
    std::uniform_int_distribution<DocumentId> dist(0, documents_.size() - 1);
    doc_id = dist(*random_);
  } else {
    std::uniform_int_distribution<DocumentId> dist(
        0, existing_doc_ids_.size() - 1);
    doc_id = existing_doc_ids_[dist(*random_)];
  }
  InMemoryIcingSearchEngine::PickDocumentResult result = {
      documents_[doc_id].document.namespace_(),
      documents_[doc_id].document.uri()};

  // Even the (name_space, uri) of the picked doc_id has not been deleted
  // specifically, doc_id may be outdated because of possible overwriting. So we
  // need to find the latest document id, and return the latest DocumentProto.
  auto latest_doc_id = InternalGet(result.name_space, result.uri);
  if (latest_doc_id.ok()) {
    result.document = documents_[latest_doc_id.ValueOrDie()].document;
  }
  return result;
}

libtextclassifier3::Status InMemoryIcingSearchEngine::Put(
    const MonkeyTokenizedDocument& document) {
  ICING_RETURN_IF_ERROR(ValidateDependency(document));

  // Delete the old one if existing.
  auto doc_id_or =
      InternalGet(document.document.namespace_(), document.document.uri());
  if (doc_id_or.ok()) {
    InternalBatchDelete({doc_id_or.ValueOrDie()});
  }

  existing_doc_ids_.push_back(documents_.size());
  namespace_uri_docid_map_[document.document.namespace_()]
                          [document.document.uri()] = documents_.size();
  documents_.push_back(document);

  return libtextclassifier3::Status::OK;
}

std::unordered_set<std::string> InMemoryIcingSearchEngine::GetAllNamespaces()
    const {
  std::unordered_set<std::string> namespaces;
  for (DocumentId doc_id : existing_doc_ids_) {
    namespaces.insert(documents_[doc_id].document.namespace_());
  }
  return namespaces;
}

libtextclassifier3::StatusOr<std::vector<DocumentMetadata>>
InMemoryIcingSearchEngine::Delete(const std::string& name_space,
                                  const std::string& uri) {
  ICING_ASSIGN_OR_RETURN(DocumentId doc_id, InternalGet(name_space, uri));
  ICING_ASSIGN_OR_RETURN(std::unordered_set<DocumentId> doc_ids_to_delete,
                         GetDocIdsForDeletePropagation({doc_id}));
  return InternalBatchDelete(doc_ids_to_delete);
}

libtextclassifier3::StatusOr<uint32_t>
InMemoryIcingSearchEngine::DeleteByNamespace(const std::string& name_space) {
  std::unordered_set<DocumentId> doc_ids_to_delete;
  for (DocumentId doc_id : existing_doc_ids_) {
    if (documents_[doc_id].document.namespace_() == name_space) {
      doc_ids_to_delete.insert(doc_id);
    }
  }

  ICING_RETURN_IF_ERROR(InternalBatchDelete(doc_ids_to_delete));
  return doc_ids_to_delete.size();
}

libtextclassifier3::StatusOr<uint32_t>
InMemoryIcingSearchEngine::DeleteBySchemaType(const std::string& schema_type) {
  std::unordered_set<DocumentId> doc_ids_to_delete;
  for (DocumentId doc_id : existing_doc_ids_) {
    if (documents_[doc_id].document.schema() == schema_type) {
      doc_ids_to_delete.insert(doc_id);
    }
  }

  ICING_RETURN_IF_ERROR(InternalBatchDelete(doc_ids_to_delete));
  return doc_ids_to_delete.size();
}

libtextclassifier3::StatusOr<std::vector<DocumentMetadata>>
InMemoryIcingSearchEngine::DeleteByQuery(const MonkeyAbstractQueryNode* node) {
  ScoringSpecProto scoring_spec;
  // We don't need to delete the results in any particular order, so we can
  // skip the scoring and sorting step.
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::NONE);
  ICING_ASSIGN_OR_RETURN(
      std::vector<NestedScoredDocumentId> nested_scored_doc_ids_to_delete,
      InternalSearch(node, /*nested_queries=*/{}, scoring_spec));

  // Unzip the nested structure and collect all document ids to delete. This
  // will dedupe doc ids and count # of deletions correctly.
  std::unordered_set<DocumentId> doc_ids_to_delete;
  UnzipNestedScoredDocumentIds(nested_scored_doc_ids_to_delete,
                               doc_ids_to_delete);

  ICING_ASSIGN_OR_RETURN(doc_ids_to_delete, GetDocIdsForDeletePropagation(
                                                std::move(doc_ids_to_delete)));
  return InternalBatchDelete(doc_ids_to_delete);
}

libtextclassifier3::StatusOr<std::vector<SearchResultProto::ResultProto>>
InMemoryIcingSearchEngine::Search(
    const MonkeyAbstractQueryNode* base_query_node,
    const std::vector<JoinQuerySpec>& nested_queries,
    const ScoringSpecProto& scoring_spec) const {
  ICING_ASSIGN_OR_RETURN(
      std::vector<NestedScoredDocumentId> matched_results,
      InternalSearch(base_query_node, nested_queries, scoring_spec));

  return FetchNestedResultDocuments(matched_results);
}

libtextclassifier3::StatusOr<SearchResultProto>
InMemoryIcingSearchEngine::Search(
    const MonkeyAbstractQueryNode* base_query_node,
    const std::vector<JoinQuerySpec>& nested_queries,
    const ScoringSpecProto& scoring_spec, int page_size) const {
  ICING_ASSIGN_OR_RETURN(
      std::vector<NestedScoredDocumentId> matched_results,
      InternalSearch(base_query_node, nested_queries, scoring_spec));

  std::uniform_int_distribution<int> dist(0, std::numeric_limits<int>::max());
  int next_page_token = dist((*random_));

  // Reverse the matched results, so that the first result is at the end of the
  // list for more efficient access.
  std::reverse(matched_results.begin(), matched_results.end());
  auto [itr, is_inserted] = paginated_results_map_.insert(
      {next_page_token,
       PaginationState(std::move(matched_results), page_size)});
  if (!is_inserted) {
    ICING_LOG(FATAL) << "Token collision: " << next_page_token;  // Crash OK.
  }
  return Paginate(itr);
}

libtextclassifier3::StatusOr<SearchResultProto>
InMemoryIcingSearchEngine::GetNextPage(int next_page_token) const {
  SearchResultProto result;
  if (next_page_token == 0) {
    return result;
  }
  auto itr = paginated_results_map_.find(next_page_token);
  // If the next page token is not found, then return a search result with
  // page_token_not_found set to true. This will happen if we never issued a
  // query corresponding to that token.
  // Tokens with no more results do not return page_token_not_found and return
  // an empty result instead (with set_page_token_not_found to false).
  if (itr == paginated_results_map_.end()) {
    result.set_page_token_not_found(true);
    return result;
  }
  return Paginate(itr);
}

libtextclassifier3::StatusOr<int>
InMemoryIcingSearchEngine::RevalidateDocuments(
    const std::unordered_set<std::string>& schema_types_deleted,
    const std::unordered_set<std::string>& schema_types_incompatible) {
  DependencyGraphResult dependency_graph_result = BuildDependencyGraph();

  std::unordered_set<DocumentId> doc_ids_to_delete =
      std::move(dependency_graph_result.unsatisfied_doc_ids);
  for (DocumentId doc_id : existing_doc_ids_) {
    if (schema_types_deleted.find(documents_[doc_id].document.schema()) !=
            schema_types_deleted.end() ||
        schema_types_incompatible.find(documents_[doc_id].document.schema()) !=
            schema_types_incompatible.end()) {
      doc_ids_to_delete.insert(doc_id);
    }
  }

  doc_ids_to_delete = TraverseToAllChildren(dependency_graph_result.graph,
                                            std::move(doc_ids_to_delete));
  ICING_RETURN_IF_ERROR(InternalBatchDelete(doc_ids_to_delete));
  return static_cast<int>(doc_ids_to_delete.size());
}

std::vector<std::string> InMemoryIcingSearchEngine::GetAllJoinProperties()
    const {
  std::unordered_set<std::string> join_properties;
  for (const auto& [_, property_config_map] : property_config_map_) {
    for (const auto& [property_name, property_config] : property_config_map) {
      if (property_config.joinable_config().value_type() !=
          JoinableConfig::ValueType::NONE) {
        join_properties.insert(property_name);
      }
    }
  }
  return std::vector<std::string>(join_properties.begin(),
                                  join_properties.end());
}

libtextclassifier3::StatusOr<DocumentId> InMemoryIcingSearchEngine::InternalGet(
    const std::string &name_space, const std::string &uri) const {
  auto uris = namespace_uri_docid_map_.find(name_space);
  if (uris != namespace_uri_docid_map_.end()) {
    auto doc = uris->second.find(uri);
    if (doc != uris->second.end()) {
      return doc->second;
    }
  }
  return absl_ports::NotFoundError(absl_ports::StrCat(
      name_space, ", ", uri,
      " is not found by InMemoryIcingSearchEngine::InternalGet."));
}

libtextclassifier3::StatusOr<
    std::vector<InMemoryIcingSearchEngine::NestedScoredDocumentId>>
InMemoryIcingSearchEngine::InternalSearch(
    const MonkeyAbstractQueryNode* base_query_node,
    const std::vector<JoinQuerySpec>& nested_queries,
    const ScoringSpecProto& scoring_spec) const {
  // Step 1: evaluate join search by all levels of queries in reverse order.
  JoinedNestedScoredDocumentIdMap curr_map;
  for (auto itr = nested_queries.rbegin(); itr != nested_queries.rend();
       ++itr) {
    // TODO: b/537846099 - Support scoring for nested levels.
    ICING_ASSIGN_OR_RETURN(
        curr_map, InternalSingleLevelJoinSearch(*itr, std::move(curr_map)));
  }

  // Step 2: finally, evaluate the top (1st) level search spec, and join with
  //   the results from the lower levels (if any).
  ICING_ASSIGN_OR_RETURN(std::vector<DocumentId> base_query_matched_doc_ids,
                         base_query_node->EvaluateQuery(this));
  // TODO: b/537846888 - Support RANKING_STRATEGY_JOIN_AGGREGATE_SCORE.
  ICING_ASSIGN_OR_RETURN(
      std::vector<ScoredDocumentId> scored_base_query_matched_doc_ids,
      Score(base_query_matched_doc_ids, scoring_spec.rank_by()));
  std::vector<NestedScoredDocumentId> matched_results;
  matched_results.reserve(scored_base_query_matched_doc_ids.size());
  for (ScoredDocumentId scored_document_id :
       scored_base_query_matched_doc_ids) {
    auto itr = curr_map.find(scored_document_id.document_id);
    matched_results.push_back(NestedScoredDocumentId{
        .scored_document_id = scored_document_id,
        .nested_scored_document_ids =
            itr == curr_map.end() ? std::vector<NestedScoredDocumentId>()
                                  : std::move(itr->second)});
  }
  if (scoring_spec.rank_by() == ScoringSpecProto::RankingStrategy::NONE) {
    return matched_results;
  }
  SortResults(matched_results, scoring_spec.order_by());
  return matched_results;
}

libtextclassifier3::StatusOr<
    InMemoryIcingSearchEngine::JoinedNestedScoredDocumentIdMap>
InMemoryIcingSearchEngine::InternalSingleLevelJoinSearch(
    const JoinQuerySpec& join_query_spec,
    JoinedNestedScoredDocumentIdMap&& child_map) const {
  // Evaluate the query and get all matched doc ids at the current level.
  ICING_ASSIGN_OR_RETURN(std::vector<DocumentId> matched_doc_ids,
                         join_query_spec.curr_query_node->EvaluateQuery(this));

  // Join:
  // - child_map contains doc id -> nested result ids of the LOWER (child)
  //   level, obtained from the previous iteration.
  // - matched_doc_ids are the result doc ids at the the current level.
  // - From this level's perspective, the child_map's key (parent doc id) is the
  //   result doc id at this level, so join matched_doc_ids with child_map's
  //   key.
  // - Since it is left join, only keep doc ids that are present in
  //   matched_doc_ids and discard the key-value pairs in child_map whose keys
  //   are not in matched_doc_ids.
  // - Finally, read out the join property values from the current level's
  //   document and group the results by the values. This will be used for the
  //   next iteration (upper level).
  JoinedNestedScoredDocumentIdMap curr_map;
  for (DocumentId matched_doc_id : matched_doc_ids) {
    // Check if the target joinable property (specified by
    // curr_join_prop_expr) of this document is joinable or not.
    // If it is not joinable, then this document CANNOT be joined with the upper
    // level, so skip it.
    std::optional<PropertyJoinableInfo> property_joinable_info =
        GetPropertyJoinableInfo(documents_[matched_doc_id].document.schema(),
                                join_query_spec.curr_join_prop_expr);
    if (property_joinable_info == std::nullopt ||
        property_joinable_info->data_type !=
            PropertyConfigProto::DataType::STRING ||
        property_joinable_info->value_type == JoinableConfig::ValueType::NONE) {
      // Skip non-joinable property.
      continue;
    }

    auto itr = child_map.find(matched_doc_id);
    // TODO: b/537846099 - Support scoring for nested levels.
    NestedScoredDocumentId nested_scored_doc_id = {
        .scored_document_id =
            ScoredDocumentId{.document_id = matched_doc_id, .score = 0},
        .nested_scored_document_ids =
            itr == child_map.end() ? std::vector<NestedScoredDocumentId>()
                                   : std::move(itr->second)};

    // Get all referenced (parent) doc qualified ids. This is the join
    // relationship between the current level and the upper level.
    const MonkeySection* joinable_property =
        documents_[matched_doc_id].GetSectionByPath(
            join_query_spec.curr_join_prop_expr);
    if (joinable_property == nullptr) {
      continue;
    }

    std::unordered_set<DocumentId> seen_ref_doc_ids;
    for (std::string_view qualified_id_str : joinable_property->string_values) {
      auto qualified_id_or = QualifiedId::Parse(qualified_id_str);
      if (!qualified_id_or.ok()) {
        // Skip invalid qualified id.
        continue;
      }
      QualifiedId qualified_id = std::move(qualified_id_or).ValueOrDie();

      auto ref_doc_id_or =
          InternalGet(qualified_id.name_space(), qualified_id.uri());
      if (ref_doc_id_or.ok() &&
          seen_ref_doc_ids.find(ref_doc_id_or.ValueOrDie()) ==
              seen_ref_doc_ids.end()) {
        curr_map[ref_doc_id_or.ValueOrDie()].push_back(
            nested_scored_doc_id);  // Copy instead of move, given that now we
                                    // support N-N join and nested_scored_doc_id
                                    // might be added to multiple parents.
        seen_ref_doc_ids.insert(ref_doc_id_or.ValueOrDie());
      }
    }
  }
  return curr_map;
}

libtextclassifier3::StatusOr<std::vector<DocumentMetadata>>
InMemoryIcingSearchEngine::InternalBatchDelete(
    const std::unordered_set<DocumentId>& doc_ids_to_delete) {
  std::vector<DocumentMetadata> deleted_documents;

  // Delete actual documents from the in-memory Icing.
  for (DocumentId doc_id : doc_ids_to_delete) {
    // Record the metadata of the deleted document.
    deleted_documents.push_back(DocumentMetadata{
        .schema_type_name = documents_[doc_id].document.schema(),
        .name_space = documents_[doc_id].document.namespace_(),
        .uri = documents_[doc_id].document.uri(),
        .document_id = doc_id});

    namespace_uri_docid_map_[documents_[doc_id].document.namespace_()].erase(
        documents_[doc_id].document.uri());
    documents_[doc_id].Clear();
  }

  // Remove deleted doc ids from existing_doc_ids_.
  int head_idx = 0;
  for (int i = 0; i < existing_doc_ids_.size(); ++i) {
    if (doc_ids_to_delete.find(existing_doc_ids_[i]) ==
        doc_ids_to_delete.end()) {
      // Keep the document.
      existing_doc_ids_[head_idx++] = existing_doc_ids_[i];
    }
  }
  existing_doc_ids_.resize(head_idx);

  // Remove deleted document ids from the paginated results map.
  for (auto itr = paginated_results_map_.begin();
       itr != paginated_results_map_.end(); ++itr) {
    itr->second.remaining_results.erase(
        std::remove_if(
            itr->second.remaining_results.begin(),
            itr->second.remaining_results.end(),
            [&doc_ids_to_delete](const NestedScoredDocumentId& doc_id) {
              return doc_ids_to_delete.contains(
                  doc_id.scored_document_id.document_id);
            }),
        itr->second.remaining_results.end());
  }
  return deleted_documents;
}

std::vector<SearchResultProto::ResultProto>
InMemoryIcingSearchEngine::FetchNestedResultDocuments(
    const std::vector<NestedScoredDocumentId>& nested_scored_doc_ids) const {
  std::vector<SearchResultProto::ResultProto> results;
  results.reserve(nested_scored_doc_ids.size());
  for (const NestedScoredDocumentId& nested_scored_doc_id :
       nested_scored_doc_ids) {
    SearchResultProto::ResultProto result;

    // Parent document.
    *result.mutable_document() =
        documents_[nested_scored_doc_id.scored_document_id.document_id]
            .document;

    // Child documents.
    std::vector<SearchResultProto::ResultProto> nested_results =
        FetchNestedResultDocuments(
            nested_scored_doc_id.nested_scored_document_ids);
    if (!nested_results.empty()) {
      result.mutable_joined_results()->Add(nested_results.begin(),
                                           nested_results.end());
    }

    results.push_back(std::move(result));
  }
  return results;
}

void InMemoryIcingSearchEngine::UnzipNestedScoredDocumentIds(
    const std::vector<NestedScoredDocumentId>& nested_scored_doc_ids,
    std::unordered_set<DocumentId>& doc_ids_out) const {
  for (const NestedScoredDocumentId& nested_scored_doc_id :
       nested_scored_doc_ids) {
    doc_ids_out.insert(nested_scored_doc_id.scored_document_id.document_id);
    UnzipNestedScoredDocumentIds(
        nested_scored_doc_id.nested_scored_document_ids, doc_ids_out);
  }
}

libtextclassifier3::Status InMemoryIcingSearchEngine::ValidateDependency(
    const MonkeyTokenizedDocument& document) const {
  if (!enable_delete_propagation_) {
    // If delete propagation is not enabled, then return OK directly since there
    // are no dependencies to validate.
    return libtextclassifier3::Status::OK;
  }

  for (const MonkeySection& property : document.sections) {
    std::optional<PropertyJoinableInfo> property_joinable_info =
        GetPropertyJoinableInfo(document.document.schema(), property.path);
    if (property_joinable_info == std::nullopt ||
        !property_joinable_info->HasDeletePropagation()) {
      continue;
    }

    for (std::string_view qualified_id_str : property.string_values) {
      if (qualified_id_str.empty()) {
        // Empty qualified id string is allowed.
        continue;
      }

      // Parse the qualified id string. If it fails, then return an error since
      // it is not a valid qualified id.
      ICING_ASSIGN_OR_RETURN(QualifiedId qualified_id,
                             QualifiedId::Parse(qualified_id_str));

      if (qualified_id.name_space() == document.document.namespace_() &&
          qualified_id.uri() == document.document.uri()) {
        // Self-reference detected, which is allowed.
        continue;
      }

      // Otherwise, the referenced document must exist.
      if (!InternalGet(qualified_id.name_space(), qualified_id.uri()).ok()) {
        return absl_ports::InvalidArgumentError(absl_ports::StrCat(
            "A dependency document is not alive: ", qualified_id.ToString()));
      }
    }
  }
  return libtextclassifier3::Status::OK;
}

InMemoryIcingSearchEngine::DependencyGraphResult
InMemoryIcingSearchEngine::BuildDependencyGraph() const {
  DependencyGraphResult result;
  if (!enable_delete_propagation_) {
    // If delete propagation is not enabled, then there are no dependencies, so
    // skip the following iterations to save time.
    return result;
  }

  for (DocumentId doc_id : existing_doc_ids_) {
    const MonkeyTokenizedDocument& document = documents_[doc_id];
    for (const MonkeySection& property : document.sections) {
      std::optional<PropertyJoinableInfo> property_joinable_info =
          GetPropertyJoinableInfo(document.document.schema(), property.path);
      if (property_joinable_info == std::nullopt ||
          !property_joinable_info->HasDeletePropagation()) {
        continue;
      }

      for (std::string_view qualified_id_str : property.string_values) {
        if (qualified_id_str.empty()) {
          // Empty qualified id string is allowed.
          continue;
        }

        // Add an edge from qualified_id to doc_id. In this case, the referenced
        // document MUST be alive given that delete propagation is enabled. If
        // not, then add it into unsatisfied_doc_ids.
        auto qualified_id_or = QualifiedId::Parse(qualified_id_str);
        if (!qualified_id_or.ok()) {
          result.unsatisfied_doc_ids.insert(doc_id);
          continue;
        }
        QualifiedId qualified_id = std::move(qualified_id_or).ValueOrDie();

        auto ref_doc_id_or =
            InternalGet(qualified_id.name_space(), qualified_id.uri());
        if (!ref_doc_id_or.ok()) {
          result.unsatisfied_doc_ids.insert(doc_id);
        } else {
          result.graph[ref_doc_id_or.ValueOrDie()].insert(doc_id);
        }
      }
    }
  }
  return result;
}

libtextclassifier3::StatusOr<std::unordered_set<DocumentId>>
InMemoryIcingSearchEngine::GetDocIdsForDeletePropagation(
    std::unordered_set<DocumentId>&& doc_ids_to_delete) {
  if (!enable_delete_propagation_) {
    // If delete propagation is not enabled, then just return the original doc
    // ids to delete.
    return doc_ids_to_delete;
  }

  // 1. Build the graph, with the direction from parent to child.
  DependencyGraphResult dependency_graph_result = BuildDependencyGraph();
  if (!dependency_graph_result.unsatisfied_doc_ids.empty()) {
    return absl_ports::InvalidArgumentError(
        "In-memory Icing has documents with unsatisfied dependency. This "
        "should not happen.");
  }

  // 2. Propagate to children.
  return TraverseToAllChildren(dependency_graph_result.graph,
                               std::move(doc_ids_to_delete));
}

libtextclassifier3::StatusOr<
    std::vector<InMemoryIcingSearchEngine::ScoredDocumentId>>
InMemoryIcingSearchEngine::Score(
    const std::vector<DocumentId>& doc_ids,
    ScoringSpecProto::RankingStrategy::Code rank_by) const {
  std::vector<ScoredDocumentId> scored_document_ids;
  scored_document_ids.reserve(doc_ids.size());
  for (DocumentId doc_id : doc_ids) {
    int score;
    if (rank_by == ScoringSpecProto::RankingStrategy::NONE) {
      // If the ranking strategy is NONE, then just return the document id
      // without any score, which means the score will be 0.
      score = 0;
    } else if (rank_by == ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE) {
      score = documents_[doc_id].document.score();
    } else if (rank_by ==
               ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP) {
      score = (int)documents_[doc_id].document.creation_timestamp_ms();
    } else {
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "Unsupported ranking strategy: ",
          ScoringSpecProto::RankingStrategy::Code_Name(rank_by)));
    }
    scored_document_ids.push_back(
        ScoredDocumentId{.document_id = doc_id, .score = score});
  }
  return scored_document_ids;
}

SearchResultProto InMemoryIcingSearchEngine::Paginate(
    std::unordered_map<int, PaginationState>::iterator& paginated_state_itr)
    const {
  int next_page_token = paginated_state_itr->first;
  std::vector<NestedScoredDocumentId>& doc_ids =
      paginated_state_itr->second.remaining_results;
  int remaining_results_size = static_cast<int>(doc_ids.size());
  int page_size = paginated_state_itr->second.page_size;

  SearchResultProto search_result_proto;
  std::vector<NestedScoredDocumentId> doc_ids_for_page;
  // If all of the results can fit in one page, then don't add to the map of
  // tokens to results, and erase the entry in the map if it exists.
  if (remaining_results_size <= page_size) {
    search_result_proto.set_next_page_token(0);
    doc_ids_for_page = std::move(doc_ids);
    // The ids are inserted in reverse order, so we need to reverse them
    // back before retrieving the documents.
    std::reverse(doc_ids_for_page.begin(), doc_ids_for_page.end());
    paginated_results_map_.erase(next_page_token);
  } else {
    search_result_proto.set_next_page_token(next_page_token);
    // The ids are inserted in reverse order, so we need to traverse and
    // add in reverse order.
    doc_ids_for_page.reserve(page_size);
    std::move(doc_ids.rbegin(), doc_ids.rbegin() + page_size,
              std::back_inserter(doc_ids_for_page));
    // Resize to remove the ids that we are going to return documents for in
    // this page.
    doc_ids.resize(remaining_results_size - page_size);
  }

  std::vector<SearchResultProto::ResultProto> results =
      FetchNestedResultDocuments(std::move(doc_ids_for_page));
  search_result_proto.mutable_results()->Add(results.begin(), results.end());

  return search_result_proto;
}

}  // namespace lib
}  // namespace icing
