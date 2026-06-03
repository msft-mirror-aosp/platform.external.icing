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

#ifndef ICING_MONKEY_TEST_IN_MEMORY_ICING_SEARCH_ENGINE_H_
#define ICING_MONKEY_TEST_IN_MEMORY_ICING_SEARCH_ENGINE_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-id.h"
#include "icing/tokenization/language-segmenter.h"

namespace icing {
namespace lib {

class InMemoryIcingSearchEngine {
 public:
  struct PickDocumentResult {
    std::string name_space;
    std::string uri;
    // document is empty if and only if such (name_space, uri) is not alive
    // in the in-memory icing.
    std::optional<DocumentProto> document;
  };

  struct JoinQuerySpec {
    // - prev_join_prop_expr: the join property expression for the UPPER
    //   level's results to join with this level. Currently, this field should
    //   always be kQualifiedIdExpr, as we only support qualified id join.
    // - curr_join_prop_expr: the join property expression for this level's
    //   results to join with the UPPER level.
    //
    // IOW, these 2 join property expressions are used to connect the current
    // level with the UPPER level.
    std::string prev_join_prop_expr;
    std::string curr_join_prop_expr;

    // The query node of the current level.
    std::unique_ptr<MonkeyAbstractQueryNode> curr_query_node;
  };

  InMemoryIcingSearchEngine(MonkeyTestRandomEngine* random);

  uint32_t GetNumAliveDocuments() const { return existing_doc_ids_.size(); }

  const SchemaProto *GetSchema() const { return schema_.get(); }

  void SetSchema(SchemaProto schema);

  // Randomly pick a document from the in-memory Icing for monkey testing.
  //
  // p_alive: chance of getting an alive document.
  // p_all:   chance of getting a document that has ever been "Put" before,
  //          including already "Delete"d documents.
  // p_other: chance of getting a random namespace + uri that has never been
  //          "Put" before.
  //
  //  p_alive, p_all, and p_other is required to be positive and sum to 1.
  //  Otherwise, they will be normalized to ensure this.
  //
  // Returns an instance of PickDocumentResult.
  PickDocumentResult RandomPickDocument(float p_alive, float p_all,
                                        float p_other) const;

  const std::vector<DocumentId>& GetExistingDocumentIds() const {
    return existing_doc_ids_;
  }

  const MonkeyTokenizedDocument& GetDocumentById(DocumentId doc_id) const {
    return documents_[doc_id];
  }

  const LanguageSegmenter* GetLanguageSegmenter() const {
    return language_segmenter_.get();
  }

  // Puts the document into the in-memory Icing. If the (namespace, uri) pair
  // already exists, the old document will be overwritten.
  void Put(const MonkeyTokenizedDocument &document);

  std::unordered_set<std::string> GetAllNamespaces() const;

  // Deletes the Document specified by the given (namespace, uri) pair.
  //
  // Returns:
  //   OK on success
  //   NOT_FOUND if no document exists with namespace, uri
  libtextclassifier3::Status Delete(const std::string &name_space,
                                    const std::string &uri);

  // Deletes all Documents belonging to the specified namespace.
  //
  // Returns:
  //   The number of deleted documents on success
  //   INTERNAL_ERROR if there are inconsistencies in the in-memory Icing
  libtextclassifier3::StatusOr<uint32_t> DeleteByNamespace(
      const std::string &name_space);

  // Deletes all Documents belonging to the specified type
  //
  // Returns:
  //   The number of deleted documents on success
  //   INTERNAL_ERROR if there are inconsistencies in the in-memory Icing
  libtextclassifier3::StatusOr<uint32_t> DeleteBySchemaType(
      const std::string &schema_type);

  // Deletes all Documents that match the query specified in the
  // MonkeyAbstractQueryNode. Check the comments of Search() for the supported
  // query types.
  //
  // Returns:
  //   The number of deleted documents on success
  //   INTERNAL_ERROR if there are inconsistencies in the in-memory Icing
  libtextclassifier3::StatusOr<uint32_t> DeleteByQuery(
      const MonkeyAbstractQueryNode* node);

  // Retrieves documents according to MonkeyAbstractQueryNode, which is a
  // structured representation of a query.
  // - Currently, only the "query", "term_match_type",
  //   "embedding_query_vectors", and "embedding_query_metric_type" fields are
  //   recognized by the in-memory Icing.
  // - Qualified id join search (with nested queries) is also supported. For a
  //   non-join query, nested_queries will be empty. For a join query,
  //   nested_queries will contain the nested query specs for multiple nested
  //   levels. For example:
  //
  //       base_query_node
  //           JOINS (nested_queries[0].prev_join_prop_expr,
  //                  nested_queries[0].curr_join_prop_expr)
  //       nested_queries[0].curr_query_node
  //           JOINS (nested_queries[1].prev_join_prop_expr,
  //                  nested_queries[1].curr_join_prop_expr)
  //       nested_queries[1].curr_query_node
  //           ...
  //
  // - For term based queries, only single term queries with possible section
  //   restrictions are supported.
  // - For embedding based queries, only the fixed format of
  //   `semanticSearch(getEmbeddingParameter(0), low, high)` is supported, where
  //   `low` and `high` are floating point numbers that specify the score range.
  //   Section restrictions are also recognized.
  libtextclassifier3::StatusOr<std::vector<SearchResultProto::ResultProto>>
  Search(const MonkeyAbstractQueryNode* base_query_node,
         const std::vector<JoinQuerySpec>& nested_queries) const;

  // Returns all join properties in the in-memory Icing. This is used for
  // IcingMonkeyTestRunner to generate join specs.
  std::vector<std::string> GetAllJoinProperties() const;

  struct PropertyIndexInfo {
    // Data type of the property.
    PropertyConfigProto::DataType::Code data_type =
        PropertyConfigProto::DataType::UNKNOWN;

    // The term match type if the property is of type string.
    TermMatchType::Code term_match_type = TermMatchType::UNKNOWN;

    // The tokenizer type if the property is of type string.
    StringIndexingConfig::TokenizerType::Code tokenizer_type =
        StringIndexingConfig::TokenizerType::NONE;

    // The quantization type if the property is of type vector.
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type =
        EmbeddingIndexingConfig::QuantizationType::NONE;

    // The embedding indexing type if the property is of type vector.
    EmbeddingIndexingConfig::EmbeddingIndexingType::Code
        embedding_indexing_type =
            EmbeddingIndexingConfig::EmbeddingIndexingType::UNKNOWN;

    // The numeric match type if the property is of type int64.
    IntegerIndexingConfig::NumericMatchType::Code numeric_match_type =
        IntegerIndexingConfig::NumericMatchType::UNKNOWN;

    // Whether the property is indexable.
    bool indexable = false;
  };
  libtextclassifier3::StatusOr<PropertyIndexInfo> GetPropertyIndexInfo(
      const std::string& schema_type, std::string_view property_path) const;

  struct PropertyJoinableInfo {
    // Data type of the property.
    PropertyConfigProto::DataType::Code data_type;
    // The joinable value type.
    JoinableConfig::ValueType::Code value_type =
        JoinableConfig::ValueType::NONE;
    // The delete propagation type.
    JoinableConfig::DeletePropagationType::Code delete_propagation_type =
        JoinableConfig::DeletePropagationType::NONE;
  };
  std::optional<PropertyJoinableInfo> GetPropertyJoinableInfo(
      const std::string& schema_type, std::string_view property_path) const;

 private:
  // Nested document id structure for join. It also supports normal search
  // without joining, and in this case, the nested document ids will be empty.
  struct NestedResultDocumentId {
    DocumentId document_id;
    std::vector<NestedResultDocumentId> nested_document_ids;
  };

  // An intermediate result type for join, mapping parent doc id -> a list of
  // NestedResultDocumentId of its children.
  //
  // Note: we use NestedResultDocumentId for the children since it is possible
  //   to have multiple levels of join.
  using JoinedNestedResultDocumentIdMap =
      std::unordered_map<DocumentId, std::vector<NestedResultDocumentId>>;

  // Finds and returns the internal document id for the document identified by
  // the given key (namespace, uri)
  //
  // Returns:
  //   The document id found on success
  //   NOT_FOUND if the key doesn't exist or doc has been deleted
  libtextclassifier3::StatusOr<DocumentId> InternalGet(
      const std::string& name_space, const std::string& uri) const;

  // A helper method for DeleteByQuery and Search to get matched internal doc
  // ids.
  //
  // Note: join search is supported, and the nested structure is wrapped in
  //   NestedResultDocumentId. Join iteration is done in reverse order of the
  //   nested spec structure (each iteration is a single level of join, see
  //   InternalSingleLevelJoinSearch).
  //
  //   For example, search_spec0 JOIN search_spec1 JOIN search_spec2.
  //   Monkey test will generate:
  //   - base_query_node: search_spec0
  //   - nested_queries:
  //     - JoinQuerySpec(prev_prop_expr_1, curr_prop_expr_1, query_node_1)
  //        - query_node_1 is search_spec1.
  //        - prev_prop_expr_1 and curr_prop_expr_1 are join properties for
  //          the results of base_query_node (parent) and query_node_1 (child).
  //     - JoinQuerySpec(prev_prop_expr_2, curr_prop_expr_2, query_node_2)
  //        - query_node_2 is search_spec2.
  //        - prev_prop_expr_2 and curr_prop_expr_2 are join properties for
  //          the results of query_node_1 (parent) and query_node_2 (child).
  //
  //   The execution flow is:
  //   - query_node_2 (search_spec2) is evaluated first.
  //     - Get the 2nd level matched doc ids.
  //     - Join property values are read out, and the 2nd level matched doc ids
  //       are grouped by the join property values.
  //     - Obtain the grouped doc ids:
  //       join value (for the upper level to use) -> a list of 2nd level docs
  //   - query_node_1 (search_spec1) is evaluated next.
  //     - Get the 1st level matched doc ids.
  //     - Join the 1st level matched doc ids with the grouped doc ids obtained
  //       from the previous step.
  //     - Join property values (of the 1st level docs) are read out, and the
  //       1st level matched doc ids are grouped by the join property values.
  //     - Obtain the grouped doc ids:
  //       join value (for the upper level to use) -> a list of 1st level docs
  //   - Finally, the 0th level base_query_node (top most level, search_spec1)
  //     is evaluated.
  //     - Get the 0th level matched doc ids.
  //     - Join the 0th level matched doc ids with the grouped doc ids obtained
  //       from the previous step.
  //   - Return the joined nested result doc ids.
  libtextclassifier3::StatusOr<std::vector<NestedResultDocumentId>>
  InternalSearch(const MonkeyAbstractQueryNode* base_query_node,
                 const std::vector<JoinQuerySpec>& nested_queries) const;

  // A helper method to do a single level of join search, according to the given
  // JoinQuerySpec.
  //
  // Args:
  // - join_query_spec: the join query spec for the current level.
  //   - curr_query_node represents the query of this level.
  //   - curr_join_prop_expr is the property for this level's results to join
  //     with the UPPER level.
  //   - prev_join_prop_expr is the join property expression for the UPPER
  //     level's results to join with this level. Currently, this field should
  //     always be kQualifiedIdExpr, as we only support qualified id join.
  // - child_map: the join result of the PREVIOUS iteration. Note: "previous
  //   iteration" means the lower level as we iterate nested queries in reverse
  //   order.
  //   - JoinedNestedResultDocumentIdMap maps parent doc id -> a list of
  //     NestedResultDocumentId of its children.
  //   - child_map was obtained from the previous iteration (next level down of
  //     the join structure). Its key "parent doc id" connects to the current
  //     level's result doc ids.
  //   - The result doc ids from obtained from join_query_spec.curr_query_node
  //     of this level will be joined with child_map's key (parent doc id).
  //
  // Output:
  // - join_query_spec.curr_join_prop_expr is the join property to be used for
  //   joining docs from this level with the UPPER level, which will be
  //   evaluated in the next iteration.
  // - At this level, read out the join property values from the matched
  //   documents according to join_query_spec.curr_query_node, and group the
  //   results by the values.
  // - Return the grouped results. It will be used as "child_map" in the next
  //   iteration (upper level).
  libtextclassifier3::StatusOr<JoinedNestedResultDocumentIdMap>
  InternalSingleLevelJoinSearch(
      const JoinQuerySpec& join_query_spec,
      JoinedNestedResultDocumentIdMap&& child_map) const;

  // Helper function to recursively fetch the nested documents according to the
  // nested document ids.
  std::vector<SearchResultProto::ResultProto> FetchNestedResultDocuments(
      const std::vector<NestedResultDocumentId>& nested_result_doc_ids) const;

  // Helper function to recursively unzip the nested document ids into a set of
  // document ids.
  void UnzipNestedResultDocumentIds(
      const std::vector<NestedResultDocumentId>& nested_result_doc_ids,
      std::unordered_set<DocumentId>& doc_ids_out) const;

  libtextclassifier3::StatusOr<const PropertyConfigProto*> GetPropertyConfig(
      const std::string& schema_type, const std::string& property_name) const;

  // Does not own.
  MonkeyTestRandomEngine* random_;

  // Language segmenter for tokenization.
  std::unique_ptr<LanguageSegmenter> language_segmenter_;

  std::vector<MonkeyTokenizedDocument> documents_;
  std::vector<DocumentId> existing_doc_ids_;

  // A map from namespaces to uris and then from uris to internal document ids,
  // which is used for fast lookups.
  std::unordered_map<std::string, std::unordered_map<std::string, DocumentId>>
      namespace_uri_docid_map_;

  std::unique_ptr<SchemaProto> schema_;
  // A map that maps from (schema_type, property_name) to the corresponding
  // PropertyConfigProto.
  std::unordered_map<
      std::string, std::unordered_map<std::string, const PropertyConfigProto&>>
      property_config_map_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_IN_MEMORY_ICING_SEARCH_ENGINE_H_
