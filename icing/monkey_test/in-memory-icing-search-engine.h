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
  // Currently, only the "query", "term_match_type", "embedding_query_vectors",
  // and "embedding_query_metric_type" fields are recognized by the in-memory
  // Icing.
  //
  // For term based queries, only single term queries with possible section
  // restrictions are supported.
  //
  // For embedding based queries, only the fixed format of
  // `semanticSearch(getEmbeddingParameter(0), low, high)` is supported, where
  // `low` and `high` are floating point numbers that specify the score range.
  // Section restrictions are also recognized.
  libtextclassifier3::StatusOr<std::vector<DocumentProto>> Search(
      const MonkeyAbstractQueryNode* node) const;

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

 private:
  // Finds and returns the internal document id for the document identified by
  // the given key (namespace, uri)
  //
  // Returns:
  //   The document id found on success
  //   NOT_FOUND if the key doesn't exist or doc has been deleted
  libtextclassifier3::StatusOr<DocumentId> InternalGet(
      const std::string &name_space, const std::string &uri) const;

  libtextclassifier3::StatusOr<const PropertyConfigProto *> GetPropertyConfig(
      const std::string &schema_type, const std::string &property_name) const;

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
