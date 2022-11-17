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
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/monkey-test-generators.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/store/document-id.h"

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

  InMemoryIcingSearchEngine(MonkeyTestRandomEngine *random,
                            SchemaProto &&schema)
      : random_(random),
        schema_(std::make_unique<SchemaProto>(std::move(schema))) {}

  const SchemaProto *GetSchema() const { return schema_.get(); }

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
  //   OK on success
  //   NOT_FOUND if namespace doesn't exist
  libtextclassifier3::Status DeleteByNamespace(const std::string &name_space);

  // Deletes all Documents belonging to the specified type
  //
  // Returns:
  //   OK on success
  //   NOT_FOUND if schema type doesn't exist
  libtextclassifier3::Status DeleteBySchemaType(const std::string &schema_type);

 private:
  // Does not own.
  MonkeyTestRandomEngine *random_;

  std::vector<MonkeyTokenizedDocument> documents_;
  std::vector<DocumentId> existing_doc_ids_;
  // A map from namespaces to uris and then from uris to internal document ids,
  // which is used for fast lookups.
  std::unordered_map<std::string, std::unordered_map<std::string, DocumentId>>
      namespace_uri_docid_map;

  std::unique_ptr<SchemaProto> schema_;

  // Finds and returns the internal document id for the document identified by
  // the given key (namespace, uri)
  //
  // Returns:
  //   The document id found on success
  //   NOT_FOUND if the key doesn't exist or doc has been deleted
  libtextclassifier3::StatusOr<DocumentId> InternalGet(
      const std::string &name_space, const std::string &uri) const;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_IN_MEMORY_ICING_SEARCH_ENGINE_H_
