// Copyright (C) 2025 Google LLC
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

#ifndef THIRD_PARTY_ICING_JOIN_DOCUMENT_DEPENDENT_GRAPH_H_
#define THIRD_PARTY_ICING_JOIN_DOCUMENT_DEPENDENT_GRAPH_H_

#include <memory>
#include <utility>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/graph/graph-interface.h"
#include "third_party/icing/join/qualified-id-join-index.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"

namespace icing {
namespace lib {

// Document dependent graph using qualified id join index v3 as the data source.
// This class is just an interface for the dependent graph and does not own any
// data. Instead, it reads data from the given data sources and returns
// dependent relations.
//
// Dependent graph:
// - The nodes are the documents, and the node id type is DocumentId.
// - The edges are directed and represent the dependencies between documents.
//   For example, an edge A -> B means:
//   - A is a dependency of B.
//   - B is a dependent of A.
//   - If A is deleted or expired, then B should be deleted or expired as well.
class DocumentDependentGraph : public graph::GraphInterface<DocumentId> {
 public:
  // Creates a document dependent graph object.
  //
  // Returns:
  //   - Non-null unique pointer of DocumentDependentGraph on success.
  //   - FAILED_PRECONDITION_ERROR if any data source is null.
  //   - INVALID_ARGUMENT_ERROR if join_index is not version V3.
  //   - Any errors from the underlying data source.
  static libtextclassifier3::StatusOr<std::unique_ptr<DocumentDependentGraph>>
  Create(const SchemaStore* schema_store, const DocumentStore* doc_store,
         const QualifiedIdJoinIndex* join_index);

  int GetNumNodes() const override;

  libtextclassifier3::StatusOr<std::unique_ptr<EdgeIteratorIf>>
  GetEdgesIterator(int node_id) const override;

 private:
  class EdgeIterator : public EdgeIteratorIf {
   public:
    explicit EdgeIterator(
        const SchemaStore& schema_store, const DocumentStore& doc_store,
        QualifiedIdJoinIndex::DocumentJoinIdPairArrayView join_data_array_view)
        : schema_store_(schema_store),
          doc_store_(doc_store),
          join_data_array_view_(std::move(join_data_array_view)),
          curr_idx_(-1),
          curr_(kInvalidDocumentId) {}

    libtextclassifier3::Status Advance() override;

    const DocumentId& Get() const override { return curr_; }

   private:
    const SchemaStore& schema_store_;  // Does not own.
    const DocumentStore& doc_store_;   // Does not own.

    QualifiedIdJoinIndex::DocumentJoinIdPairArrayView join_data_array_view_;
    int curr_idx_;
    DocumentId curr_;
  };

  explicit DocumentDependentGraph(const SchemaStore* schema_store,
                                  const DocumentStore* doc_store,
                                  const QualifiedIdJoinIndex* join_index)
      : schema_store_(*schema_store),
        doc_store_(*doc_store),
        join_index_(*join_index) {}

  const SchemaStore& schema_store_;         // Does not own.
  const DocumentStore& doc_store_;          // Does not own.
  const QualifiedIdJoinIndex& join_index_;  // Does not own.
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_JOIN_DOCUMENT_DEPENDENT_GRAPH_H_
