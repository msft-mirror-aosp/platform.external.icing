// Copyright (C) 2024 Google LLC
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

#ifndef ICING_JOIN_JOIN_CHILDREN_FETCHER_IMPL_V3_H_
#define ICING_JOIN_JOIN_CHILDREN_FETCHER_IMPL_V3_H_

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

// A class that provides the GetChildren method for joins to fetch all children
// documents given a parent document id.
// - It does lazy lookup and caches the results after lookup and filter.
// - Only QualifiedIdJoinIndexImplV3 will use this class.
class JoinChildrenFetcherImplV3 : public JoinChildrenFetcher {
 public:
  // Creates JoinChildrenFetcherImplV3.
  //
  // Returns:
  //   - A JoinChildrenFetcherImplV3 instance on success.
  //   - FAILED_PRECONDITION_ERROR if any of the input pointer is null
  //   - UNIMPLEMENTED_ERROR if the join type specified by join_spec is not
  //     supported.
  //   - INVALID_ARGUMENT_ERROR if qualified_id_join_index is not v3.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<JoinChildrenFetcherImplV3>>
  Create(const JoinSpecProto& join_spec, const SchemaStore* schema_store,
         const DocumentStore* doc_store,
         const QualifiedIdJoinIndex* qualified_id_join_index,
         int64_t current_time_ms,
         std::vector<ScoredDocumentHit>&& child_scored_document_hits);

  ~JoinChildrenFetcherImplV3() override = default;

  libtextclassifier3::StatusOr<std::vector<ScoredDocumentHit>> GetChildren(
      DocumentId parent_doc_id) const override;

 private:
  explicit JoinChildrenFetcherImplV3(
      const JoinSpecProto& join_spec,
      const QualifiedIdJoinIndex* qualified_id_join_index,
      std::unordered_map<DocumentJoinIdPair, ScoredDocumentHit,
                         DocumentJoinIdPair::Hasher>&&
          child_join_id_pair_to_scored_document_hit_map)
      : JoinChildrenFetcher(join_spec),
        qualified_id_join_index_(*qualified_id_join_index),
        child_join_id_pair_to_scored_document_hit_map_(
            std::move(child_join_id_pair_to_scored_document_hit_map)) {}

  const QualifiedIdJoinIndex& qualified_id_join_index_;  // Does not own.

  // Map the child join id pair to its scored document hit. It will be used to
  // filter the child join id pairs. See GetChildren() implementation for more
  // details.
  std::unordered_map<DocumentJoinIdPair, ScoredDocumentHit,
                     DocumentJoinIdPair::Hasher>
      child_join_id_pair_to_scored_document_hit_map_;

  // The cache for storing parent document id to its children's scored document
  // hits. GetChildren() will first check the cache to see if the parent
  // document id is already in the cache. If not, it will do a lookup and
  // filter the children (via child_join_id_pair_to_scored_document_hit_map_)
  // and then store the result in the cache.
  mutable std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      cached_parent_to_children_map_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_JOIN_CHILDREN_FETCHER_IMPL_V3_H_
