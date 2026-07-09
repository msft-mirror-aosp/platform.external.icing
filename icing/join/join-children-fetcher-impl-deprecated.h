// Copyright (C) 2023 Google LLC
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

#ifndef ICING_JOIN_JOIN_CHILDREN_FETCHER_IMPL_DEPRECATED_H_
#define ICING_JOIN_JOIN_CHILDREN_FETCHER_IMPL_DEPRECATED_H_

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/proto/search.pb.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// A class that provides the GetChildren method for joins to fetch all children
// documents given a parent document id. Only QualifiedIdJoinIndexImplV1 and
// QualifiedIdJoinIndexImplV2 will use this class, since we can construct the
// map without parent document id available.
//
// Internally, the class maintains a map for each joinable value type that
// groups children according to the joinable values. Currently we only support
// QUALIFIED_ID joining, in which the joinable value type is document id.
class JoinChildrenFetcherImplDeprecated : public JoinChildrenFetcher {
 public:
  // Creates JoinChildrenFetcherImplDeprecated.
  //
  // Returns:
  //   - A JoinChildrenFetcherImplDeprecated instance on success.
  //   - UNIMPLEMENTED_ERROR if the join type specified by join_spec is not
  //     supported.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<JoinChildrenFetcherImplDeprecated>>
  Create(const JoinSpecProto& join_spec,
         std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>&&
             map_joinable_qualified_id);

  ~JoinChildrenFetcherImplDeprecated() override = default;

  libtextclassifier3::StatusOr<std::vector<ScoredDocumentHit>> GetChildren(
      DocumentId parent_doc_id) const override;

 private:
  explicit JoinChildrenFetcherImplDeprecated(
      const JoinSpecProto& join_spec,
      std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>&&
          map_joinable_qualified_id)
      : JoinChildrenFetcher(join_spec),
        map_joinable_qualified_id_(std::move(map_joinable_qualified_id)) {}

  // The map that groups children by qualified id used to support QualifiedId
  // joining. The joining type is document id.
  std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      map_joinable_qualified_id_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_JOIN_CHILDREN_FETCHER_IMPL_DEPRECATED_H_
