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

#ifndef ICING_GRAPH_GRAPH_INTERFACE_H_
#define ICING_GRAPH_GRAPH_INTERFACE_H_

#include <memory>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"

namespace icing {
namespace lib {

namespace graph {

// Graph interface with integer node id and given EdgeType.
template <typename EdgeType>
class GraphInterface {
 public:
  // Edge iterator interface.
  class EdgeIteratorIf {
   public:
    virtual ~EdgeIteratorIf() = default;

    // Advances to the next edge.
    //
    // Returns:
    // - OK if successfully advanced to the next edge.
    // - RESOURCE_EXHAUSTED_ERROR if there is no more edge to advance to.
    // - Any other errors from the underlying implementation.
    virtual libtextclassifier3::Status Advance() = 0;

    // Gets the current edge.
    //
    // REQUIRES: preceding Advance() succeeded.
    virtual const EdgeType& Get() const = 0;
  };

  virtual ~GraphInterface() = default;

  virtual int GetNumNodes() const = 0;

  // Returns an iterator to the (out) edges of the given node.
  //
  // Returns:
  // - On success, a non-null unique pointer of EdgeIteratorIf. If there is no
  //   edge for a valid node_id, then it should still return a valid iterator
  //   with no edge to advance to.
  // - INVALID_ARGUMENT_ERROR if node_id is invalid.
  // - Any other errors from the underlying implementation.
  virtual libtextclassifier3::StatusOr<std::unique_ptr<EdgeIteratorIf>>
  GetEdgesIterator(int node_id) const = 0;

  // Add GetInEdgesIterator if needed.
};

}  // namespace graph

}  // namespace lib
}  // namespace icing

#endif  // ICING_GRAPH_GRAPH_INTERFACE_H_
