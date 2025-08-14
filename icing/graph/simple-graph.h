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

#ifndef THIRD_PARTY_ICING_GRAPH_SIMPLE_GRAPH_H_
#define THIRD_PARTY_ICING_GRAPH_SIMPLE_GRAPH_H_

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/absl_ports/canonical_errors.h"
#include "third_party/icing/graph/graph-interface.h"

namespace icing {
namespace lib {

namespace graph {

// A simple in-memory graph data structure without edge weights.
// - Node ids are from 0 to GetNumNodes() - 1.
// - Edges only contain the connected node ids, and are stored in adjacent
//   lists in memory.
class SimpleGraph : public GraphInterface<int> {
 public:
  // Builder class for SimpleGraph.
  class Builder {
   public:
    explicit Builder(int num_nodes) : out_(num_nodes) {}

    // Builds the graph. It is undefined behavior to use the builder after this
    // call.
    SimpleGraph Build() { return SimpleGraph(std::move(out_)); }

    // Adds an edge from node u to node v.
    //
    // REQUIRES: 0 <= u, v < num_nodes.
    Builder& AddEdge(int u, int v) {
      out_[u].insert(v);
      return *this;
    }

   private:
    std::vector<std::unordered_set<int>> out_;
  };

  int GetNumNodes() const override { return out_edges_.size(); }

  libtextclassifier3::StatusOr<std::unique_ptr<EdgeIteratorIf>>
  GetEdgesIterator(int node_id) const override {
    if (node_id < 0 || node_id >= GetNumNodes()) {
      return absl_ports::InvalidArgumentError("Invalid node id.");
    }
    return std::make_unique<EdgeIterator>(out_edges_[node_id].cbegin(),
                                          out_edges_[node_id].size());
  }

 private:
  class EdgeIterator : public EdgeIteratorIf {
   public:
    explicit EdgeIterator(std::unordered_set<int>::const_iterator it, int len)
        : it_(std::move(it)), len_(len), num_advanced_(0) {}

    libtextclassifier3::Status Advance() override {
      if (num_advanced_ >= len_) {
        return absl_ports::OutOfRangeError("No more edges to advance to.");
      }
      if (num_advanced_ != 0) {
        ++it_;
      }
      ++num_advanced_;
      return libtextclassifier3::Status::OK;
    }

    const int& Get() const override { return *it_; }

   private:
    std::unordered_set<int>::const_iterator it_;
    int len_;
    int num_advanced_;
  };

  explicit SimpleGraph(std::vector<std::unordered_set<int>>&& out_edges)
      : out_edges_(std::move(out_edges)) {}

  std::vector<std::unordered_set<int>> out_edges_;
};

}  // namespace graph

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_GRAPH_SIMPLE_GRAPH_H_
