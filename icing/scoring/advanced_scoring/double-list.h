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

#ifndef ICING_SCORING_ADVANCED_SCORING_DOUBLE_LIST_H_
#define ICING_SCORING_ADVANCED_SCORING_DOUBLE_LIST_H_

#include <cstddef>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

// Represents the kDoubleList type, which can either own its data or provide a
// non-owning view of existing data.
class DoubleList {
 public:
  using value_type = double;

  // Creates a list by taking ownership of an existing vector's data via move.
  explicit DoubleList(std::vector<double>&& vec) : storage_(std::move(vec)) {}

  // Creates a non-owning view of an external data buffer.
  // The caller must ensure the lifetime of "data" exceeds the lifetime of this
  // DoubleList.
  explicit DoubleList(const double* data, size_t size)
      : storage_(DataView(data, size)) {}

  // Creates an empty list. Represents as an empty non-owning view by default.
  explicit DoubleList() : storage_(DataView()) {}

  // Disallow copy but allow move.
  DoubleList(const DoubleList&) = delete;
  DoubleList& operator=(const DoubleList&) = delete;
  DoubleList(DoubleList&&) = default;
  DoubleList& operator=(DoubleList&&) = default;

  const double* data() const {
    return std::visit(
        [](const auto& arg) -> const double* { return arg.data(); }, storage_);
  }

  size_t size() const {
    return std::visit([](const auto& arg) -> size_t { return arg.size(); },
                      storage_);
  }

  const double* begin() const { return data(); }

  const double* end() const { return data() + size(); }

  bool empty() const { return size() == 0; }

  // Releases the ownership of the internal vector, if owned.
  // If not owned, returns a *new* vector containing a copy of the viewed data.
  std::vector<double> ReleaseVector() && {
    return std::visit(
        [](auto&& arg) -> std::vector<double> {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, std::vector<double>>) {
            return std::forward<decltype(arg)>(arg);
          } else {
            return std::vector<double>(arg.data(), arg.data() + arg.size());
          }
        },
        std::move(storage_));
  }

 private:
  // Simple class to represent a non-owning view of data
  class DataView {
   public:
    // Default constructor represents an empty non-owning view.
    DataView() : DataView(nullptr, 0) {}
    DataView(const double* data, size_t size) : data_(data), size_(size) {}

    const double* data() const { return data_; }
    size_t size() const { return size_; }

   private:
    const double* data_ = nullptr;
    size_t size_ = 0;
  };

  // Storage can be either an owned vector or a non-owning view
  std::variant<std::vector<double>, DataView> storage_;
};

#endif  // ICING_SCORING_ADVANCED_SCORING_DOUBLE_LIST_H_
