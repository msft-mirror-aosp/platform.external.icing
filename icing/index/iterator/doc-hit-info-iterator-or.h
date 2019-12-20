// Copyright (C) 2019 Google LLC
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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_OR_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_OR_H_

#include <cstdint>
#include <string>

#include "icing/index/iterator/doc-hit-info-iterator.h"

namespace icing {
namespace lib {

// Given n iterators, will decide what the fastest Or-iterator implementation
// will be.
std::unique_ptr<DocHitInfoIterator> CreateOrIterator(
    std::vector<std::unique_ptr<DocHitInfoIterator>> iterators);

// Iterate over a logical OR of two child iterators.
class DocHitInfoIteratorOr : public DocHitInfoIterator {
 public:
  explicit DocHitInfoIteratorOr(std::unique_ptr<DocHitInfoIterator> left_it,
                                std::unique_ptr<DocHitInfoIterator> right_it);

  libtextclassifier3::Status Advance() override;

  int32_t GetNumBlocksInspected() const override;

  int32_t GetNumLeafAdvanceCalls() const override;

  std::string ToString() const override;

 private:
  std::unique_ptr<DocHitInfoIterator> left_;
  std::unique_ptr<DocHitInfoIterator> right_;
  DocumentId left_document_id_ = kMaxDocumentId;
  DocumentId right_document_id_ = kMaxDocumentId;
};

// Iterate over a logical OR of multiple child iterators.
//
// NOTE: DocHitInfoIteratorOr is a faster alternative to OR exactly 2 iterators.
class DocHitInfoIteratorOrNary : public DocHitInfoIterator {
 public:
  explicit DocHitInfoIteratorOrNary(
      std::vector<std::unique_ptr<DocHitInfoIterator>> iterators);

  libtextclassifier3::Status Advance() override;

  int32_t GetNumBlocksInspected() const override;

  int32_t GetNumLeafAdvanceCalls() const override;

  std::string ToString() const override;

 private:
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_OR_H_
