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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_SECTION_RESTRICT_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_SECTION_RESTRICT_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

// A iterator that helps filter for DocHitInfos whose term was in a section
// named target_section.
//
// NOTE: This is a little different from the DocHitInfoIteratorFilter class.
// That class is meant to be applied to the root of a query tree and filter over
// all results at the end. This class is more used in the limited scope of a
// term or a small group of terms.
class DocHitInfoIteratorSectionRestrict : public DocHitInfoIterator {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the one constructed.
  explicit DocHitInfoIteratorSectionRestrict(
      std::unique_ptr<DocHitInfoIterator> delegate,
      const DocumentStore* document_store, const SchemaStore* schema_store,
      std::string_view target_section);

  libtextclassifier3::Status Advance() override;

  int32_t GetNumBlocksInspected() const override;

  int32_t GetNumLeafAdvanceCalls() const override;

  std::string ToString() const override;

  // NOTE: currently, section restricts does decide which documents to
  // return, but doesn't impact the relevance score of a document.
  // TODO(b/173156803): decide whether we want to filter the matched_terms_stats
  // for the restricted sections.
  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats) const override {
    delegate_->PopulateMatchedTermsStats(matched_terms_stats);
  }

 private:
  std::unique_ptr<DocHitInfoIterator> delegate_;
  const DocumentStore& document_store_;
  const SchemaStore& schema_store_;

  // Ensure that this does not outlive the underlying string value.
  std::string_view target_section_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_SECTION_RESTRICT_H_
