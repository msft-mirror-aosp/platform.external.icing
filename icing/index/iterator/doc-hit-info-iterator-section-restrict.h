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
#include <unordered_map>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
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
      std::set<std::string> target_sections, int64_t current_time_ms);

  explicit DocHitInfoIteratorSectionRestrict(
      std::unique_ptr<DocHitInfoIterator> delegate,
      const DocumentStore* document_store, const SchemaStore* schema_store,
      const SearchSpecProto& search_spec,
      int64_t current_time_ms);

  libtextclassifier3::Status Advance() override;

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override;

  int32_t GetNumBlocksInspected() const override;

  int32_t GetNumLeafAdvanceCalls() const override;

  std::string ToString() const override;

  // Note that the DocHitInfoIteratorSectionRestrict is the only iterator that
  // should set filtering_section_mask, hence the received
  // filtering_section_mask is ignored and the filtering_section_mask passed to
  // the delegate will be set to hit_intersect_section_ids_mask_. This will
  // allow to filter the matching sections in the delegate.
  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask = kSectionIdMaskAll) const override {
    if (doc_hit_info_.document_id() == kInvalidDocumentId) {
      // Current hit isn't valid, return.
      return;
    }
    delegate_->PopulateMatchedTermsStats(
        matched_terms_stats,
        /*filtering_section_mask=*/hit_intersect_section_ids_mask_);
  }

 private:
  explicit DocHitInfoIteratorSectionRestrict(
      std::unique_ptr<DocHitInfoIterator> delegate,
      const DocumentStore* document_store, const SchemaStore* schema_store,
      std::unordered_map<std::string, std::set<std::string>>
      type_property_filters,
      std::unordered_map<std::string, SectionIdMask> type_property_masks,
      int64_t current_time_ms);
  // Calculates the section mask of allowed sections(determined by the property
  // filters map) for the given schema type and caches the same for any future
  // calls.
  //
  // Returns:
  //  - If type_property_filters_ has an entry for the given schema type or
  //    wildcard(*), return a bitwise or of section IDs in the schema type that
  //    that are also present in the relevant filter list.
  //  - Otherwise, return kSectionIdMaskAll.
  SectionIdMask ComputeAndCacheSchemaTypeAllowedSectionsMask(
      const std::string& schema_type);
  // Generates a section mask for the given schema type and the target sections.
  //
  // Returns:
  //  - A bitwise or of section IDs in the schema_type that that are also
  //    present in the target_sections list.
  //  - If none of the sections in the schema_type are present in the
  //    target_sections list, return kSectionIdMaskNone.
  // This is done by doing a bitwise or of the target section ids for the given
  // schema type.
  SectionIdMask GenerateSectionMask(const std::string& schema_type,
                                    const std::set<std::string>&
                                    target_sections) const;

  std::unique_ptr<DocHitInfoIterator> delegate_;
  const DocumentStore& document_store_;
  const SchemaStore& schema_store_;
  int64_t current_time_ms_;

  // Map of property filters per schema type. Supports wildcard(*) for schema
  // type that will apply to all schema types that are not specifically
  // specified in the mapping otherwise.
  std::unordered_map<std::string, std::set<std::string>>
      type_property_filters_;
  // Mapping of schema type to the section mask of allowed sections for that
  // schema type. This section mask is lazily calculated based on the specified
  // property filters and cached for any future use.
  std::unordered_map<std::string, SectionIdMask> type_property_masks_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_SECTION_RESTRICT_H_
