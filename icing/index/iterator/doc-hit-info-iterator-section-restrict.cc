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

#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

DocHitInfoIteratorSectionRestrict::DocHitInfoIteratorSectionRestrict(
    std::unique_ptr<DocHitInfoIterator> delegate,
    const DocumentStore* document_store, const SchemaStore* schema_store,
    std::set<std::string> target_sections, int64_t current_time_ms)
    : delegate_(std::move(delegate)),
      document_store_(*document_store),
      schema_store_(*schema_store),
      current_time_ms_(current_time_ms) {
  type_property_filters_[std::string(SchemaStore::kSchemaTypeWildcard)] =
      std::move(target_sections);
}

DocHitInfoIteratorSectionRestrict::DocHitInfoIteratorSectionRestrict(
    std::unique_ptr<DocHitInfoIterator> delegate,
    const DocumentStore* document_store, const SchemaStore* schema_store,
    const SearchSpecProto& search_spec,
    int64_t current_time_ms)
    : delegate_(std::move(delegate)),
      document_store_(*document_store),
      schema_store_(*schema_store),
      current_time_ms_(current_time_ms) {
  // TODO(b/294274922): Add support for polymorphism in type property filters.
  for (const TypePropertyMask& type_property_mask :
           search_spec.type_property_filters()) {
    type_property_filters_[type_property_mask.schema_type()] =
        std::set<std::string>(type_property_mask.paths().begin(),
                              type_property_mask.paths().end());
  }
}

DocHitInfoIteratorSectionRestrict::DocHitInfoIteratorSectionRestrict(
    std::unique_ptr<DocHitInfoIterator> delegate,
    const DocumentStore* document_store, const SchemaStore* schema_store,
    std::unordered_map<std::string, std::set<std::string>>
    type_property_filters,
    std::unordered_map<std::string, SectionIdMask> type_property_masks,
    int64_t current_time_ms)
    : delegate_(std::move(delegate)),
      document_store_(*document_store),
      schema_store_(*schema_store),
      current_time_ms_(current_time_ms),
      type_property_filters_(std::move(type_property_filters)),
      type_property_masks_(std::move(type_property_masks)) {}

SectionIdMask DocHitInfoIteratorSectionRestrict::GenerateSectionMask(
    const std::string& schema_type,
    const std::set<std::string>& target_sections) const {
  SectionIdMask section_mask = kSectionIdMaskNone;
  auto section_metadata_list_or =
      schema_store_.GetSectionMetadata(schema_type);
  if (!section_metadata_list_or.ok()) {
    // The current schema doesn't have section metadata.
    return kSectionIdMaskNone;
  }
  const std::vector<SectionMetadata>* section_metadata_list =
      section_metadata_list_or.ValueOrDie();
  for (const SectionMetadata& section_metadata : *section_metadata_list) {
    if (target_sections.find(section_metadata.path) !=
        target_sections.end()) {
      section_mask |= UINT64_C(1) << section_metadata.id;
    }
  }
  return section_mask;
}

SectionIdMask DocHitInfoIteratorSectionRestrict::
    ComputeAndCacheSchemaTypeAllowedSectionsMask(
    const std::string& schema_type) {
  if (const auto type_property_mask_itr =
      type_property_masks_.find(schema_type);
      type_property_mask_itr != type_property_masks_.end()) {
    return type_property_mask_itr->second;
  }

  // Section id mask of schema_type is never calculated before, so
  // calculate it here and put it into type_property_masks_.
  // - If type property filters of schema_type or wildcard (*) are
  //   specified, then create a mask according to the filters.
  // - Otherwise, create a mask to match all properties.
  SectionIdMask new_section_id_mask = kSectionIdMaskAll;
  if (const auto itr = type_property_filters_.find(schema_type);
      itr != type_property_filters_.end()) {
    // Property filters defined for given schema type
    new_section_id_mask = GenerateSectionMask(
        schema_type, itr->second);
  } else if (const auto wildcard_itr = type_property_filters_.find(
                 std::string(SchemaStore::kSchemaTypeWildcard));
             wildcard_itr != type_property_filters_.end()) {
    // Property filters defined for wildcard entry
    new_section_id_mask = GenerateSectionMask(
        schema_type, wildcard_itr->second);
  } else {
    // Do not cache the section mask if no property filters apply to this schema
    // type to avoid taking up unnecessary space.
    return kSectionIdMaskAll;
  }

  type_property_masks_[schema_type] = new_section_id_mask;
  return new_section_id_mask;
}

libtextclassifier3::Status DocHitInfoIteratorSectionRestrict::Advance() {
  doc_hit_info_ = DocHitInfo(kInvalidDocumentId);
  hit_intersect_section_ids_mask_ = kSectionIdMaskNone;
  while (delegate_->Advance().ok()) {
    DocumentId document_id = delegate_->doc_hit_info().document_id();

    SectionIdMask section_id_mask =
        delegate_->doc_hit_info().hit_section_ids_mask();

    auto data_optional = document_store_.GetAliveDocumentFilterData(
        document_id, current_time_ms_);
    if (!data_optional) {
      // Ran into some error retrieving information on this hit, skip
      continue;
    }

    // Guaranteed that the DocumentFilterData exists at this point
    SchemaTypeId schema_type_id = data_optional.value().schema_type_id();
    auto schema_type_or = schema_store_.GetSchemaType(schema_type_id);
    if (!schema_type_or.ok()) {
      // Ran into error retrieving schema type, skip
      continue;
    }
    const std::string* schema_type = std::move(schema_type_or).ValueOrDie();
    SectionIdMask allowed_sections_mask =
        ComputeAndCacheSchemaTypeAllowedSectionsMask(*schema_type);

    // A hit can be in multiple sections at once, need to check which of the
    // section ids match the sections allowed by type_property_masks_. This can
    // be done by doing a bitwise and of the section_id_mask in the doc hit and
    // the allowed_sections_mask.
    hit_intersect_section_ids_mask_ = section_id_mask & allowed_sections_mask;

    // Return this document if:
    //  - the sectionIdMask is not empty after applying property filters, or
    //  - no property filters apply for its schema type (allowed_sections_mask
    //    == kSectionIdMaskAll). This is needed to ensure that in case of empty
    //    query (which uses doc-hit-info-iterator-all-document-id), where
    //    section_id_mask (and hence hit_intersect_section_ids_mask_) is
    //    kSectionIdMaskNone, doc hits with no property restrictions don't get
    //    filtered out. Doc hits for schema types for whom property filters are
    //    specified will still get filtered out.
    if (allowed_sections_mask == kSectionIdMaskAll
        || hit_intersect_section_ids_mask_ != kSectionIdMaskNone) {
      doc_hit_info_ = delegate_->doc_hit_info();
      doc_hit_info_.set_hit_section_ids_mask(hit_intersect_section_ids_mask_);
      return libtextclassifier3::Status::OK;
    }
    // Didn't find a matching section name for this hit. Continue.
  }

  // Didn't find anything on the delegate iterator.
  return absl_ports::ResourceExhaustedError("No more DocHitInfos in iterator");
}

libtextclassifier3::StatusOr<DocHitInfoIterator::TrimmedNode>
DocHitInfoIteratorSectionRestrict::TrimRightMostNode() && {
  ICING_ASSIGN_OR_RETURN(TrimmedNode trimmed_delegate,
                         std::move(*delegate_).TrimRightMostNode());
  // TrimRightMostNode is only used by suggestion processor to process query
  // expression, so an entry for wildcard should always be present in
  // type_property_filters_ when code flow reaches here. If the InternalError
  // below is returned, that means TrimRightMostNode hasn't been called in the
  // right context.
  const auto it = type_property_filters_.find("*");
  if (it == type_property_filters_.end()) {
    return absl_ports::InternalError(
        "A wildcard entry should always be present in type property filters "
        "whenever TrimRightMostNode() is called for "
        "DocHitInfoIteratorSectionRestrict");
  }
  std::set<std::string>& target_sections = it->second;
  if (target_sections.empty()) {
    return absl_ports::InternalError(
        "Target sections should not be empty whenever TrimRightMostNode() is "
        "called for DocHitInfoIteratorSectionRestrict");
  }
  if (trimmed_delegate.iterator_ == nullptr) {
    // TODO(b/228240987): Update TrimmedNode and downstream code to handle
    // multiple section restricts.
    trimmed_delegate.target_section_ = std::move(*target_sections.begin());
    return trimmed_delegate;
  }
  trimmed_delegate.iterator_ =
      std::unique_ptr<DocHitInfoIteratorSectionRestrict>(
          new DocHitInfoIteratorSectionRestrict(
          std::move(trimmed_delegate.iterator_), &document_store_,
          &schema_store_, std::move(type_property_filters_),
          std::move(type_property_masks_), current_time_ms_));
  return std::move(trimmed_delegate);
}

int32_t DocHitInfoIteratorSectionRestrict::GetNumBlocksInspected() const {
  return delegate_->GetNumBlocksInspected();
}

int32_t DocHitInfoIteratorSectionRestrict::GetNumLeafAdvanceCalls() const {
  return delegate_->GetNumLeafAdvanceCalls();
}

std::string DocHitInfoIteratorSectionRestrict::ToString() const {
  std::string output = "";
  for (auto it = type_property_filters_.cbegin();
    it != type_property_filters_.cend(); it++) {
    std::string paths = absl_ports::StrJoin(it->second, ",");
    output += (it->first) + ":" + (paths) + "; ";
  }
  std::string result = "{" + output.substr(0, output.size() - 2) + "}: ";
  return absl_ports::StrCat(result, delegate_->ToString());
}

}  // namespace lib
}  // namespace icing
