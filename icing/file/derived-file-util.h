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

#ifndef ICING_FILE_DERIVED_FILE_UTIL_H_
#define ICING_FILE_DERIVED_FILE_UTIL_H_

namespace icing {
namespace lib {

namespace derived_file_util {

// Contains information about whether the derived files of each component need
// to be rebuild. Whether the derived files of a component need to be rebuilt
// (during initialization) depends on the following conditions:
// - Version change.
// - Feature flag change.
// - General marker file presence. It means that something wonky happened when
//   we last tried to do some complex operations (e.g. SetSchema, Optimize).
//
// These flags only reflect whether each component should be rebuilt, but do not
// handle any dependencies. The caller should handle the dependencies by
// themselves.
// e.g. - qualified id join index depends on document store derived files, but
//        it's possible to have needs_document_store_derived_files_rebuild =
//        true and needs_qualified_id_join_index_rebuild = false.
//      - The caller should know that join index should also be rebuilt in this
//        case even though needs_qualified_id_join_index_rebuild = false.
struct DerivedFilesRebuildInfo {
  bool needs_document_store_derived_files_rebuild = false;
  bool needs_schema_store_derived_files_rebuild = false;
  bool needs_term_index_rebuild = false;
  bool needs_integer_index_rebuild = false;
  bool needs_qualified_id_join_index_rebuild = false;
  bool needs_embedding_index_rebuild = false;

  DerivedFilesRebuildInfo() = default;

  explicit DerivedFilesRebuildInfo(
      bool needs_document_store_derived_files_rebuild_in,
      bool needs_schema_store_derived_files_rebuild_in,
      bool needs_term_index_rebuild_in, bool needs_integer_index_rebuild_in,
      bool needs_qualified_id_join_index_rebuild_in,
      bool needs_embedding_index_rebuild_in)
      : needs_document_store_derived_files_rebuild(
            needs_document_store_derived_files_rebuild_in),
        needs_schema_store_derived_files_rebuild(
            needs_schema_store_derived_files_rebuild_in),
        needs_term_index_rebuild(needs_term_index_rebuild_in),
        needs_integer_index_rebuild(needs_integer_index_rebuild_in),
        needs_qualified_id_join_index_rebuild(
            needs_qualified_id_join_index_rebuild_in),
        needs_embedding_index_rebuild(needs_embedding_index_rebuild_in) {}

  bool IsRebuildNeeded() const {
    return needs_document_store_derived_files_rebuild ||
           needs_schema_store_derived_files_rebuild ||
           needs_term_index_rebuild || needs_integer_index_rebuild ||
           needs_qualified_id_join_index_rebuild ||
           needs_embedding_index_rebuild;
  }

  void RebuildAll() {
    needs_document_store_derived_files_rebuild = true;
    needs_schema_store_derived_files_rebuild = true;
    needs_term_index_rebuild = true;
    needs_integer_index_rebuild = true;
    needs_qualified_id_join_index_rebuild = true;
    needs_embedding_index_rebuild = true;
  }

  bool operator==(const DerivedFilesRebuildInfo& other) const {
    return needs_document_store_derived_files_rebuild ==
               other.needs_document_store_derived_files_rebuild &&
           needs_schema_store_derived_files_rebuild ==
               other.needs_schema_store_derived_files_rebuild &&
           needs_term_index_rebuild == other.needs_term_index_rebuild &&
           needs_integer_index_rebuild == other.needs_integer_index_rebuild &&
           needs_qualified_id_join_index_rebuild ==
               other.needs_qualified_id_join_index_rebuild &&
           needs_embedding_index_rebuild == other.needs_embedding_index_rebuild;
  }

  DerivedFilesRebuildInfo& operator|=(const DerivedFilesRebuildInfo& other) {
    needs_document_store_derived_files_rebuild |=
        other.needs_document_store_derived_files_rebuild;
    needs_schema_store_derived_files_rebuild |=
        other.needs_schema_store_derived_files_rebuild;
    needs_term_index_rebuild |= other.needs_term_index_rebuild;
    needs_integer_index_rebuild |= other.needs_integer_index_rebuild;
    needs_qualified_id_join_index_rebuild |=
        other.needs_qualified_id_join_index_rebuild;
    needs_embedding_index_rebuild |= other.needs_embedding_index_rebuild;
    return *this;
  }
};

}  // namespace derived_file_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_DERIVED_FILE_UTIL_H_
