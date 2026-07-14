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

#ifndef ICING_FEATURE_FLAGS_H_
#define ICING_FEATURE_FLAGS_H_

#include <cstdint>
#include <utility>

namespace icing {
namespace lib {

class FeatureFlags {
 public:
  explicit FeatureFlags(bool allow_circular_schema_definitions,
                        bool enable_repeated_field_joins,
                        bool enable_embedding_backup_generation,
                        bool enable_optimize_improvements,
                        int64_t expired_document_purge_threshold_ms,
                        bool enable_non_existent_qualified_id_join,
                        bool enable_skip_set_schema_type_equality_check,
                        bool enable_schema_definition_deduping,
                        bool enable_delete_propagation_from,
                        bool enable_account_property_incompatibility_check,
                        bool schema_store_release_cached_proto_after_use)
      : allow_circular_schema_definitions_(allow_circular_schema_definitions),
        enable_repeated_field_joins_(enable_repeated_field_joins),
        enable_embedding_backup_generation_(enable_embedding_backup_generation),
        enable_optimize_improvements_(enable_optimize_improvements),
        expired_document_purge_threshold_ms_(
            expired_document_purge_threshold_ms),
        enable_non_existent_qualified_id_join_(
            enable_non_existent_qualified_id_join),
        enable_skip_set_schema_type_equality_check_(
            enable_skip_set_schema_type_equality_check),
        enable_schema_definition_deduping_(enable_schema_definition_deduping),
        enable_delete_propagation_from_(enable_delete_propagation_from),
        enable_account_property_incompatibility_check_(
            enable_account_property_incompatibility_check),
        schema_store_release_cached_proto_after_use_(
            schema_store_release_cached_proto_after_use) {}

  bool allow_circular_schema_definitions() const {
    return allow_circular_schema_definitions_;
  }

  bool enable_repeated_field_joins() const {
    return enable_repeated_field_joins_;
  }

  bool enable_embedding_backup_generation() const {
    return enable_embedding_backup_generation_;
  }

  bool enable_optimize_improvements() const {
    return enable_optimize_improvements_;
  }

  int64_t expired_document_purge_threshold_ms() const {
    return expired_document_purge_threshold_ms_;
  }

  bool enable_non_existent_qualified_id_join() const {
    return enable_non_existent_qualified_id_join_;
  }

  bool enable_skip_set_schema_type_equality_check() const {
    return enable_skip_set_schema_type_equality_check_;
  }

  bool enable_schema_definition_deduping() const {
    return enable_schema_definition_deduping_;
  }

  bool enable_delete_propagation_from() const {
    return enable_delete_propagation_from_;
  }

  bool enable_account_property_incompatibility_check() const {
    return enable_account_property_incompatibility_check_;
  }

  bool schema_store_release_cached_proto_after_use() const {
    return schema_store_release_cached_proto_after_use_;
  }

 private:
  FeatureFlags() = default;

  friend class FeatureFlagsBuilder;

  // Whether to allow circular references in the schema definition. This was
  // added in the Android U timeline and is not a trunk-stable flag.
  bool allow_circular_schema_definitions_ = false;

  bool enable_repeated_field_joins_ = false;

  // Controls code that runs in backup schema producer to remove embedding
  // properties.
  bool enable_embedding_backup_generation_ = false;

  // Whether to enable a few minor improvements to Optimize:
  // 1. Avoid unnecessary Status allocs for deleted/expired docs
  // 2. Remove an unnecessary persist to disk call
  bool enable_optimize_improvements_ = false;

  // The time threshold for an expired document to be purged.
  // - Since we schedule a task to purge expired documents according to the next
  //   expiration time of the documents, it is possible that some documents
  //   expire within a small time window and the task executes too frequently.
  // - Therefore, we use this flag to purge more documents that also expire in a
  //   short period of time after the current time.
  //
  // For example, if the value is 1000 ms and the current time is 10000 ms:
  // - All documents that are expired before 10000 ms will be purged, since they
  //   are already expired.
  // - Additionally, we will also purge documents that expire in the next 1000
  //   ms, i.e. (10000, 11000] ms.
  int64_t expired_document_purge_threshold_ms_ = 0;

  // Whether to allow a document to reference a join parent (by its qualified
  // id) that does not yet exist. When enabled, the join index will handle cases
  // where a child document is indexed before its parent. The join relationship
  // will be established once the parent document is indexed. If disabled, the
  // join relationship will be lost if the child is indexed before the parent.
  bool enable_non_existent_qualified_id_join_ = false;

  // Whether to skip the schema type equality check during SetSchema. We
  // serialize the schema proto to strings when doing this check, which is slow.
  //
  // This will be set to true in AppSearch.
  // - AppSearch already checks if schema types are unchanged for a new
  //   SetSchema request, and skips the Icing interaction entirely if that is
  //   the case.
  // - Therefore, the Icing-side equality check in SetSchema is redundant and
  //   can be skipped if caller is AppSearch.
  bool enable_skip_set_schema_type_equality_check_ = false;

  // Whether to enable deduping for the schema's type definitions.
  bool enable_schema_definition_deduping_ = false;

  // Whether to enable delete propagation PROPAGATE_FROM.
  bool enable_delete_propagation_from_ = false;

  // Whether to enable the account property incompatibility check.
  bool enable_account_property_incompatibility_check_ = false;

  // Whether to release schema-store's cached proto instances after use.
  bool schema_store_release_cached_proto_after_use_ = false;
};

class FeatureFlagsBuilder {
 public:
  FeatureFlagsBuilder() = default;

  explicit FeatureFlagsBuilder(FeatureFlags feature_flags)
      : feature_flags_(std::move(feature_flags)) {}

  FeatureFlagsBuilder& set_allow_circular_schema_definitions(
      bool allow_circular_schema_definitions) {
    feature_flags_.allow_circular_schema_definitions_ =
        allow_circular_schema_definitions;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_repeated_field_joins(
      bool enable_repeated_field_joins) {
    feature_flags_.enable_repeated_field_joins_ = enable_repeated_field_joins;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_embedding_backup_generation(
      bool enable_embedding_backup_generation) {
    feature_flags_.enable_embedding_backup_generation_ =
        enable_embedding_backup_generation;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_optimize_improvements(
      bool enable_optimize_improvements) {
    feature_flags_.enable_optimize_improvements_ = enable_optimize_improvements;
    return *this;
  }

  FeatureFlagsBuilder& set_expired_document_purge_threshold_ms(
      int64_t expired_document_purge_threshold_ms) {
    feature_flags_.expired_document_purge_threshold_ms_ =
        expired_document_purge_threshold_ms;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_non_existent_qualified_id_join(
      bool enable_non_existent_qualified_id_join) {
    feature_flags_.enable_non_existent_qualified_id_join_ =
        enable_non_existent_qualified_id_join;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_skip_set_schema_type_equality_check(
      bool enable_skip_set_schema_type_equality_check) {
    feature_flags_.enable_skip_set_schema_type_equality_check_ =
        enable_skip_set_schema_type_equality_check;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_schema_definition_deduping(
      bool enable_schema_definition_deduping) {
    feature_flags_.enable_schema_definition_deduping_ =
        enable_schema_definition_deduping;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_delete_propagation_from(
      bool enable_delete_propagation_from) {
    feature_flags_.enable_delete_propagation_from_ =
        enable_delete_propagation_from;
    return *this;
  }

  FeatureFlagsBuilder& set_enable_account_property_incompatibility_check(
      bool enable_account_property_incompatibility_check) {
    feature_flags_.enable_account_property_incompatibility_check_ =
        enable_account_property_incompatibility_check;
    return *this;
  }

  FeatureFlagsBuilder& set_schema_store_release_cached_proto_after_use(
      bool schema_store_release_cached_proto_after_use) {
    feature_flags_.schema_store_release_cached_proto_after_use_ =
        schema_store_release_cached_proto_after_use;
    return *this;
  }

  FeatureFlags Build() { return feature_flags_; }

 private:
  FeatureFlags feature_flags_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FEATURE_FLAGS_H_
