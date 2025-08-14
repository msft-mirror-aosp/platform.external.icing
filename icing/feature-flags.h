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

#ifndef THIRD_PARTY_ICING_FEATURE_FLAGS_H_
#define THIRD_PARTY_ICING_FEATURE_FLAGS_H_

namespace icing {
namespace lib {

class FeatureFlags {
 public:
  explicit FeatureFlags(bool allow_circular_schema_definitions,
                        bool enable_scorable_properties,
                        bool enable_embedding_quantization,
                        bool enable_repeated_field_joins,
                        bool enable_embedding_backup_generation,
                        bool enable_schema_database,
                        bool release_backup_schema_file_if_overlay_present,
                        bool enable_strict_page_byte_size_limit,
                        bool enable_smaller_decompression_buffer_size,
                        bool enable_eigen_embedding_scoring,
                        bool enable_passing_filter_to_children,
                        bool enable_proto_log_new_header_format,
                        bool enable_embedding_iterator_v2,
                        bool enable_reusable_decompression_buffer)
      : allow_circular_schema_definitions_(allow_circular_schema_definitions),
        enable_scorable_properties_(enable_scorable_properties),
        enable_embedding_quantization_(enable_embedding_quantization),
        enable_repeated_field_joins_(enable_repeated_field_joins),
        enable_embedding_backup_generation_(enable_embedding_backup_generation),
        enable_schema_database_(enable_schema_database),
        release_backup_schema_file_if_overlay_present_(
            release_backup_schema_file_if_overlay_present),
        enable_strict_page_byte_size_limit_(enable_strict_page_byte_size_limit),
        enable_smaller_decompression_buffer_size_(
            enable_smaller_decompression_buffer_size),
        enable_eigen_embedding_scoring_(enable_eigen_embedding_scoring),
        enable_passing_filter_to_children_(enable_passing_filter_to_children),
        enable_proto_log_new_header_format_(enable_proto_log_new_header_format),
        enable_embedding_iterator_v2_(enable_embedding_iterator_v2),
        enable_reusable_decompression_buffer_(
            enable_reusable_decompression_buffer) {}

  bool allow_circular_schema_definitions() const {
    return allow_circular_schema_definitions_;
  }

  bool enable_scorable_properties() const {
    return enable_scorable_properties_;
  }

  bool enable_embedding_quantization() const {
    return enable_embedding_quantization_;
  }

  bool enable_repeated_field_joins() const {
    return enable_repeated_field_joins_;
  }

  bool enable_embedding_backup_generation() const {
    return enable_embedding_backup_generation_;
  }

  bool enable_schema_database() const { return enable_schema_database_; }

  bool release_backup_schema_file_if_overlay_present() const {
    return release_backup_schema_file_if_overlay_present_;
  }

  bool enable_strict_page_byte_size_limit() const {
    return enable_strict_page_byte_size_limit_;
  }

  bool enable_smaller_decompression_buffer_size() const {
    return enable_smaller_decompression_buffer_size_;
  }

  bool enable_eigen_embedding_scoring() const {
    return enable_eigen_embedding_scoring_;
  }

  bool enable_passing_filter_to_children() const {
    return enable_passing_filter_to_children_;
  }

  bool enable_proto_log_new_header_format() const {
    return enable_proto_log_new_header_format_;
  }

  bool enable_embedding_iterator_v2() const {
    return enable_embedding_iterator_v2_;
  }

  bool enable_reusable_decompression_buffer() const {
    return enable_reusable_decompression_buffer_;
  }

 private:
  // Whether to allow circular references in the schema definition. This was
  // added in the Android U timeline and is not a trunk-stable flag.
  bool allow_circular_schema_definitions_;

  bool enable_scorable_properties_;

  // Whether to enable quantization for embedding vectors. If false, all
  // embedding vectors will not be quantized. Otherwise, quantization will be
  // controlled by the quantization type specified in the schema.
  bool enable_embedding_quantization_;

  bool enable_repeated_field_joins_;

  // Controls code that runs in backup schema producer to remove embedding
  // properties.
  bool enable_embedding_backup_generation_;

  bool enable_schema_database_;

  bool release_backup_schema_file_if_overlay_present_;

  // Whether to enable strict page byte size limit enforcement in
  // ResultRetrieverV2.
  bool enable_strict_page_byte_size_limit_;

  bool enable_smaller_decompression_buffer_size_;

  // Whether to enable the Eigen library for embedding scoring.
  // If set to true **and** Eigen is compiled in (when ICING_DISABLE_EIGEN is
  // not defined), Eigen will be used for embedding scoring.
  bool enable_eigen_embedding_scoring_;

  bool enable_passing_filter_to_children_;

  // Whether to enable the new header format (refactor legacy format and
  // introduce unsynced tail checksum) related changes in
  // PortableFileBackedProtoLog.
  bool enable_proto_log_new_header_format_;

  bool enable_embedding_iterator_v2_;

  // Whether PortableFileBackedProtoLog should retain a decompression buffer
  // that reads can reuse rather than allocating a new one for each read.
  bool enable_reusable_decompression_buffer_;
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_FEATURE_FLAGS_H_
