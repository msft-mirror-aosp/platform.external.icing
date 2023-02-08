// Copyright (C) 2022 Google LLC
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

#ifndef ICING_INDEX_NUMERIC_DUMMY_NUMERIC_INDEX_H_
#define ICING_INDEX_NUMERIC_DUMMY_NUMERIC_INDEX_H_

#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/file/persistent-storage.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/hit/hit.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/doc-hit-info-iterator-numeric.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/util/crc32.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// DummyNumericIndex: dummy class to help with testing and unblock e2e
// integration for numeric search. It stores all numeric index data (keys and
// hits) in memory without actual persistent storages. All PersistentStorage
// features do not work as expected, i.e. they don't persist any data into disk
// and therefore data are volatile.
template <typename T>
class DummyNumericIndex : public NumericIndex<T> {
 public:
  static libtextclassifier3::StatusOr<std::unique_ptr<DummyNumericIndex<T>>>
  Create(const Filesystem& filesystem, std::string working_path) {
    auto dummy_numeric_index = std::unique_ptr<DummyNumericIndex<T>>(
        new DummyNumericIndex<T>(filesystem, std::move(working_path)));
    ICING_RETURN_IF_ERROR(dummy_numeric_index->InitializeNewStorage());
    return dummy_numeric_index;
  }

  ~DummyNumericIndex() override = default;

  std::unique_ptr<typename NumericIndex<T>::Editor> Edit(
      std::string_view property_path, DocumentId document_id,
      SectionId section_id) override {
    return std::make_unique<Editor>(property_path, document_id, section_id,
                                    storage_);
  }

  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>> GetIterator(
      std::string_view property_path, T key_lower, T key_upper) const override;

  libtextclassifier3::Status Reset() override {
    storage_.clear();
    return libtextclassifier3::Status::OK;
  }

 private:
  class Editor : public NumericIndex<T>::Editor {
   public:
    explicit Editor(
        std::string_view property_path, DocumentId document_id,
        SectionId section_id,
        std::unordered_map<std::string, std::map<T, std::vector<BasicHit>>>&
            storage)
        : NumericIndex<T>::Editor(property_path, document_id, section_id),
          storage_(storage) {}

    ~Editor() override = default;

    libtextclassifier3::Status BufferKey(T key) override {
      seen_keys_.insert(key);
      return libtextclassifier3::Status::OK;
    }

    libtextclassifier3::Status IndexAllBufferedKeys() override;

   private:
    std::unordered_set<T> seen_keys_;
    std::unordered_map<std::string, std::map<T, std::vector<BasicHit>>>&
        storage_;
  };

  class Iterator : public NumericIndex<T>::Iterator {
   public:
    // We group BasicHits (sorted by document_id) of a key into a Bucket (stored
    // as std::vector) and store key -> vector in an std::map. When doing range
    // query, we may access vectors from multiple keys and want to return
    // BasicHits to callers sorted by document_id. Therefore, this problem is
    // actually "merge K sorted vectors".
    // To implement this algorithm via priority_queue, we create this wrapper
    // class to store iterators of map and vector.
    class BucketInfo {
     public:
      explicit BucketInfo(
          typename std::map<T, std::vector<BasicHit>>::const_iterator
              bucket_iter)
          : bucket_iter_(bucket_iter),
            vec_iter_(bucket_iter_->second.rbegin()) {}

      bool Advance() { return ++vec_iter_ != bucket_iter_->second.rend(); }

      const BasicHit& GetCurrentBasicHit() const { return *vec_iter_; }

      bool operator<(const BucketInfo& other) const {
        // std::priority_queue is a max heap and we should return BasicHits in
        // DocumentId descending order.
        // - BucketInfo::operator< should have the same order as DocumentId.
        // - BasicHit encodes inverted document id and its operator< compares
        //   the encoded raw value directly.
        // - Therefore, BucketInfo::operator< should compare BasicHit reversely.
        // - This will make priority_queue return buckets in DocumentId
        //   descending and SectionId ascending order.
        // - Whatever direction we sort SectionId by (or pop by priority_queue)
        //   doesn't matter because all hits for the same DocumentId will be
        //   merged into a single DocHitInfo.
        return other.GetCurrentBasicHit() < GetCurrentBasicHit();
      }

     private:
      typename std::map<T, std::vector<BasicHit>>::const_iterator bucket_iter_;
      std::vector<BasicHit>::const_reverse_iterator vec_iter_;
    };

    explicit Iterator(T key_lower, T key_upper,
                      std::vector<BucketInfo>&& bucket_info_vec)
        : NumericIndex<T>::Iterator(key_lower, key_upper),
          pq_(std::less<BucketInfo>(), std::move(bucket_info_vec)) {}

    ~Iterator() override = default;

    libtextclassifier3::Status Advance() override;

    DocHitInfo GetDocHitInfo() const override { return doc_hit_info_; }

   private:
    std::priority_queue<BucketInfo> pq_;
    DocHitInfo doc_hit_info_;
  };

 private:
  explicit DummyNumericIndex(const Filesystem& filesystem,
                             std::string&& working_path)
      : NumericIndex<T>(filesystem, std::move(working_path),
                        PersistentStorage::WorkingPathType::kDummy) {}

  libtextclassifier3::Status PersistStoragesToDisk() override {
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::Status PersistMetadataToDisk() override {
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::StatusOr<Crc32> ComputeInfoChecksum() override {
    return Crc32(0);
  }

  libtextclassifier3::StatusOr<Crc32> ComputeStoragesChecksum() override {
    return Crc32(0);
  }

  PersistentStorage::Crcs& crcs() override { return dummy_crcs_; }
  const PersistentStorage::Crcs& crcs() const override { return dummy_crcs_; }

  std::unordered_map<std::string, std::map<T, std::vector<BasicHit>>> storage_;
  PersistentStorage::Crcs dummy_crcs_;
};

template <typename T>
libtextclassifier3::Status
DummyNumericIndex<T>::Editor::IndexAllBufferedKeys() {
  auto property_map_iter = storage_.find(this->property_path_);
  if (property_map_iter == storage_.end()) {
    const auto& [inserted_iter, insert_result] =
        storage_.insert({this->property_path_, {}});
    if (!insert_result) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Failed to create a new map for property \"",
                             this->property_path_, "\""));
    }
    property_map_iter = inserted_iter;
  }

  for (const T& key : seen_keys_) {
    auto key_map_iter = property_map_iter->second.find(key);
    if (key_map_iter == property_map_iter->second.end()) {
      const auto& [inserted_iter, insert_result] =
          property_map_iter->second.insert({key, {}});
      if (!insert_result) {
        return absl_ports::InternalError("Failed to create a new map for key");
      }
      key_map_iter = inserted_iter;
    }
    key_map_iter->second.push_back(
        BasicHit(this->section_id_, this->document_id_));
  }
  return libtextclassifier3::Status::OK;
}

template <typename T>
libtextclassifier3::Status DummyNumericIndex<T>::Iterator::Advance() {
  if (pq_.empty()) {
    return absl_ports::OutOfRangeError("End of iterator");
  }

  DocumentId document_id = pq_.top().GetCurrentBasicHit().document_id();
  doc_hit_info_ = DocHitInfo(document_id);
  // Merge sections with same document_id into a single DocHitInfo
  while (!pq_.empty() &&
         pq_.top().GetCurrentBasicHit().document_id() == document_id) {
    doc_hit_info_.UpdateSection(pq_.top().GetCurrentBasicHit().section_id());

    BucketInfo info = pq_.top();
    pq_.pop();

    if (info.Advance()) {
      pq_.push(std::move(info));
    }
  }

  return libtextclassifier3::Status::OK;
}

template <typename T>
libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
DummyNumericIndex<T>::GetIterator(std::string_view property_path, T key_lower,
                                  T key_upper) const {
  if (key_lower > key_upper) {
    return absl_ports::InvalidArgumentError(
        "key_lower should not be greater than key_upper");
  }

  auto property_map_iter = storage_.find(std::string(property_path));
  if (property_map_iter == storage_.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Property \"", property_path, "\" not found"));
  }

  std::vector<typename Iterator::BucketInfo> bucket_info_vec;
  for (auto key_map_iter = property_map_iter->second.lower_bound(key_lower);
       key_map_iter != property_map_iter->second.cend() &&
       key_map_iter->first <= key_upper;
       ++key_map_iter) {
    bucket_info_vec.push_back(typename Iterator::BucketInfo(key_map_iter));
  }

  return std::make_unique<DocHitInfoIteratorNumeric<T>>(
      std::make_unique<Iterator>(key_lower, key_upper,
                                 std::move(bucket_info_vec)));
}

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_NUMERIC_DUMMY_NUMERIC_INDEX_H_
