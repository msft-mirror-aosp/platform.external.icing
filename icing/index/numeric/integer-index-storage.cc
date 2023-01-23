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

#include "icing/index/numeric/integer-index-storage.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/doc-hit-info-iterator-numeric.h"
#include "icing/index/numeric/integer-index-data.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/numeric/posting-list-integer-index-accessor.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Helper function to PWrite crcs and info to metadata_file_path.
libtextclassifier3::Status WriteMetadata(
    const Filesystem& filesystem, const std::string& metadata_file_path,
    const IntegerIndexStorage::Crcs* crcs,
    const IntegerIndexStorage::Info* info) {
  ScopedFd sfd(filesystem.OpenForWrite(metadata_file_path.c_str()));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError("Failed to create metadata file");
  }

  // Write crcs and info. File layout: <Crcs><Info>
  ICING_RETURN_IF_ERROR(crcs->Serialize(filesystem, sfd.get()));
  ICING_RETURN_IF_ERROR(info->Serialize(filesystem, sfd.get()));

  return libtextclassifier3::Status::OK;
}

// Helper function to update checksums from info and storages to a Crcs
// instance.
libtextclassifier3::Status UpdateChecksums(
    IntegerIndexStorage::Crcs* crcs, IntegerIndexStorage::Info* info,
    FileBackedVector<IntegerIndexStorage::Bucket>* sorted_buckets,
    FileBackedVector<IntegerIndexStorage::Bucket>* unsorted_buckets,
    FlashIndexStorage* flash_index_storage) {
  // Compute crcs
  ICING_ASSIGN_OR_RETURN(Crc32 sorted_buckets_crc,
                         sorted_buckets->ComputeChecksum());
  ICING_ASSIGN_OR_RETURN(Crc32 unsorted_buckets_crc,
                         unsorted_buckets->ComputeChecksum());

  crcs->component_crcs.info_crc = info->ComputeChecksum().Get();
  crcs->component_crcs.sorted_buckets_crc = sorted_buckets_crc.Get();
  crcs->component_crcs.unsorted_buckets_crc = unsorted_buckets_crc.Get();
  // TODO(b/259744228): implement and update flash_index_storage checksum
  crcs->component_crcs.flash_index_storage_crc = 0;
  crcs->all_crc = crcs->component_crcs.ComputeChecksum().Get();

  return libtextclassifier3::Status::OK;
}

// Helper function to validate checksums.
libtextclassifier3::Status ValidateChecksums(
    const IntegerIndexStorage::Crcs* crcs,
    const IntegerIndexStorage::Info* info,
    FileBackedVector<IntegerIndexStorage::Bucket>* sorted_buckets,
    FileBackedVector<IntegerIndexStorage::Bucket>* unsorted_buckets,
    FlashIndexStorage* flash_index_storage) {
  if (crcs->all_crc != crcs->component_crcs.ComputeChecksum().Get()) {
    return absl_ports::FailedPreconditionError(
        "Invalid all crc for IntegerIndexStorage");
  }

  if (crcs->component_crcs.info_crc != info->ComputeChecksum().Get()) {
    return absl_ports::FailedPreconditionError(
        "Invalid info crc for IntegerIndexStorage");
  }

  ICING_ASSIGN_OR_RETURN(Crc32 sorted_buckets_crc,
                         sorted_buckets->ComputeChecksum());
  if (crcs->component_crcs.sorted_buckets_crc != sorted_buckets_crc.Get()) {
    return absl_ports::FailedPreconditionError(
        "Mismatch crc with IntegerIndexStorage sorted buckets");
  }

  ICING_ASSIGN_OR_RETURN(Crc32 unsorted_buckets_crc,
                         unsorted_buckets->ComputeChecksum());
  if (crcs->component_crcs.unsorted_buckets_crc != unsorted_buckets_crc.Get()) {
    return absl_ports::FailedPreconditionError(
        "Mismatch crc with IntegerIndexStorage unsorted buckets");
  }

  // TODO(b/259744228): implement and verify flash_index_storage checksum

  return libtextclassifier3::Status::OK;
}

// The following 4 methods are helper functions to get the correct file path of
// metadata/sorted_buckets/unsorted_buckets/flash_index_storage, according to
// the given working directory.
std::string GetMetadataFilePath(std::string_view working_dir) {
  return absl_ports::StrCat(working_dir, "/", IntegerIndexStorage::kFilePrefix,
                            ".m");
}

std::string GetSortedBucketsFilePath(std::string_view working_dir) {
  return absl_ports::StrCat(working_dir, "/", IntegerIndexStorage::kFilePrefix,
                            ".s");
}

std::string GetUnsortedBucketsFilePath(std::string_view working_dir) {
  return absl_ports::StrCat(working_dir, "/", IntegerIndexStorage::kFilePrefix,
                            ".u");
}

std::string GetFlashIndexStorageFilePath(std::string_view working_dir) {
  return absl_ports::StrCat(working_dir, "/", IntegerIndexStorage::kFilePrefix,
                            ".f");
}

}  // namespace

// We add (BasicHits, key) into a bucket in DocumentId descending and SectionId
// ascending order. When doing range query, we may access buckets and want to
// return BasicHits to callers sorted by DocumentId. Therefore, this problem is
// actually "merge K sorted lists".
// To implement this algorithm via priority_queue, we create this wrapper class
// to store PostingListIntegerIndexAccessor for iterating through the posting
// list chain.
// - Non-relevant (i.e. not in range [key_lower, key_upper]) will be skipped.
// - Relevant BasicHits will be returned.
class BucketPostingListIterator {
 public:
  class Comparator {
   public:
    // REQUIRES: 2 BucketPostingListIterator* instances (lhs, rhs) should be
    //   valid, i.e. the preceding AdvanceAndFilter() succeeded.
    bool operator()(const BucketPostingListIterator* lhs,
                    const BucketPostingListIterator* rhs) const {
      // std::priority_queue is a max heap and we should return BasicHits in
      // DocumentId descending order.
      // - BucketPostingListIterator::operator< should have the same order as
      //   DocumentId.
      // - BasicHit encodes inverted document id and BasicHit::operator<
      //   compares the encoded raw value directly.
      // - Therefore, BucketPostingListIterator::operator< should compare
      //   BasicHit reversely.
      // - This will make priority_queue return buckets in DocumentId
      //   descending and SectionId ascending order.
      // - Whatever direction we sort SectionId by (or pop by priority_queue)
      //   doesn't matter because all hits for the same DocumentId will be
      //   merged into a single DocHitInfo.
      return rhs->GetCurrentBasicHit() < lhs->GetCurrentBasicHit();
    }
  };

  explicit BucketPostingListIterator(
      std::unique_ptr<PostingListIntegerIndexAccessor> pl_accessor)
      : pl_accessor_(std::move(pl_accessor)),
        should_retrieve_next_batch_(true) {}

  // Advances to the next relevant data. The posting list of a bucket contains
  // keys within range [bucket.key_lower, bucket.key_upper], but some of them
  // may be out of [query_key_lower, query_key_upper], so when advancing we have
  // to filter out those non-relevant keys.
  //
  // Returns:
  //   - OK on success
  //   - OUT_OF_RANGE_ERROR if reaching the end (i.e. no more relevant data)
  //   - Any other PostingListIntegerIndexAccessor errors
  libtextclassifier3::Status AdvanceAndFilter(int64_t query_key_lower,
                                              int64_t query_key_upper) {
    // Move curr_ until reaching a relevant data (i.e. key in range
    // [query_key_lower, query_key_upper])
    do {
      if (!should_retrieve_next_batch_) {
        ++curr_;
        should_retrieve_next_batch_ =
            curr_ >= cached_batch_integer_index_data_.cend();
      }
      if (should_retrieve_next_batch_) {
        ICING_RETURN_IF_ERROR(GetNextDataBatch());
        should_retrieve_next_batch_ = false;
      }
    } while (curr_->key() < query_key_lower || curr_->key() > query_key_upper);

    return libtextclassifier3::Status::OK;
  }

  const BasicHit& GetCurrentBasicHit() const { return curr_->basic_hit(); }

 private:
  // Gets next batch of data from the posting list chain, caches in
  // cached_batch_integer_index_data_, and sets curr_ to the begin of the cache.
  libtextclassifier3::Status GetNextDataBatch() {
    auto cached_batch_integer_index_data_or = pl_accessor_->GetNextDataBatch();
    if (!cached_batch_integer_index_data_or.ok()) {
      ICING_LOG(WARNING)
          << "Fail to get next batch data from posting list due to: "
          << cached_batch_integer_index_data_or.status().error_message();
      return std::move(cached_batch_integer_index_data_or).status();
    }

    cached_batch_integer_index_data_ =
        std::move(cached_batch_integer_index_data_or).ValueOrDie();
    curr_ = cached_batch_integer_index_data_.cbegin();

    if (cached_batch_integer_index_data_.empty()) {
      return absl_ports::OutOfRangeError("End of iterator");
    }

    return libtextclassifier3::Status::OK;
  }

  std::unique_ptr<PostingListIntegerIndexAccessor> pl_accessor_;
  std::vector<IntegerIndexData> cached_batch_integer_index_data_;
  std::vector<IntegerIndexData>::const_iterator curr_;
  bool should_retrieve_next_batch_;
};

// Wrapper class to iterate through IntegerIndexStorage to get relevant data.
// It uses multiple BucketPostingListIterator instances from different candidate
// buckets and merges all relevant BasicHits from these buckets by
// std::priority_queue in DocumentId descending order. Also different SectionIds
// of the same DocumentId will be merged into SectionIdMask and returned as a
// single DocHitInfo.
class IntegerIndexStorageIterator : public NumericIndex<int64_t>::Iterator {
 public:
  explicit IntegerIndexStorageIterator(
      int64_t query_key_lower, int64_t query_key_upper,
      std::vector<std::unique_ptr<BucketPostingListIterator>>&& bucket_pl_iters)
      : NumericIndex<int64_t>::Iterator(query_key_lower, query_key_upper) {
    std::vector<BucketPostingListIterator*> bucket_pl_iters_raw_ptrs;
    for (std::unique_ptr<BucketPostingListIterator>& bucket_pl_itr :
         bucket_pl_iters) {
      // Before adding BucketPostingListIterator* into the priority queue, we
      // have to advance the bucket iterator to the first valid data since the
      // priority queue needs valid data to compare the order.
      // Note: it is possible that the bucket iterator fails to advance for the
      // first round, because data could be filtered out by [query_key_lower,
      // query_key_upper]. In this case, just discard the iterator.
      if (bucket_pl_itr->AdvanceAndFilter(query_key_lower, query_key_upper)
              .ok()) {
        bucket_pl_iters_raw_ptrs.push_back(bucket_pl_itr.get());
        bucket_pl_iters_.push_back(std::move(bucket_pl_itr));
      }
    }

    pq_ = std::priority_queue<BucketPostingListIterator*,
                              std::vector<BucketPostingListIterator*>,
                              BucketPostingListIterator::Comparator>(
        comparator_, std::move(bucket_pl_iters_raw_ptrs));
  }

  ~IntegerIndexStorageIterator() override = default;

  // Advances to the next DocHitInfo. Note: several BucketPostingListIterator
  // instances may be advanced if they point to data with the same DocumentId.
  //
  // Returns:
  //   - OK on success
  //   - OUT_OF_RANGE_ERROR if reaching the end (i.e. no more relevant data)
  //   - Any BucketPostingListIterator errors
  libtextclassifier3::Status Advance() override;

  DocHitInfo GetDocHitInfo() const override { return doc_hit_info_; }

 private:
  BucketPostingListIterator::Comparator comparator_;

  // We have to fetch and pop the top BucketPostingListIterator from
  // std::priority_queue to perform "merge K sorted lists algorithm".
  // - Since std::priority_queue::pop() doesn't return the top element, we have
  //   to call top() and pop() together.
  // - std::move the top() element by const_cast is not an appropriate way
  //   because it introduces transient unstable state for std::priority_queue.
  // - We don't want to copy BucketPostingListIterator, either.
  // - Therefore, add bucket_pl_iters_ for the ownership of all
  //   BucketPostingListIterator instances and std::priority_queue uses the raw
  //   pointer. So when calling top(), we can simply copy the raw pointer via
  //   top() and avoid transient unstable state.
  std::vector<std::unique_ptr<BucketPostingListIterator>> bucket_pl_iters_;
  std::priority_queue<BucketPostingListIterator*,
                      std::vector<BucketPostingListIterator*>,
                      BucketPostingListIterator::Comparator>
      pq_;

  DocHitInfo doc_hit_info_;
};

libtextclassifier3::Status IntegerIndexStorageIterator::Advance() {
  if (pq_.empty()) {
    return absl_ports::OutOfRangeError("End of iterator");
  }

  DocumentId document_id = pq_.top()->GetCurrentBasicHit().document_id();
  doc_hit_info_ = DocHitInfo(document_id);
  // Merge sections with same document_id into a single DocHitInfo
  while (!pq_.empty() &&
         pq_.top()->GetCurrentBasicHit().document_id() == document_id) {
    doc_hit_info_.UpdateSection(pq_.top()->GetCurrentBasicHit().section_id());

    BucketPostingListIterator* bucket_itr = pq_.top();
    pq_.pop();

    if (bucket_itr->AdvanceAndFilter(key_lower_, key_upper_).ok()) {
      pq_.push(bucket_itr);
    }
  }

  return libtextclassifier3::Status::OK;
}

bool IntegerIndexStorage::Options::IsValid() const {
  if (!HasCustomInitBuckets()) {
    return true;
  }

  // Verify if the range of buckets are disjoint and the range union is
  // [INT64_MIN, INT64_MAX].
  std::vector<Bucket> buckets;
  buckets.reserve(custom_init_sorted_buckets.size() +
                  custom_init_unsorted_buckets.size());
  buckets.insert(buckets.end(), custom_init_sorted_buckets.begin(),
                 custom_init_sorted_buckets.end());
  buckets.insert(buckets.end(), custom_init_unsorted_buckets.begin(),
                 custom_init_unsorted_buckets.end());
  if (buckets.empty()) {
    return false;
  }
  std::sort(buckets.begin(), buckets.end());
  int64_t expected_lower = std::numeric_limits<int64_t>::min();
  for (int i = 0; i < buckets.size(); ++i) {
    // key_lower should not be greater than key_upper and init bucket should
    // have invalid posting list identifier.
    if (buckets[i].key_lower() > buckets[i].key_upper() ||
        buckets[i].posting_list_identifier().is_valid()) {
      return false;
    }

    if (buckets[i].key_lower() != expected_lower) {
      return false;
    }

    // If it is the last bucket, then key_upper should be INT64_MAX. Otherwise
    // it should not be INT64_MAX. Use XOR for this logic.
    if ((buckets[i].key_upper() == std::numeric_limits<int64_t>::max()) ^
        (i == buckets.size() - 1)) {
      return false;
    }
    expected_lower = buckets[i].key_upper() + 1;
  }

  return true;
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
IntegerIndexStorage::Create(
    const Filesystem& filesystem, std::string_view base_dir, Options options,
    PostingListIntegerIndexSerializer* posting_list_serializer) {
  if (!options.IsValid()) {
    return absl_ports::InvalidArgumentError(
        "Invalid IntegerIndexStorage options");
  }

  std::string working_dir = absl_ports::StrCat(base_dir, "/", kSubDirectory);
  if (!filesystem.FileExists(GetMetadataFilePath(working_dir).c_str()) ||
      !filesystem.FileExists(GetSortedBucketsFilePath(working_dir).c_str()) ||
      !filesystem.FileExists(GetUnsortedBucketsFilePath(working_dir).c_str()) ||
      !filesystem.FileExists(
          GetFlashIndexStorageFilePath(working_dir).c_str())) {
    // Delete working_dir if any of them is missing, and reinitialize.
    if (!filesystem.DeleteDirectoryRecursively(working_dir.c_str())) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Failed to delete directory: ", working_dir));
    }
    return InitializeNewFiles(filesystem, std::move(working_dir),
                              std::move(options), posting_list_serializer);
  }
  return InitializeExistingFiles(filesystem, std::move(working_dir),
                                 std::move(options), posting_list_serializer);
}

IntegerIndexStorage::~IntegerIndexStorage() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING)
        << "Failed to persist hash map to disk while destructing "
        << working_dir_;
  }
}

class IntegerIndexStorageComparator {
 public:
  bool operator()(const IntegerIndexStorage::Bucket& lhs, int64_t rhs) const {
    return lhs.key_upper() < rhs;
  }
} kComparator;

libtextclassifier3::Status IntegerIndexStorage::AddKeys(
    DocumentId document_id, SectionId section_id,
    std::vector<int64_t>&& new_keys) {
  if (new_keys.empty()) {
    return libtextclassifier3::Status::OK;
  }

  std::sort(new_keys.begin(), new_keys.end());

  // Dedupe
  auto last = std::unique(new_keys.begin(), new_keys.end());
  new_keys.erase(last, new_keys.end());

  // When adding keys into a bucket, we potentially split it into 2 new buckets
  // and one of them will be added into the unsorted bucket array.
  // When handling keys belonging to buckets in the unsorted bucket array, we
  // don't have to (and must not) handle these newly split buckets. Therefore,
  // collect all newly split buckets in another vector and append them into the
  // unsorted bucket array after adding all keys.
  std::vector<Bucket> new_buckets;

  // Binary search range of the sorted bucket array.
  const Bucket* sorted_bucket_arr_begin = sorted_buckets_->array();
  const Bucket* sorted_bucket_arr_end =
      sorted_buckets_->array() + sorted_buckets_->num_elements();

  // Step 1: handle keys belonging to buckets in the sorted bucket array. Skip
  //         keys belonging to the unsorted bucket array and deal with them in
  //         the next step.
  // - Iterate through new_keys by it_start.
  // - Binary search (std::lower_bound comparing key with bucket.key_upper()) to
  //   find the first bucket in the sorted bucket array with key_upper is not
  //   smaller than (>=) the key.
  // - Skip (and advance it_start) all keys smaller than the target bucket's
  //   key_lower. It means these keys belong to buckets in the unsorted bucket
  //   array and we will deal with them later.
  // - Find it_end such that all keys within range [it_start, it_end) belong to
  //   the target bucket.
  // - Batch add keys within range [it_start, it_end) into the target bucket.
  auto it_start = new_keys.cbegin();
  while (it_start != new_keys.cend() &&
         sorted_bucket_arr_begin < sorted_bucket_arr_end) {
    // Use std::lower_bound to find the first bucket in the sorted bucket array
    // with key_upper >= *it_start.
    const Bucket* target_bucket = std::lower_bound(
        sorted_bucket_arr_begin, sorted_bucket_arr_end, *it_start, kComparator);
    if (target_bucket >= sorted_bucket_arr_end) {
      // Keys in range [it_start, new_keys.cend()) are greater than all sorted
      // buckets' key_upper, so we can end step 1. In fact, they belong to
      // buckets in the unsorted bucket array and we will deal with them in
      // step 2.
      break;
    }

    // Sequential instead of binary search to advance it_start and it_end for
    // several reasons:
    // - Eventually we have to iterate through all keys within range [it_start,
    //   it_end) and add them into the posting list, so binary search doesn't
    //   improve the overall time complexity.
    // - Binary search may jump to far-away indices, which potentially
    //   downgrades the cache performance.

    // After binary search, we've ensured *it_start <=
    // target_bucket->key_upper(), but it is still possible that *it_start (and
    // the next several keys) is still smaller than target_bucket->key_lower(),
    // so we have to skip them. In fact, they belong to buckets in the unsorted
    // bucket array.
    //
    // For example:
    // - sorted bucket array: [(INT_MIN, 0), (1, 5), (100, 300), (301, 550)]
    // - unsorted bucket array: [(550, INT_MAX), (6, 99)]
    // - new_keys: [10, 20, 40, 102, 150, 200, 500, 600]
    // std::lower_bound (target = 10) will get target_bucket = (100, 300), but
    // we have to skip 10, 20, 40 because they are smaller than 100 (the
    // bucket's key_lower). We should move it_start pointing to key 102.
    while (it_start != new_keys.cend() &&
           *it_start < target_bucket->key_lower()) {
      ++it_start;
    }

    // Locate it_end such that all keys within range [it_start, it_end) belong
    // to target_bucket and all keys outside this range don't belong to
    // target_bucket.
    //
    // For example (continue above), we should locate it_end to point to key
    // 500.
    auto it_end = it_start;
    while (it_end != new_keys.cend() && *it_end <= target_bucket->key_upper()) {
      ++it_end;
    }

    // Now, keys within range [it_start, it_end) belong to target_bucket, so
    // construct IntegerIndexData and add them into the bucket's posting list.
    if (it_start != it_end) {
      ICING_ASSIGN_OR_RETURN(
          FileBackedVector<Bucket>::MutableView mutable_bucket,
          sorted_buckets_->GetMutable(target_bucket -
                                      sorted_buckets_->array()));
      ICING_ASSIGN_OR_RETURN(
          std::vector<Bucket> round_new_buckets,
          AddKeysIntoBucketAndSplitIfNecessary(
              document_id, section_id, it_start, it_end, mutable_bucket));
      new_buckets.insert(new_buckets.end(), round_new_buckets.begin(),
                         round_new_buckets.end());
    }

    it_start = it_end;
    sorted_bucket_arr_begin = target_bucket + 1;
  }

  // Step 2: handle keys belonging to buckets in the unsorted bucket array. They
  //         were skipped in step 1.
  // For each bucket in the unsorted bucket array, find [it_start, it_end) such
  // that all keys within this range belong to the bucket and add them.
  // - Binary search (std::lower_bound comparing bucket.key_lower() with key) to
  //   find it_start.
  // - Sequential advance (start from it_start) to find it_end. Same reason as
  //   above for choosing sequential advance instead of binary search.
  // - Add keys within range [it_start, it_end) into the bucket.
  for (int32_t i = 0; i < unsorted_buckets_->num_elements(); ++i) {
    ICING_ASSIGN_OR_RETURN(FileBackedVector<Bucket>::MutableView mutable_bucket,
                           unsorted_buckets_->GetMutable(i));
    auto it_start = std::lower_bound(new_keys.cbegin(), new_keys.cend(),
                                     mutable_bucket.Get().key_lower());
    if (it_start == new_keys.cend()) {
      continue;
    }

    // Sequential advance instead of binary search to find the correct position
    // of it_end for the same reasons mentioned above in step 1.
    auto it_end = it_start;
    while (it_end != new_keys.cend() &&
           *it_end <= mutable_bucket.Get().key_upper()) {
      ++it_end;
    }

    // Now, key within range [it_start, it_end) belong to the bucket, so
    // construct IntegerIndexData and add them into the bucket's posting list.
    if (it_start != it_end) {
      ICING_ASSIGN_OR_RETURN(
          std::vector<Bucket> round_new_buckets,
          AddKeysIntoBucketAndSplitIfNecessary(
              document_id, section_id, it_start, it_end, mutable_bucket));
      new_buckets.insert(new_buckets.end(), round_new_buckets.begin(),
                         round_new_buckets.end());
    }
  }

  // Step 3: append new buckets into the unsorted bucket array.
  if (!new_buckets.empty()) {
    ICING_ASSIGN_OR_RETURN(
        typename FileBackedVector<Bucket>::MutableArrayView mutable_new_arr,
        unsorted_buckets_->Allocate(new_buckets.size()));
    mutable_new_arr.SetArray(/*idx=*/0, new_buckets.data(), new_buckets.size());
  }

  // Step 4: merge the unsorted bucket array into the sorted bucket array if the
  //         length of the unsorted bucket array exceeds the threshold.
  // TODO(b/259743562): [Optimization 1] implement merge

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
IntegerIndexStorage::GetIterator(int64_t query_key_lower,
                                 int64_t query_key_upper) const {
  if (query_key_lower > query_key_upper) {
    return absl_ports::InvalidArgumentError(
        "key_lower should not be greater than key_upper");
  }

  std::vector<std::unique_ptr<BucketPostingListIterator>> bucket_pl_iters;

  // Sorted bucket array
  const Bucket* sorted_bucket_arr_begin = sorted_buckets_->array();
  const Bucket* sorted_bucket_arr_end =
      sorted_buckets_->array() + sorted_buckets_->num_elements();
  for (const Bucket* bucket =
           std::lower_bound(sorted_bucket_arr_begin, sorted_bucket_arr_end,
                            query_key_lower, kComparator);
       bucket < sorted_bucket_arr_end && bucket->key_lower() <= query_key_upper;
       ++bucket) {
    if (!bucket->posting_list_identifier().is_valid()) {
      continue;
    }

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListIntegerIndexAccessor> pl_accessor,
        PostingListIntegerIndexAccessor::CreateFromExisting(
            flash_index_storage_.get(), posting_list_serializer_,
            bucket->posting_list_identifier()));
    bucket_pl_iters.push_back(
        std::make_unique<BucketPostingListIterator>(std::move(pl_accessor)));
  }

  // Unsorted bucket array
  for (int32_t i = 0; i < unsorted_buckets_->num_elements(); ++i) {
    ICING_ASSIGN_OR_RETURN(const Bucket* bucket, unsorted_buckets_->Get(i));
    if (query_key_upper < bucket->key_lower() ||
        query_key_lower > bucket->key_upper() ||
        !bucket->posting_list_identifier().is_valid()) {
      // Skip bucket whose range doesn't overlap with [query_key_lower,
      // query_key_upper] or posting_list_identifier is invalid.
      continue;
    }

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListIntegerIndexAccessor> pl_accessor,
        PostingListIntegerIndexAccessor::CreateFromExisting(
            flash_index_storage_.get(), posting_list_serializer_,
            bucket->posting_list_identifier()));
    bucket_pl_iters.push_back(
        std::make_unique<BucketPostingListIterator>(std::move(pl_accessor)));
  }

  return std::make_unique<DocHitInfoIteratorNumeric<int64_t>>(
      std::make_unique<IntegerIndexStorageIterator>(
          query_key_lower, query_key_upper, std::move(bucket_pl_iters)));
}

libtextclassifier3::Status IntegerIndexStorage::PersistToDisk() {
  ICING_RETURN_IF_ERROR(sorted_buckets_->PersistToDisk());
  ICING_RETURN_IF_ERROR(unsorted_buckets_->PersistToDisk());
  if (!flash_index_storage_->PersistToDisk()) {
    return absl_ports::InternalError(
        "Fail to persist FlashIndexStorage to disk");
  }

  ICING_RETURN_IF_ERROR(UpdateChecksums(crcs(), info(), sorted_buckets_.get(),
                                        unsorted_buckets_.get(),
                                        flash_index_storage_.get()));
  // Changes should have been applied to the underlying file when using
  // MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, but call msync() as an
  // extra safety step to ensure they are written out.
  ICING_RETURN_IF_ERROR(metadata_mmapped_file_->PersistToDisk());

  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
IntegerIndexStorage::InitializeNewFiles(
    const Filesystem& filesystem, std::string&& working_dir, Options&& options,
    PostingListIntegerIndexSerializer* posting_list_serializer) {
  // Create working directory.
  if (!filesystem.CreateDirectoryRecursively(working_dir.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to create directory: ", working_dir));
  }

  // TODO(b/259743562): [Optimization 1] decide max # buckets, unsorted buckets
  //                    threshold
  // Initialize sorted_buckets
  int32_t pre_mapping_mmap_size = sizeof(Bucket) * (1 << 10);
  int32_t max_file_size =
      pre_mapping_mmap_size + FileBackedVector<Bucket>::Header::kHeaderSize;
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Bucket>> sorted_buckets,
      FileBackedVector<Bucket>::Create(
          filesystem, GetSortedBucketsFilePath(working_dir),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, max_file_size,
          pre_mapping_mmap_size));

  // Initialize unsorted_buckets
  pre_mapping_mmap_size = sizeof(Bucket) * 100;
  max_file_size =
      pre_mapping_mmap_size + FileBackedVector<Bucket>::Header::kHeaderSize;
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Bucket>> unsorted_buckets,
      FileBackedVector<Bucket>::Create(
          filesystem, GetUnsortedBucketsFilePath(working_dir),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, max_file_size,
          pre_mapping_mmap_size));

  // Initialize flash_index_storage
  ICING_ASSIGN_OR_RETURN(
      FlashIndexStorage flash_index_storage,
      FlashIndexStorage::Create(GetFlashIndexStorageFilePath(working_dir),
                                &filesystem, posting_list_serializer));

  if (options.HasCustomInitBuckets()) {
    // Insert custom init buckets.
    std::sort(options.custom_init_sorted_buckets.begin(),
              options.custom_init_sorted_buckets.end());
    ICING_ASSIGN_OR_RETURN(
        typename FileBackedVector<Bucket>::MutableArrayView
            mutable_new_sorted_bucket_arr,
        sorted_buckets->Allocate(options.custom_init_sorted_buckets.size()));
    mutable_new_sorted_bucket_arr.SetArray(
        /*idx=*/0, options.custom_init_sorted_buckets.data(),
        options.custom_init_sorted_buckets.size());

    ICING_ASSIGN_OR_RETURN(typename FileBackedVector<Bucket>::MutableArrayView
                               mutable_new_unsorted_bucket_arr,
                           unsorted_buckets->Allocate(
                               options.custom_init_unsorted_buckets.size()));
    mutable_new_unsorted_bucket_arr.SetArray(
        /*idx=*/0, options.custom_init_unsorted_buckets.data(),
        options.custom_init_unsorted_buckets.size());

    // After inserting buckets, we can clear vectors since there is no need to
    // cache them.
    options.custom_init_sorted_buckets.clear();
    options.custom_init_unsorted_buckets.clear();
  } else {
    // Insert one bucket with range [INT64_MIN, INT64_MAX].
    ICING_RETURN_IF_ERROR(sorted_buckets->Append(Bucket(
        /*key_lower=*/std::numeric_limits<int64_t>::min(),
        /*key_upper=*/std::numeric_limits<int64_t>::max())));
  }
  ICING_RETURN_IF_ERROR(sorted_buckets->PersistToDisk());

  // Create and initialize new info
  Info new_info;
  new_info.magic = Info::kMagic;
  new_info.num_keys = 0;

  // Compute checksums
  Crcs new_crcs;
  ICING_RETURN_IF_ERROR(
      UpdateChecksums(&new_crcs, &new_info, sorted_buckets.get(),
                      unsorted_buckets.get(), &flash_index_storage));

  const std::string metadata_file_path = GetMetadataFilePath(working_dir);
  // Write new metadata file
  ICING_RETURN_IF_ERROR(
      WriteMetadata(filesystem, metadata_file_path, &new_crcs, &new_info));

  // Mmap the content of the crcs and info.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem, metadata_file_path,
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));

  return std::unique_ptr<IntegerIndexStorage>(new IntegerIndexStorage(
      filesystem, std::move(working_dir), std::move(options),
      posting_list_serializer,
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file)),
      std::move(sorted_buckets), std::move(unsorted_buckets),
      std::make_unique<FlashIndexStorage>(std::move(flash_index_storage))));
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
IntegerIndexStorage::InitializeExistingFiles(
    const Filesystem& filesystem, std::string&& working_dir, Options&& options,
    PostingListIntegerIndexSerializer* posting_list_serializer) {
  // Mmap the content of the crcs and info.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem, GetMetadataFilePath(working_dir),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));

  // TODO(b/259743562): [Optimization 1] decide max # buckets, unsorted buckets
  //                    threshold
  // Initialize sorted_buckets
  int32_t pre_mapping_mmap_size = sizeof(Bucket) * (1 << 10);
  int32_t max_file_size =
      pre_mapping_mmap_size + FileBackedVector<Bucket>::Header::kHeaderSize;
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Bucket>> sorted_buckets,
      FileBackedVector<Bucket>::Create(
          filesystem, GetSortedBucketsFilePath(working_dir),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, max_file_size,
          pre_mapping_mmap_size));

  // Initialize unsorted_buckets
  pre_mapping_mmap_size = sizeof(Bucket) * 100;
  max_file_size =
      pre_mapping_mmap_size + FileBackedVector<Bucket>::Header::kHeaderSize;
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Bucket>> unsorted_buckets,
      FileBackedVector<Bucket>::Create(
          filesystem, GetUnsortedBucketsFilePath(working_dir),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, max_file_size,
          pre_mapping_mmap_size));

  // Initialize flash_index_storage
  ICING_ASSIGN_OR_RETURN(
      FlashIndexStorage flash_index_storage,
      FlashIndexStorage::Create(GetFlashIndexStorageFilePath(working_dir),
                                &filesystem, posting_list_serializer));

  Crcs* crcs_ptr = reinterpret_cast<Crcs*>(
      metadata_mmapped_file.mutable_region() + Crcs::kFileOffset);
  Info* info_ptr = reinterpret_cast<Info*>(
      metadata_mmapped_file.mutable_region() + Info::kFileOffset);
  // Validate checksums of info and 3 storages.
  ICING_RETURN_IF_ERROR(
      ValidateChecksums(crcs_ptr, info_ptr, sorted_buckets.get(),
                        unsorted_buckets.get(), &flash_index_storage));

  return std::unique_ptr<IntegerIndexStorage>(new IntegerIndexStorage(
      filesystem, std::move(working_dir), std::move(options),
      posting_list_serializer,
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file)),
      std::move(sorted_buckets), std::move(unsorted_buckets),
      std::make_unique<FlashIndexStorage>(std::move(flash_index_storage))));
}

libtextclassifier3::StatusOr<std::vector<IntegerIndexStorage::Bucket>>
IntegerIndexStorage::AddKeysIntoBucketAndSplitIfNecessary(
    DocumentId document_id, SectionId section_id,
    const std::vector<int64_t>::const_iterator& it_start,
    const std::vector<int64_t>::const_iterator& it_end,
    FileBackedVector<Bucket>::MutableView& mutable_bucket) {
  std::unique_ptr<PostingListIntegerIndexAccessor> pl_accessor;
  if (mutable_bucket.Get().posting_list_identifier().is_valid()) {
    ICING_ASSIGN_OR_RETURN(
        pl_accessor, PostingListIntegerIndexAccessor::CreateFromExisting(
                         flash_index_storage_.get(), posting_list_serializer_,
                         mutable_bucket.Get().posting_list_identifier()));
  } else {
    ICING_ASSIGN_OR_RETURN(
        pl_accessor, PostingListIntegerIndexAccessor::Create(
                         flash_index_storage_.get(), posting_list_serializer_));
  }

  for (auto it = it_start; it != it_end; ++it) {
    // TODO(b/259743562): [Optimization 1] implement split bucket if pl is full
    //                    and the bucket is splittable
    ICING_RETURN_IF_ERROR(pl_accessor->PrependData(
        IntegerIndexData(section_id, document_id, *it)));
  }

  // TODO(b/259743562): [Optimization 1] implement split and return new buckets.
  //                    We will change the original bucket (mutable_bucket)
  //                    in-place to one of the new buckets, and the rest will
  //                    be returned and added into unsorted buckets in AddKeys.
  PostingListAccessor::FinalizeResult result =
      std::move(*pl_accessor).Finalize();
  if (!result.status.ok()) {
    return result.status;
  }

  mutable_bucket.Get().set_posting_list_identifier(result.id);

  return std::vector<Bucket>();
}
}  // namespace lib
}  // namespace icing
