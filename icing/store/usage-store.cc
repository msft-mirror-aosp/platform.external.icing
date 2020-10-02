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

#include "icing/store/usage-store.h"

#include "icing/file/file-backed-vector.h"
#include "icing/proto/usage.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

namespace {
std::string MakeUsageScoreCacheFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/usage-scores");
}
}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<UsageStore>> UsageStore::Create(
    const Filesystem* filesystem, const std::string& base_dir) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);

  auto usage_score_cache_or = FileBackedVector<UsageScores>::Create(
      *filesystem, MakeUsageScoreCacheFilename(base_dir),
      MemoryMappedFile::READ_WRITE_AUTO_SYNC);

  if (!usage_score_cache_or.ok()) {
    ICING_LOG(ERROR) << usage_score_cache_or.status().error_message()
                     << "Failed to initialize usage_score_cache";
    return usage_score_cache_or.status();
  }

  return std::unique_ptr<UsageStore>(new UsageStore(
      std::move(usage_score_cache_or).ValueOrDie(), *filesystem, base_dir));
}

libtextclassifier3::Status UsageStore::AddUsageReport(const UsageReport& report,
                                                      DocumentId document_id) {
  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Document id %d is invalid.", document_id));
  }

  auto usage_scores_or = usage_score_cache_->Get(document_id);

  // OutOfRange means that the mapper hasn't seen this document id before, it's
  // not an error here.
  UsageScores usage_scores;
  if (usage_scores_or.ok()) {
    usage_scores = *std::move(usage_scores_or).ValueOrDie();
  } else if (!absl_ports::IsOutOfRange(usage_scores_or.status())) {
    // Real error
    return usage_scores_or.status();
  }

  // Update last used timestamps and type counts. The counts won't be
  // incremented if they are already the maximum values. The timestamp from
  // UsageReport is in milliseconds, we need to convert it to seconds.
  int64_t report_timestamp_s = report.usage_timestamp_ms() / 1000;

  switch (report.usage_type()) {
    case UsageReport::USAGE_TYPE1:
      if (report_timestamp_s > std::numeric_limits<uint32_t>::max()) {
        usage_scores.usage_type1_last_used_timestamp_s =
            std::numeric_limits<uint32_t>::max();
      } else if (report_timestamp_s >
                 usage_scores.usage_type1_last_used_timestamp_s) {
        usage_scores.usage_type1_last_used_timestamp_s = report_timestamp_s;
      }

      if (usage_scores.usage_type1_count < std::numeric_limits<int>::max()) {
        ++usage_scores.usage_type1_count;
      }
      break;
    case UsageReport::USAGE_TYPE2:
      if (report_timestamp_s > std::numeric_limits<uint32_t>::max()) {
        usage_scores.usage_type2_last_used_timestamp_s =
            std::numeric_limits<uint32_t>::max();
      } else if (report_timestamp_s >
                 usage_scores.usage_type2_last_used_timestamp_s) {
        usage_scores.usage_type2_last_used_timestamp_s = report_timestamp_s;
      }

      if (usage_scores.usage_type2_count < std::numeric_limits<int>::max()) {
        ++usage_scores.usage_type2_count;
      }
      break;
    case UsageReport::USAGE_TYPE3:
      if (report_timestamp_s > std::numeric_limits<uint32_t>::max()) {
        usage_scores.usage_type3_last_used_timestamp_s =
            std::numeric_limits<uint32_t>::max();
      } else if (report_timestamp_s >
                 usage_scores.usage_type3_last_used_timestamp_s) {
        usage_scores.usage_type3_last_used_timestamp_s = report_timestamp_s;
      }

      if (usage_scores.usage_type3_count < std::numeric_limits<int>::max()) {
        ++usage_scores.usage_type3_count;
      }
  }

  // Write updated usage scores to file.
  ICING_RETURN_IF_ERROR(usage_score_cache_->Set(document_id, usage_scores));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status UsageStore::DeleteUsageScores(
    DocumentId document_id) {
  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Document id %d is invalid.", document_id));
  }

  // Clear all the scores of the document.
  ICING_RETURN_IF_ERROR(usage_score_cache_->Set(document_id, UsageScores()));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<UsageStore::UsageScores>
UsageStore::GetUsageScores(DocumentId document_id) {
  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Document id %d is invalid.", document_id));
  }

  auto usage_scores_or = usage_score_cache_->Get(document_id);
  if (absl_ports::IsOutOfRange(usage_scores_or.status())) {
    // No usage scores found. Return the default scores.
    return UsageScores();
  } else if (!usage_scores_or.ok()) {
    // Pass up any other errors.
    return usage_scores_or.status();
  }

  return *std::move(usage_scores_or).ValueOrDie();
}

libtextclassifier3::Status UsageStore::SetUsageScores(
    DocumentId document_id, UsageScores usage_scores) {
  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Document id %d is invalid.", document_id));
  }

  ICING_RETURN_IF_ERROR(usage_score_cache_->Set(document_id, usage_scores));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status UsageStore::PersistToDisk() {
  ICING_RETURN_IF_ERROR(usage_score_cache_->PersistToDisk());
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status UsageStore::Reset() {
  // We delete all the scores by deleting the whole file.
  libtextclassifier3::Status status = FileBackedVector<int64_t>::Delete(
      filesystem_, MakeUsageScoreCacheFilename(base_dir_));
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete usage_score_cache";
    return status;
  }

  // Create a new usage_score_cache
  auto usage_score_cache_or = FileBackedVector<UsageScores>::Create(
      filesystem_, MakeUsageScoreCacheFilename(base_dir_),
      MemoryMappedFile::READ_WRITE_AUTO_SYNC);
  if (!usage_score_cache_or.ok()) {
    ICING_LOG(ERROR) << usage_score_cache_or.status().error_message()
                     << "Failed to re-create usage_score_cache";
    return usage_score_cache_or.status();
  }
  usage_score_cache_ = std::move(usage_score_cache_or).ValueOrDie();

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
