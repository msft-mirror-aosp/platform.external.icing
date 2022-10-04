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

#include "icing/index/lite/lite-index.h"

#include <sys/mman.h>

#include <algorithm>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/hit/hit.h"
#include "icing/index/lite/lite-index-header.h"
#include "icing/index/term-property-id.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/legacy/core/icing-timer.h"
#include "icing/legacy/index/icing-array-storage.h"
#include "icing/legacy/index/icing-dynamic-trie.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/legacy/index/icing-mmapper.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Point at which we declare the trie full.
constexpr double kTrieFullFraction = 0.95;

std::string MakeHitBufferFilename(const std::string& filename_base) {
  return filename_base + "hb";
}

size_t header_size() { return sizeof(LiteIndex_HeaderImpl::HeaderData); }

}  // namespace

const TermIdHitPair::Value TermIdHitPair::kInvalidValue =
    TermIdHitPair(0, Hit()).value();

libtextclassifier3::StatusOr<std::unique_ptr<LiteIndex>> LiteIndex::Create(
    const LiteIndex::Options& options, const IcingFilesystem* filesystem) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);

  std::unique_ptr<LiteIndex> lite_index =
      std::unique_ptr<LiteIndex>(new LiteIndex(options, filesystem));
  ICING_RETURN_IF_ERROR(lite_index->Initialize());
  return std::move(lite_index);
}

// size is max size in elements. An appropriate lexicon and display
// mapping size will be chosen based on hit buffer size.
LiteIndex::LiteIndex(const LiteIndex::Options& options,
                     const IcingFilesystem* filesystem)
    : hit_buffer_(*filesystem),
      hit_buffer_crc_(0),
      lexicon_(options.filename_base + "lexicon", MakeTrieRuntimeOptions(),
               filesystem),
      header_mmap_(false, MAP_SHARED),
      options_(options),
      filesystem_(filesystem) {}

LiteIndex::~LiteIndex() {
  if (initialized()) {
    libtextclassifier3::Status unused = PersistToDisk();
  }
}

IcingDynamicTrie::RuntimeOptions LiteIndex::MakeTrieRuntimeOptions() {
  return IcingDynamicTrie::RuntimeOptions().set_storage_policy(
      IcingDynamicTrie::RuntimeOptions::kMapSharedWithCrc);
}

libtextclassifier3::Status LiteIndex::Initialize() {
  // Size of hit buffer's header struct, rounded up to the nearest number of
  // system memory pages.
  const size_t header_padded_size =
      IcingMMapper::page_aligned_size(header_size());

  // Variable declarations cannot cross goto jumps, so declare these up top.
  libtextclassifier3::Status status;
  uint64_t file_size;
  IcingTimer timer;

  if (!lexicon_.CreateIfNotExist(options_.lexicon_options) ||
      !lexicon_.Init()) {
    return absl_ports::InternalError("Failed to initialize lexicon trie");
  }

  hit_buffer_fd_.reset(filesystem_->OpenForWrite(
      MakeHitBufferFilename(options_.filename_base).c_str()));
  if (!hit_buffer_fd_.is_valid()) {
    status = absl_ports::InternalError("Failed to open hit buffer file");
    goto error;
  }

  file_size = filesystem_->GetFileSize(hit_buffer_fd_.get());
  if (file_size == IcingFilesystem::kBadFileSize) {
    status = absl_ports::InternalError("Failed to query hit buffer file size");
    goto error;
  }

  if (file_size < header_padded_size) {
    if (file_size != 0) {
      status = absl_ports::InternalError(IcingStringUtil::StringPrintf(
          "Hit buffer had unexpected size %" PRIu64, file_size));
      goto error;
    }

    ICING_VLOG(2) << "Creating new hit buffer";
    // Make sure files are fresh.
    if (!lexicon_.Remove() ||
        !lexicon_.CreateIfNotExist(options_.lexicon_options) ||
        !lexicon_.Init()) {
      status =
          absl_ports::InternalError("Failed to refresh lexicon during clear");
      goto error;
    }

    // Create fresh hit buffer by first emptying the hit buffer file and then
    // allocating header_padded_size of the cleared space.
    if (!filesystem_->Truncate(hit_buffer_fd_.get(), 0) ||
        !filesystem_->Truncate(hit_buffer_fd_.get(), header_padded_size)) {
      status = absl_ports::InternalError("Failed to truncate hit buffer file");
      goto error;
    }

    // Set up header.
    header_mmap_.Remap(hit_buffer_fd_.get(), 0, header_size());
    header_ = std::make_unique<LiteIndex_HeaderImpl>(
        reinterpret_cast<LiteIndex_HeaderImpl::HeaderData*>(
            header_mmap_.address()));
    header_->Reset();

    if (!hit_buffer_.Init(hit_buffer_fd_.get(), header_padded_size, true,
                          sizeof(TermIdHitPair::Value), header_->cur_size(),
                          options_.hit_buffer_size, &hit_buffer_crc_, true)) {
      status = absl_ports::InternalError("Failed to initialize new hit buffer");
      goto error;
    }

    UpdateChecksum();
  } else {
    header_mmap_.Remap(hit_buffer_fd_.get(), 0, header_size());
    header_ = std::make_unique<LiteIndex_HeaderImpl>(
        reinterpret_cast<LiteIndex_HeaderImpl::HeaderData*>(
            header_mmap_.address()));

    if (!hit_buffer_.Init(hit_buffer_fd_.get(), header_padded_size, true,
                          sizeof(TermIdHitPair::Value), header_->cur_size(),
                          options_.hit_buffer_size, &hit_buffer_crc_, true)) {
      status = absl_ports::InternalError(
          "Failed to re-initialize existing hit buffer");
      goto error;
    }

    // Check integrity.
    if (!header_->check_magic()) {
      status = absl_ports::InternalError("Lite index header magic mismatch");
      goto error;
    }
    Crc32 crc = ComputeChecksum();
    if (crc.Get() != header_->lite_index_crc()) {
      status = absl_ports::DataLossError(
          IcingStringUtil::StringPrintf("Lite index crc check failed: %u vs %u",
                                        crc.Get(), header_->lite_index_crc()));
      goto error;
    }
  }

  ICING_VLOG(2) << "Lite index init ok in " << timer.Elapsed() * 1000 << "ms";
  return status;

error:
  header_ = nullptr;
  header_mmap_.Unmap();
  lexicon_.Close();
  hit_buffer_crc_ = 0;
  hit_buffer_.Reset();
  hit_buffer_fd_.reset();
  if (status.ok()) {
    return absl_ports::InternalError(
        "Error handling code ran but status was ok");
  }
  return status;
}

Crc32 LiteIndex::ComputeChecksum() {
  IcingTimer timer;

  // Update crcs.
  uint32_t dependent_crcs[2];
  hit_buffer_.UpdateCrc();
  dependent_crcs[0] = hit_buffer_crc_;
  dependent_crcs[1] = lexicon_.UpdateCrc();

  // Compute the master crc.

  // Header crc, excluding the actual crc field.
  Crc32 all_crc(header_->CalculateHeaderCrc());
  all_crc.Append(std::string_view(reinterpret_cast<const char*>(dependent_crcs),
                                  sizeof(dependent_crcs)));
  ICING_VLOG(2) << "Lite index crc computed in " << timer.Elapsed() * 1000
                << "ms";

  return all_crc;
}

libtextclassifier3::Status LiteIndex::Reset() {
  IcingTimer timer;

  // TODO(b/140436942): When these components have been changed to return errors
  // they should be propagated from here.
  lexicon_.Clear();
  hit_buffer_.Clear();
  header_->Reset();
  UpdateChecksum();

  ICING_VLOG(2) << "Lite index clear in " << timer.Elapsed() * 1000 << "ms";
  return libtextclassifier3::Status::OK;
}

void LiteIndex::Warm() {
  hit_buffer_.Warm();
  lexicon_.Warm();
}

libtextclassifier3::Status LiteIndex::PersistToDisk() {
  bool success = true;
  if (!lexicon_.Sync()) {
    ICING_VLOG(1) << "Failed to sync the lexicon.";
    success = false;
  }
  hit_buffer_.Sync();
  UpdateChecksum();
  header_mmap_.Sync();

  return (success) ? libtextclassifier3::Status::OK
                   : absl_ports::InternalError(
                         "Unable to sync lite index components.");
}

void LiteIndex::UpdateChecksum() {
  header_->set_lite_index_crc(ComputeChecksum().Get());
}

libtextclassifier3::StatusOr<uint32_t> LiteIndex::InsertTerm(
    const std::string& term, TermMatchType::Code term_match_type,
    NamespaceId namespace_id) {
  uint32_t tvi;
  if (!lexicon_.Insert(term.c_str(), "", &tvi, false)) {
    return absl_ports::ResourceExhaustedError(
        absl_ports::StrCat("Unable to add term ", term, " to lexicon!"));
  }
  ICING_RETURN_IF_ERROR(UpdateTermProperties(
      tvi, term_match_type == TermMatchType::PREFIX, namespace_id));
  return tvi;
}

libtextclassifier3::Status LiteIndex::UpdateTermProperties(
    uint32_t tvi, bool hasPrefixHits, NamespaceId namespace_id) {
  if (hasPrefixHits &&
      !lexicon_.SetProperty(tvi, GetHasHitsInPrefixSectionPropertyId())) {
    return absl_ports::ResourceExhaustedError(
        "Insufficient disk space to create prefix property!");
  }

  if (!lexicon_.SetProperty(tvi, GetNamespacePropertyId(namespace_id))) {
    return absl_ports::ResourceExhaustedError(
        "Insufficient disk space to create namespace property!");
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status LiteIndex::AddHit(uint32_t term_id, const Hit& hit) {
  if (is_full()) {
    return absl_ports::ResourceExhaustedError("Hit buffer is full!");
  }

  TermIdHitPair term_id_hit_pair(term_id, hit);
  uint32_t cur_size = header_->cur_size();
  TermIdHitPair::Value* valp =
      hit_buffer_.GetMutableMem<TermIdHitPair::Value>(cur_size, 1);
  if (valp == nullptr) {
    return absl_ports::ResourceExhaustedError(
        "Allocating more space in hit buffer failed!");
  }
  *valp = term_id_hit_pair.value();
  header_->set_cur_size(cur_size + 1);

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<uint32_t> LiteIndex::GetTermId(
    const std::string& term) const {
  char dummy;
  uint32_t tvi;
  if (!lexicon_.Find(term.c_str(), &dummy, &tvi)) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Could not find ", term, " in the lexicon."));
  }
  return tvi;
}

int LiteIndex::AppendHits(
    uint32_t term_id, SectionIdMask section_id_mask,
    bool only_from_prefix_sections,
    const SuggestionResultChecker* suggestion_result_checker,
    std::vector<DocHitInfo>* hits_out,
    std::vector<Hit::TermFrequencyArray>* term_frequency_out) {
  int count = 0;
  DocumentId last_document_id = kInvalidDocumentId;
  // Record whether the last document belongs to the given namespaces.
  bool last_document_in_namespace = false;
  for (uint32_t idx = Seek(term_id); idx < header_->cur_size(); idx++) {
    TermIdHitPair term_id_hit_pair(
        hit_buffer_.array_cast<TermIdHitPair>()[idx]);
    if (term_id_hit_pair.term_id() != term_id) break;

    const Hit& hit = term_id_hit_pair.hit();
    // Check sections.
    if (((UINT64_C(1) << hit.section_id()) & section_id_mask) == 0) {
      continue;
    }
    // Check prefix section only.
    if (only_from_prefix_sections && !hit.is_in_prefix_section()) {
      continue;
    }
    DocumentId document_id = hit.document_id();
    if (document_id != last_document_id) {
      last_document_id = document_id;
      last_document_in_namespace =
          suggestion_result_checker == nullptr ||
          suggestion_result_checker->BelongsToTargetResults(document_id,
                                                            hit.section_id());
      if (!last_document_in_namespace) {
        // The document is removed or expired or not belongs to target
        // namespaces.
        continue;
      }
      ++count;
      if (hits_out != nullptr) {
        hits_out->push_back(DocHitInfo(document_id));
        if (term_frequency_out != nullptr) {
          term_frequency_out->push_back(Hit::TermFrequencyArray());
        }
      }
    }
    if (hits_out != nullptr && last_document_in_namespace) {
      hits_out->back().UpdateSection(hit.section_id());
      if (term_frequency_out != nullptr) {
        term_frequency_out->back()[hit.section_id()] = hit.term_frequency();
      }
    }
  }
  return count;
}

libtextclassifier3::StatusOr<int> LiteIndex::CountHits(
    uint32_t term_id,
    const SuggestionResultChecker* suggestion_result_checker) {
  return AppendHits(term_id, kSectionIdMaskAll,
                    /*only_from_prefix_sections=*/false,
                    suggestion_result_checker,
                    /*hits_out=*/nullptr);
}

bool LiteIndex::is_full() const {
  return (header_->cur_size() == options_.hit_buffer_size ||
          lexicon_.min_free_fraction() < (1.0 - kTrieFullFraction));
}

std::string LiteIndex::GetDebugInfo(DebugInfoVerbosity::Code verbosity) {
  std::string res;
  std::string lexicon_info;
  lexicon_.GetDebugInfo(verbosity, &lexicon_info);
  IcingStringUtil::SStringAppendF(
      &res, 0,
      "curr_size: %u\n"
      "hit_buffer_size: %u\n"
      "last_added_document_id %u\n"
      "searchable_end: %u\n"
      "index_crc: %u\n"
      "\n"
      "lite_lexicon_info:\n%s\n",
      header_->cur_size(), options_.hit_buffer_size,
      header_->last_added_docid(), header_->searchable_end(),
      ComputeChecksum().Get(), lexicon_info.c_str());
  return res;
}

libtextclassifier3::StatusOr<int64_t> LiteIndex::GetElementsSize() const {
  IndexStorageInfoProto storage_info = GetStorageInfo(IndexStorageInfoProto());
  if (storage_info.lite_index_hit_buffer_size() == -1 ||
      storage_info.lite_index_lexicon_size() == -1) {
    return absl_ports::AbortedError(
        "Failed to get size of LiteIndex's members.");
  }
  // On initialization, we grow the file to a padded size first. So this size
  // won't count towards the size taken up by elements
  size_t header_padded_size = IcingMMapper::page_aligned_size(header_size());
  return storage_info.lite_index_hit_buffer_size() - header_padded_size +
         storage_info.lite_index_lexicon_size();
}

IndexStorageInfoProto LiteIndex::GetStorageInfo(
    IndexStorageInfoProto storage_info) const {
  int64_t header_and_hit_buffer_file_size =
      filesystem_->GetFileSize(hit_buffer_fd_.get());
  storage_info.set_lite_index_hit_buffer_size(
      IcingFilesystem::SanitizeFileSize(header_and_hit_buffer_file_size));
  int64_t lexicon_disk_usage = lexicon_.GetElementsSize();
  if (lexicon_disk_usage != Filesystem::kBadFileSize) {
    storage_info.set_lite_index_lexicon_size(lexicon_disk_usage);
  } else {
    storage_info.set_lite_index_lexicon_size(-1);
  }
  return storage_info;
}

void LiteIndex::SortHits() {
  // Make searchable by sorting by hit buffer.
  uint32_t sort_len = header_->cur_size() - header_->searchable_end();
  if (sort_len <= 0) {
    return;
  }
  IcingTimer timer;

  auto* array_start =
      hit_buffer_.GetMutableMem<TermIdHitPair::Value>(0, header_->cur_size());
  TermIdHitPair::Value* sort_start = array_start + header_->searchable_end();
  std::sort(sort_start, array_start + header_->cur_size());

  // Now merge with previous region. Since the previous region is already
  // sorted and deduplicated, optimize the merge by skipping everything less
  // than the new region's smallest value.
  if (header_->searchable_end() > 0) {
    std::inplace_merge(array_start, array_start + header_->searchable_end(),
                       array_start + header_->cur_size());
  }
  ICING_VLOG(2) << "Lite index sort and merge " << sort_len << " into "
                << header_->searchable_end() << " in " << timer.Elapsed() * 1000
                << "ms";

  // Now the entire array is sorted.
  header_->set_searchable_end(header_->cur_size());

  // Update crc in-line.
  UpdateChecksum();
}

uint32_t LiteIndex::Seek(uint32_t term_id) {
  SortHits();

  // Binary search for our term_id.  Make sure we get the first
  // element.  Using kBeginSortValue ensures this for the hit value.
  TermIdHitPair term_id_hit_pair(
      term_id, Hit(Hit::kMaxDocumentIdSortValue, Hit::kDefaultTermFrequency));

  const TermIdHitPair::Value* array =
      hit_buffer_.array_cast<TermIdHitPair::Value>();
  const TermIdHitPair::Value* ptr = std::lower_bound(
      array, array + header_->cur_size(), term_id_hit_pair.value());
  return ptr - array;
}

libtextclassifier3::Status LiteIndex::Optimize(
    const std::vector<DocumentId>& document_id_old_to_new,
    const TermIdCodec* term_id_codec, DocumentId new_last_added_document_id) {
  header_->set_last_added_docid(new_last_added_document_id);
  if (header_->cur_size() == 0) {
    return libtextclassifier3::Status::OK;
  }
  // Sort the hits so that hits with the same term id will be grouped together,
  // which helps later to determine which terms will be unused after compaction.
  SortHits();
  uint32_t new_size = 0;
  uint32_t curr_term_id = 0;
  uint32_t curr_tvi = 0;
  std::unordered_set<uint32_t> tvi_to_delete;
  for (uint32_t idx = 0; idx < header_->cur_size(); ++idx) {
    TermIdHitPair term_id_hit_pair(
        hit_buffer_.array_cast<TermIdHitPair>()[idx]);
    if (idx == 0 || term_id_hit_pair.term_id() != curr_term_id) {
      curr_term_id = term_id_hit_pair.term_id();
      ICING_ASSIGN_OR_RETURN(TermIdCodec::DecodedTermInfo term_info,
                             term_id_codec->DecodeTermInfo(curr_term_id));
      curr_tvi = term_info.tvi;
      // Mark the property of the current term as not having hits in prefix
      // section. The property will be set below if there are any valid hits
      // from a prefix section.
      lexicon_.ClearProperty(curr_tvi, GetHasHitsInPrefixSectionPropertyId());
      // Add curr_tvi to tvi_to_delete. It will be removed from tvi_to_delete
      // below if there are any valid hits pointing to that termid.
      tvi_to_delete.insert(curr_tvi);
    }
    DocumentId new_document_id =
        document_id_old_to_new[term_id_hit_pair.hit().document_id()];
    if (new_document_id == kInvalidDocumentId) {
      continue;
    }
    if (term_id_hit_pair.hit().is_in_prefix_section()) {
      lexicon_.SetProperty(curr_tvi, GetHasHitsInPrefixSectionPropertyId());
    }
    tvi_to_delete.erase(curr_tvi);
    TermIdHitPair new_term_id_hit_pair(
        term_id_hit_pair.term_id(),
        Hit::TranslateHit(term_id_hit_pair.hit(), new_document_id));
    // Rewriting the hit_buffer in place.
    // new_size is weakly less than idx so we are okay to overwrite the entry at
    // new_size, and valp should never be nullptr since it is within the already
    // allocated region of hit_buffer_.
    TermIdHitPair::Value* valp =
        hit_buffer_.GetMutableMem<TermIdHitPair::Value>(new_size++, 1);
    *valp = new_term_id_hit_pair.value();
  }
  header_->set_cur_size(new_size);
  header_->set_searchable_end(new_size);

  // Delete unused terms.
  std::unordered_set<std::string> terms_to_delete;
  for (IcingDynamicTrie::Iterator term_iter(lexicon_, /*prefix=*/"");
       term_iter.IsValid(); term_iter.Advance()) {
    if (tvi_to_delete.find(term_iter.GetValueIndex()) != tvi_to_delete.end()) {
      terms_to_delete.insert(term_iter.GetKey());
    }
  }
  for (const std::string& term : terms_to_delete) {
    // Mark "term" as deleted. This won't actually free space in the lexicon. It
    // will simply make it impossible to Find "term" in subsequent calls (which
    // saves an unnecessary search through the hit buffer). This is acceptable
    // because the free space will eventually be reclaimed the next time that
    // the lite index is merged with the main index.
    if (!lexicon_.Delete(term)) {
      return absl_ports::InternalError(
          "Could not delete invalid terms in lite lexicon during compaction.");
    }
  }
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
