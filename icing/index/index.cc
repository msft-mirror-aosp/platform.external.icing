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

#include "icing/index/index.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "utils/base/status.h"
#include "utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/hit/hit.h"
#include "icing/index/iterator/doc-hit-info-iterator-term.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/lite-index.h"
#include "icing/index/term-id-codec.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/legacy/index/icing-dynamic-trie.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/section.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace {

libtextclassifier3::StatusOr<LiteIndex::Options> CreateLiteIndexOptions(
    const Index::Options& options) {
  if (options.index_merge_size <= 0) {
    return absl_ports::InvalidArgumentError(
        "Requested hit buffer size must be greater than 0.");
  }
  if (options.index_merge_size > LiteIndex::max_hit_buffer_size()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Requested hit buffer size %d is too large.",
        options.index_merge_size));
  }
  return LiteIndex::Options(options.base_dir + "/idx/lite.",
                            options.index_merge_size);
}

// TODO(tjbarron) implement for real when the main index is added.
IcingDynamicTrie::Options GetMainLexiconOptions() {
  return IcingDynamicTrie::Options();
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<Index>> Index::Create(
    const Options& options, const IcingFilesystem* filesystem) {
  ICING_ASSIGN_OR_RETURN(LiteIndex::Options lite_index_options,
                         CreateLiteIndexOptions(options));
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<TermIdCodec> term_id_codec,
      TermIdCodec::Create(
          IcingDynamicTrie::max_value_index(GetMainLexiconOptions()),
          IcingDynamicTrie::max_value_index(
              lite_index_options.lexicon_options)));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<LiteIndex> lite_index,
                         LiteIndex::Create(lite_index_options, filesystem));
  return std::unique_ptr<Index>(
      new Index(options, std::move(term_id_codec), std::move(lite_index)));
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
Index::GetIterator(const std::string& term, SectionIdMask section_id_mask,
                   TermMatchType::Code term_match_type) {
  switch (term_match_type) {
    case TermMatchType::EXACT_ONLY:
      return std::make_unique<DocHitInfoIteratorTermExact>(
          term_id_codec_.get(), lite_index_.get(), term, section_id_mask);
    case TermMatchType::PREFIX:
      return std::make_unique<DocHitInfoIteratorTermPrefix>(
          term_id_codec_.get(), lite_index_.get(), term, section_id_mask);
    default:
      return absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Invalid TermMatchType: ",
                             TermMatchType::Code_Name(term_match_type)));
  }
}

libtextclassifier3::Status Index::Editor::AddHit(const char* term,
                                                 Hit::Score score) {
  // Step 1: See if this term is already in the lexicon
  uint32_t tvi;
  auto tvi_or = lite_index_->FindTerm(term);

  // Step 2: Update the lexicon, either add the term or update its properties
  if (tvi_or.ok()) {
    ICING_VLOG(1) << "Term " << term
                  << " is already present in lexicon. Updating.";
    tvi = tvi_or.ValueOrDie();
    // Already in the lexicon. Just update the properties.
    ICING_RETURN_IF_ERROR(lite_index_->UpdateTerm(tvi, term_match_type_));
  } else {
    ICING_VLOG(1) << "Term " << term << " is not in lexicon. Inserting.";
    // Haven't seen this term before. Add it to the lexicon.
    ICING_ASSIGN_OR_RETURN(tvi,
                           lite_index_->InsertTerm(term, term_match_type_));
  }

  // Step 3: Add the hit itself
  Hit hit(section_id_, document_id_, score,
          term_match_type_ == TermMatchType::PREFIX);
  ICING_ASSIGN_OR_RETURN(uint32_t term_id,
                         term_id_codec_->EncodeTvi(tvi, TviType::LITE));
  return lite_index_->AddHit(term_id, hit);
}

}  // namespace lib
}  // namespace icing
