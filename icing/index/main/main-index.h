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

#ifndef ICING_INDEX_MAIN_MAIN_INDEX_H_
#define ICING_INDEX_MAIN_MAIN_INDEX_H_

#include <memory>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"
#include "icing/index/lite/lite-index.h"
#include "icing/index/main/flash-index-storage.h"
#include "icing/index/main/posting-list-accessor.h"
#include "icing/index/term-id-codec.h"
#include "icing/legacy/index/icing-dynamic-trie.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

class MainIndex {
 public:
  static libtextclassifier3::StatusOr<MainIndex> Create(
      const string& index_filename, const Filesystem* filesystem,
      const IcingFilesystem* icing_filesystem);

  // Get a PostingListAccessor that holds the posting list chain for 'term'.
  //
  // RETURNS:
  //  - On success, a valid PostingListAccessor
  //  - NOT_FOUND if term is not present in the main index.
  libtextclassifier3::StatusOr<std::unique_ptr<PostingListAccessor>>
  GetAccessorForExactTerm(const std::string& term);

  // Get a PostingListAccessor for 'prefix'.
  //
  // RETURNS:
  //  - On success, a result containing a valid PostingListAccessor.
  //  - NOT_FOUND if neither 'prefix' nor any terms for which 'prefix' is a
  //    prefix are present in the main index.
  struct GetPrefixAccessorResult {
    // A PostingListAccessor that holds the posting list chain for the term
    // that best represents 'prefix' in the main index.
    std::unique_ptr<PostingListAccessor> accessor;
    // True if the returned posting list chain is for 'prefix' or false if the
    // returned posting list chain is for a term for which 'prefix' is a prefix.
    bool exact;
  };
  libtextclassifier3::StatusOr<GetPrefixAccessorResult>
  GetAccessorForPrefixTerm(const std::string& prefix);

  struct LexiconMergeOutputs {
    // Maps from main_lexicon tvi for new branching point to the main_lexicon
    // tvi for posting list whose hits must be backfilled.
    std::unordered_map<uint32_t, uint32_t> backfill_map;

    // Maps from lexicon tvis to main_lexicon tvis.
    std::unordered_map<uint32_t, uint32_t> other_tvi_to_main_tvi;

    // Maps from the lexicon tvi to the beginning position in
    // prefix_tvis_buf and the length.
    std::unordered_map<uint32_t, std::pair<int, int>>
        other_tvi_to_prefix_main_tvis;

    // Stores tvis that are mapped to by other_tvi_to_prefix_tvis.
    std::vector<uint32_t> prefix_tvis_buf;
  };

  // Merge the lexicon into the main lexicon and populate the data
  // structures necessary to translate lite tvis to main tvis, track backfilling
  // and expanding lite terms to prefix terms.
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL on IO error while writing to the main lexicon.
  libtextclassifier3::StatusOr<LexiconMergeOutputs> MergeLexicon(
      const IcingDynamicTrie& other_lexicon) {
    // Backfill branch points need to be added first so that the backfill_map
    // can be correctly populated.
    ICING_ASSIGN_OR_RETURN(LexiconMergeOutputs outputs,
                           AddBackfillBranchPoints(other_lexicon));
    ICING_ASSIGN_OR_RETURN(outputs,
                           AddTerms(other_lexicon, std::move(outputs)));
    // Non-backfill branch points need to be added last so that the mapping of
    // newly added terms to prefix terms can be correctly populated (prefix
    // terms might be branch points between two new terms or between a
    // pre-existing term and a new term).
    ICING_ASSIGN_OR_RETURN(outputs,
                           AddBranchPoints(other_lexicon, std::move(outputs)));
    return outputs;
  }

  // Add hits to the main index and backfill from existing posting lists to new
  // backfill branch points.
  //
  // RETURNS:
  //  - OK on success
  //  - INVALID_ARGUMENT if one of the elements in the lite index has a term_id
  //  exceeds the max TermId, is not valid or is not less than pre-existing hits
  //  in the main index.
  //  - INTERNAL_ERROR if unable to mmap necessary IndexBlocks
  //  - RESOURCE_EXHAUSTED error if unable to grow the index
  libtextclassifier3::Status AddHits(
      const TermIdCodec& term_id_codec,
      std::unordered_map<uint32_t, uint32_t>&& backfill_map,
      std::vector<LiteIndex::Element>&& hits);

 private:
  libtextclassifier3::Status Init(const string& index_filename,
                                  const Filesystem* filesystem,
                                  const IcingFilesystem* icing_filesystem);

  // Helpers for merging the lexicon
  // Add all 'backfill' branch points. Backfill branch points are prefix
  // branch points that are a prefix of terms that existed in the lexicon
  // to the merge.
  //
  // For example, if the main lexicon only contains "foot" and is then merged
  // with a lite lexicon containing only "fool", then a backfill branch point
  // for "foo" will be added to contain prefix hits from both the pre-existing
  // posting list for "foot" and the new posting list for "fool".
  //
  // Populates LexiconMergeOutputs.backfill_map
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL on IO error while writing to the main lexicon.
  libtextclassifier3::StatusOr<LexiconMergeOutputs> AddBackfillBranchPoints(
      const IcingDynamicTrie& other_lexicon);

  // Add all terms from the lexicon.
  //
  // Populates LexiconMergeOutputs.other_tvi_to_main_tvi
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL on IO error while writing to the main lexicon.
  libtextclassifier3::StatusOr<LexiconMergeOutputs> AddTerms(
      const IcingDynamicTrie& other_lexicon, LexiconMergeOutputs&& outputs);

  // Add all branch points for terms added from the lexicon.
  // For example, if the main lexicon is empty and is then merged with a
  // lexicon containing only "foot" and "fool", then a branch point for "foo"
  // will be added to contain prefix hits from both "foot" and "fool".
  //
  // Populates LexiconMergeOutputs.other_tvi_to_prefix_main_tvis and
  // LexiconMergeOutputs.prefix_tvis_buf;
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL on IO error while writing to the main lexicon.
  libtextclassifier3::StatusOr<LexiconMergeOutputs> AddBranchPoints(
      const IcingDynamicTrie& other_lexicon, LexiconMergeOutputs&& outputs);

  // Copies all properties from old_tvi in the other lexicon to the new_tvi in
  // the main lexicon.
  // Returns true on success, false if an IO error is encountered.
  bool CopyProperties(const IcingDynamicTrie::PropertyReadersAll& prop_reader,
                      const IcingDynamicTrie& other_lexicon, uint32_t other_tvi,
                      uint32_t new_main_tvi);

  std::unique_ptr<FlashIndexStorage> flash_index_;
  std::unique_ptr<IcingDynamicTrie> main_lexicon_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_MAIN_MAIN_INDEX_H_
