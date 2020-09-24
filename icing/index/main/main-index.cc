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
#include "icing/index/main/main-index.h"

#include <cstring>
#include <memory>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/term-id-codec.h"
#include "icing/index/term-property-id.h"
#include "icing/legacy/index/icing-dynamic-trie.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Finds the best prefix term in lexicon for which "prefix" is a prefix.
// 'Best' is defined as the shortest term that holds a valid posting list id.
// Returns a valid FindTermResult with found=true if either:
//   1. prefix exists as a term in lexicon.
//   2. the shortest, valid prefix in the lexicon exists and contains prefix
//      hits.
// Returns a FindTermResult with found=false and undefined values of tvi and
// exact if no term was found.
struct FindTermResult {
  // TVI of the term that was found. Undefined if found=false.
  uint32_t tvi;
  // Whether or not a valid term with prefix hits was found.
  bool found;
  // Whether or not that term is equal to 'prefix'
  bool exact;
};
FindTermResult FindShortestValidTermWithPrefixHits(
    const IcingDynamicTrie* lexicon, const std::string& prefix) {
  // For prefix indexing: when we are doing a prefix match for "prefix", find
  // the tvi to the equivalent posting list. prefix's own posting list might not
  // exist but one of its children acts as a proxy.
  IcingDynamicTrie::PropertyReader hits_in_prefix_section(
      *lexicon, GetHasHitsInPrefixSectionPropertyId());
  uint32_t tvi = 0;
  bool found = false;
  bool exact = false;
  for (IcingDynamicTrie::Iterator it(*lexicon, prefix.c_str()); it.IsValid();
       it.Advance()) {
    PostingListIdentifier posting_list_id = PostingListIdentifier::kInvalid;
    memcpy(&posting_list_id, it.GetValue(), sizeof(posting_list_id));

    // Posting list id might be invalid if this is also a backfill term.
    // Suppose that the main index has two pre-existing prefix hits "foot" and
    // "fool" - it will have a branch point posting list for "foo". Then, let's
    // suppose that the other index adds hits for "foul", "four" and "far". This
    // will result in branch points for "fo" and "f".
    // If "fo" was added before "f", then the iterator would first give us "fo".
    // "fo" will have an invalid posting_list_id because it hasn't been
    // backfilled yet, so we need to continue iterating to "foo".
    if (posting_list_id.is_valid()) {
      exact = (prefix.size() == strlen(it.GetKey()));
      tvi = it.GetValueIndex();
      // Found it. Does it have prefix hits?
      found = exact || hits_in_prefix_section.HasProperty(tvi);
      break;
    }
  }
  FindTermResult result = {tvi, found, exact};
  return result;
}

}  // namespace

libtextclassifier3::StatusOr<MainIndex> MainIndex::Create(
    const string& index_filename, const Filesystem* filesystem,
    const IcingFilesystem* icing_filesystem) {
  MainIndex main_index;
  ICING_RETURN_IF_ERROR(
      main_index.Init(index_filename, filesystem, icing_filesystem));
  return main_index;
}

// TODO(b/139087650) : Migrate off of IcingFilesystem.
libtextclassifier3::Status MainIndex::Init(
    const string& index_filename, const Filesystem* filesystem,
    const IcingFilesystem* icing_filesystem) {
  std::string flash_index_file = index_filename + "-main-index";
  ICING_ASSIGN_OR_RETURN(
      FlashIndexStorage flash_index,
      FlashIndexStorage::Create(flash_index_file, filesystem));
  flash_index_ = std::make_unique<FlashIndexStorage>(std::move(flash_index));

  std::string lexicon_file = index_filename + "-main-lexicon";
  IcingDynamicTrie::RuntimeOptions runtime_options;
  main_lexicon_ = std::make_unique<IcingDynamicTrie>(
      lexicon_file, runtime_options, icing_filesystem);
  IcingDynamicTrie::Options lexicon_options;
  if (!main_lexicon_->CreateIfNotExist(lexicon_options) ||
      !main_lexicon_->Init()) {
    return absl_ports::InternalError("Failed to initialize lexicon trie");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<std::unique_ptr<PostingListAccessor>>
MainIndex::GetAccessorForExactTerm(const std::string& term) {
  PostingListIdentifier posting_list_id = PostingListIdentifier::kInvalid;
  if (!main_lexicon_->Find(term.c_str(), &posting_list_id)) {
    return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
        "Term %s is not present in main lexicon.", term.c_str()));
  }
  ICING_ASSIGN_OR_RETURN(PostingListAccessor accessor,
                         PostingListAccessor::CreateFromExisting(
                             flash_index_.get(), posting_list_id));
  return std::make_unique<PostingListAccessor>(std::move(accessor));
}

libtextclassifier3::StatusOr<MainIndex::GetPrefixAccessorResult>
MainIndex::GetAccessorForPrefixTerm(const std::string& prefix) {
  bool exact = false;
  // For prefix indexing: when we are doing a prefix match for
  // "prefix", find the tvi to the equivalent posting list. prefix's
  // own posting list might not exist but its shortest child acts as a proxy.
  //
  // For example, if there are only two hits in the index are prefix hits for
  // "bar" and "bat", then both will appear on a posting list for "ba". "b"
  // won't have a posting list, but "ba" will suffice.
  IcingDynamicTrie::PropertyReader hits_in_prefix_section(
      *main_lexicon_, GetHasHitsInPrefixSectionPropertyId());
  IcingDynamicTrie::Iterator main_itr(*main_lexicon_, prefix.c_str());
  if (!main_itr.IsValid()) {
    return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
        "Term: %s is not present in the main lexicon.", prefix.c_str()));
  }
  exact = (prefix.length() == strlen(main_itr.GetKey()));

  if (!exact && !hits_in_prefix_section.HasProperty(main_itr.GetValueIndex())) {
    // Found it, but it doesn't have prefix hits. Exit early. No need to
    // retrieve the posting list because there's nothing there for us.
    return libtextclassifier3::Status::OK;
  }
  PostingListIdentifier posting_list_id = PostingListIdentifier::kInvalid;
  memcpy(&posting_list_id, main_itr.GetValue(), sizeof(posting_list_id));
  ICING_ASSIGN_OR_RETURN(PostingListAccessor pl_accessor,
                         PostingListAccessor::CreateFromExisting(
                             flash_index_.get(), posting_list_id));
  GetPrefixAccessorResult result = {std::make_unique<PostingListAccessor>(std::move(pl_accessor)), exact};
  return result;
}

libtextclassifier3::StatusOr<MainIndex::LexiconMergeOutputs>
MainIndex::AddBackfillBranchPoints(const IcingDynamicTrie& other_lexicon) {
  // Maps new branching points in main lexicon to the term such that
  // branching_point_term is a prefix of term and there are no terms smaller
  // than term and greater than branching_point_term.
  std::string prefix;
  LexiconMergeOutputs outputs;
  for (IcingDynamicTrie::Iterator other_term_itr(other_lexicon, /*prefix=*/"");
       other_term_itr.IsValid(); other_term_itr.Advance()) {
    // If term were inserted in the main lexicon, what new branching would it
    // create? (It always creates at most one.)
    int prefix_len = main_lexicon_->FindNewBranchingPrefixLength(
        other_term_itr.GetKey(), /*utf8=*/true);
    if (prefix_len <= 0) {
      continue;
    }
    prefix.assign(other_term_itr.GetKey(), prefix_len);

    // Figure out backfill tvi. Might not exist since all children terms could
    // only contain hits from non-prefix sections.
    //
    // Ex. Suppose that the main lexicon contains "foot" and "fool" and that
    // we're adding "foul". The new branching prefix will be "fo". The backfill
    // prefix will be "foo" - all hits in prefix section on "foo" will need to
    // be added to the new "fo" posting list later.
    FindTermResult result =
        FindShortestValidTermWithPrefixHits(main_lexicon_.get(), prefix);
    if (!result.found || result.exact) {
      continue;
    }

    // This is a new prefix that will need backfilling from its next-in-line
    // posting list. This new prefix will have to have a posting list eventually
    // so insert a default PostingListIdentifier as a placeholder.
    uint32_t branching_prefix_tvi;
    bool new_key;
    PostingListIdentifier posting_list_id = PostingListIdentifier::kInvalid;
    if (!main_lexicon_->Insert(prefix.c_str(), &posting_list_id,
                               &branching_prefix_tvi, false, &new_key)) {
      return absl_ports::InternalError("Could not insert branching prefix");
    }

    // Backfills only contain prefix hits by default. So set these here but
    // could be overridden when adding hits from the other index later.
    if (!main_lexicon_->SetProperty(branching_prefix_tvi,
                                    GetHasNoExactHitsPropertyId()) ||
        !main_lexicon_->SetProperty(branching_prefix_tvi,
                                    GetHasHitsInPrefixSectionPropertyId())) {
      return absl_ports::InternalError("Setting prefix prop failed");
    }

    outputs.backfill_map[branching_prefix_tvi] = result.tvi;
  }
  return outputs;
}

libtextclassifier3::StatusOr<MainIndex::LexiconMergeOutputs>
MainIndex::AddTerms(const IcingDynamicTrie& other_lexicon,
                    LexiconMergeOutputs&& outputs) {
  IcingDynamicTrie::PropertyReadersAll new_term_prop_readers(other_lexicon);
  for (IcingDynamicTrie::Iterator other_term_itr(other_lexicon, "");
       other_term_itr.IsValid(); other_term_itr.Advance()) {
    uint32_t new_main_tvi;
    PostingListIdentifier posting_list_id = PostingListIdentifier::kInvalid;
    if (!main_lexicon_->Insert(other_term_itr.GetKey(), &posting_list_id,
                               &new_main_tvi,
                               /*replace=*/false)) {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Could not insert term: ", other_term_itr.GetKey()));
    }

    // Copy the properties from the other lexicon over to the main lexicon.
    uint32_t other_tvi = other_term_itr.GetValueIndex();
    if (!CopyProperties(new_term_prop_readers, other_lexicon, other_tvi,
                        new_main_tvi)) {
      return absl_ports::InternalError("Could not insert term");
    }

    // Add other to main mapping.
    outputs.other_tvi_to_main_tvi.emplace(other_tvi, new_main_tvi);
  }
  return outputs;
}

libtextclassifier3::StatusOr<MainIndex::LexiconMergeOutputs>
MainIndex::AddBranchPoints(const IcingDynamicTrie& other_lexicon,
                           LexiconMergeOutputs&& outputs) {
  IcingDynamicTrie::PropertyReader has_prefix_prop_reader(
      other_lexicon, GetHasHitsInPrefixSectionPropertyId());
  if (!has_prefix_prop_reader.Exists()) {
    return outputs;
  }
  std::string prefix;
  for (IcingDynamicTrie::Iterator other_term_itr(other_lexicon, "");
       other_term_itr.IsValid(); other_term_itr.Advance()) {
    // Only expand terms that have hits in prefix sections.
    if (!has_prefix_prop_reader.HasProperty(other_term_itr.GetValueIndex())) {
      continue;
    }

    // Get prefixes where there is already a branching point in the main
    // lexicon. We skip prefixes which don't already have a branching point.
    std::vector<int> prefix_lengths = main_lexicon_->FindBranchingPrefixLengths(
        other_term_itr.GetKey(), /*utf8=*/true);

    int buf_start = outputs.prefix_tvis_buf.size();
    // Add prefixes.
    for (int prefix_length : prefix_lengths) {
      if (prefix_length <= 0) {
        continue;
      }

      prefix.assign(other_term_itr.GetKey(), prefix_length);
      uint32_t prefix_tvi;
      bool new_key;
      PostingListIdentifier posting_list_identifier =
          PostingListIdentifier::kInvalid;
      if (!main_lexicon_->Insert(prefix.c_str(), &posting_list_identifier,
                                 &prefix_tvi, /*replace=*/false, &new_key)) {
        return absl_ports::InternalError("Could not insert prefix");
      }

      // Prefix tvi will have hits in prefix section.
      if (!main_lexicon_->SetProperty(prefix_tvi,
                                      GetHasHitsInPrefixSectionPropertyId())) {
        return absl_ports::InternalError(
            "Setting has hits in prefix section prop failed");
      }

      // If it hasn't been added by non-prefix term insertions in
      // AddBackfillBranchPoints and AddTerms, it is a prefix-only term.
      if (new_key && !main_lexicon_->SetProperty(
                         prefix_tvi, GetHasNoExactHitsPropertyId())) {
        return absl_ports::InternalError("Setting no exact hits prop failed");
      }

      outputs.prefix_tvis_buf.push_back(prefix_tvi);
    }

    // Any prefixes added? Then add to map.
    if (buf_start < outputs.prefix_tvis_buf.size()) {
      outputs.other_tvi_to_prefix_main_tvis[other_term_itr.GetValueIndex()] = {
          buf_start, outputs.prefix_tvis_buf.size() - buf_start};
    }
  }
  return outputs;
}

bool MainIndex::CopyProperties(
    const IcingDynamicTrie::PropertyReadersAll& prop_reader,
    const IcingDynamicTrie& other_lexicon, uint32_t other_tvi,
    uint32_t new_main_tvi) {
  for (uint32_t property_id = 0; property_id < prop_reader.size();
       ++property_id) {
    if (property_id == GetHasNoExactHitsPropertyId()) {
      // HasNoExactHitsProperty is an inverse. If other_lexicon has exact hits
      // for this term, then HasNoExactHits needs to be set to false in
      // main_lexicon. If other_lexicon has no exact hits for this term, then
      // HasNoExactHits in the main_lexicon should not be modified.
      if (!prop_reader.HasProperty(property_id, other_tvi) &&
          !main_lexicon_->ClearProperty(new_main_tvi, property_id)) {
        LOG(ERROR) << "Clearing prefix prop failed";
        return false;
      }
    } else {
      // If other_lexicon has this property set for this term, then that
      // property needs to be set for the main_lexicon. If other_lexicon
      // doesn't have this property set, then
      if (prop_reader.HasProperty(property_id, other_tvi) &&
          !main_lexicon_->SetProperty(new_main_tvi, property_id)) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace lib
}  // namespace icing
