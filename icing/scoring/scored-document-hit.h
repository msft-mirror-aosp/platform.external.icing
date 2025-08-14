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

#ifndef ICING_SCORING_SCORED_DOCUMENT_HIT_H_
#define ICING_SCORING_SCORED_DOCUMENT_HIT_H_

#include <memory>
#include <utility>
#include <vector>

#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

class JoinedScoredDocumentHit;

// A data class containing information about the document, hit sections, and a
// score. The score is calculated against both the document and the hit
// sections.
class ScoredDocumentHit {
 public:
  class Converter {
   public:
    JoinedScoredDocumentHit operator()(
        ScoredDocumentHit&& scored_doc_hit) const;
  };

  ScoredDocumentHit(DocumentId document_id, SectionIdMask hit_section_id_mask,
                    double score)
      : document_id_(document_id),
        hit_section_id_mask_(hit_section_id_mask),
        score_(score) {}

  ScoredDocumentHit(DocumentId document_id, SectionIdMask hit_section_id_mask,
                    double score, std::vector<double> additional_scores)
      : document_id_(document_id),
        hit_section_id_mask_(hit_section_id_mask),
        score_(score) {
    SetAdditionalScores(std::move(additional_scores));
  }

  ScoredDocumentHit(const ScoredDocumentHit& other)
      : document_id_(other.document_id_),
        hit_section_id_mask_(other.hit_section_id_mask_),
        score_(other.score_) {
    if (other.additional_scores_ != nullptr) {
      SetAdditionalScores(*other.additional_scores_);
    }
  }

  ScoredDocumentHit& operator=(const ScoredDocumentHit& other) {
    document_id_ = other.document_id_;
    hit_section_id_mask_ = other.hit_section_id_mask_;
    score_ = other.score_;
    if (other.additional_scores_ != nullptr) {
      SetAdditionalScores(*other.additional_scores_);
    }
    return *this;
  }

  ScoredDocumentHit(ScoredDocumentHit&& other) { Swap(&other); }
  ScoredDocumentHit& operator=(ScoredDocumentHit&& other) {
    Swap(&other);
    return *this;
  }

  bool operator<(const ScoredDocumentHit& other) const {
    if (score() < other.score()) return true;
    if (score() > other.score()) return false;
    return document_id() < other.document_id();
  }

  DocumentId document_id() const { return document_id_; }

  SectionIdMask hit_section_id_mask() const { return hit_section_id_mask_; }

  double score() const { return score_; }

  // nullptr if no additional scores.
  const std::vector<double>* additional_scores() const {
    return additional_scores_.get();
  }

 private:
  void Swap(ScoredDocumentHit* other) {
    std::swap(document_id_, other->document_id_);
    std::swap(hit_section_id_mask_, other->hit_section_id_mask_);
    std::swap(score_, other->score_);
    std::swap(additional_scores_, other->additional_scores_);
  }

  void SetAdditionalScores(std::vector<double> additional_scores) {
    if (additional_scores.empty()) {
      additional_scores_.reset();
      return;
    }
    additional_scores_ =
        std::make_unique<std::vector<double>>(std::move(additional_scores));
  }

  DocumentId document_id_;
  SectionIdMask hit_section_id_mask_;
  double score_;
  // nullptr if no additional scores.
  std::unique_ptr<std::vector<double>> additional_scores_;
};

// A custom comparator for ScoredDocumentHit that determines which
// ScoredDocumentHit is better (should come first) based off of
// ScoredDocumentHit itself and the order of its score.
//
// Returns true if left is better than right according to score and order.
// Comparison is based off of score with ties broken by
// ScoredDocumentHit.document_id().
class ScoredDocumentHitComparator {
 public:
  explicit ScoredDocumentHitComparator(bool is_descending = true)
      : is_descending_(is_descending) {}

  bool operator()(const ScoredDocumentHit& lhs,
                  const ScoredDocumentHit& rhs) const {
    // STL comparator requirement: equal MUST return false.
    // If writing `return is_descending_ == !(lhs < rhs)`:
    // - When lhs == rhs, !(lhs < rhs) is true
    // - If is_descending_ is true, then we return true for equal case!
    if (is_descending_) {
      return rhs < lhs;
    }
    return lhs < rhs;
  }

 private:
  bool is_descending_;
};

// A data class containing information about a composite document after joining,
// including final score, parent ScoredDocumentHit, and a vector of all child
// ScoredDocumentHits. The final score is calculated by the strategy specified
// in join spec/rank strategy. It could be aggregated score, raw parent doc
// score, or anything else.
//
// ScoredDocumentHitsRanker may store ScoredDocumentHit or
// JoinedScoredDocumentHit.
// - We could've created a virtual class for them and ScoredDocumentHitsRanker
//   uses the abstract type.
// - However, Icing lib caches ScoredDocumentHitsRanker (which contains a list
//   of (Joined)ScoredDocumentHits) in ResultState. Inheriting the virtual class
//   makes both classes have additional 8 bytes for vtable, which increases 40%
//   and 15% memory usage respectively.
// - Also since JoinedScoredDocumentHit is a super-set of ScoredDocumentHit,
//   let's avoid the common virtual class and instead implement a convert
//   function (original type -> JoinedScoredDocumentHit) for each class, so
//   ScoredDocumentHitsRanker::PopNext can return a common type (i.e.
//   JoinedScoredDocumentHit).
class JoinedScoredDocumentHit {
 public:
  class Converter {
   public:
    JoinedScoredDocumentHit operator()(
        JoinedScoredDocumentHit&& scored_doc_hit) const {
      return scored_doc_hit;
    }
  };

  explicit JoinedScoredDocumentHit(
      double final_score, ScoredDocumentHit parent_scored_document_hit,
      std::vector<ScoredDocumentHit> child_scored_document_hits)
      : final_score_(final_score),
        parent_scored_document_hit_(std::move(parent_scored_document_hit)),
        child_scored_document_hits_(std::move(child_scored_document_hits)) {}

  bool operator<(const JoinedScoredDocumentHit& other) const {
    if (final_score_ != other.final_score_) {
      return final_score_ < other.final_score_;
    }
    return parent_scored_document_hit_ < other.parent_scored_document_hit_;
  }

  double final_score() const { return final_score_; }

  const ScoredDocumentHit& parent_scored_document_hit() const {
    return parent_scored_document_hit_;
  }

  const std::vector<ScoredDocumentHit>& child_scored_document_hits() const {
    return child_scored_document_hits_;
  }

 private:
  double final_score_;
  ScoredDocumentHit parent_scored_document_hit_;
  std::vector<ScoredDocumentHit> child_scored_document_hits_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_SCORED_DOCUMENT_HIT_H_
