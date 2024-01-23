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

#ifndef ICING_INDEX_NUMERIC_NUMERIC_INDEX_H_
#define ICING_INDEX_NUMERIC_NUMERIC_INDEX_H_

#include <memory>
#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

template <typename T>
class NumericIndex {
 public:
  using value_type = T;

  // Editor class for batch adding new records into numeric index for a given
  // property, DocumentId and SectionId. The caller should use BufferKey to
  // buffer a key (calls several times for multiple keys) and finally call
  // IndexAllBufferedKeys to batch add all buffered keys (with DocumentId +
  // SectionId info, i.e. BasicHit) into numeric index.
  //
  // For example, there are values = [5, 1, 10, -100] in DocumentId = 5,
  // SectionId = 1 (property "timestamp").
  // Then the client should call BufferKey(5), BufferKey(1), BufferKey(10),
  // BufferKey(-100) first, and finally call IndexAllBufferedKeys once to batch
  // add these records into numeric index.
  class Editor {
   public:
    explicit Editor(std::string_view property_name, DocumentId document_id,
                    SectionId section_id)
        : property_name_(property_name),
          document_id_(document_id),
          section_id_(section_id) {}

    virtual ~Editor() = default;

    // Buffers a new key.
    //
    // Returns:
    //   - OK on success
    //   - Any other errors, depending on the actual implementation
    virtual libtextclassifier3::Status BufferKey(T key) = 0;

    // Adds all buffered keys into numeric index.
    //
    // Returns:
    //   - OK on success
    //   - Any other errors, depending on the actual implementation
    virtual libtextclassifier3::Status IndexAllBufferedKeys() = 0;

   protected:
    std::string property_name_;
    DocumentId document_id_;
    SectionId section_id_;
  };

  // Iterator class for numeric index range query [key_lower, key_upper]
  // (inclusive for both side) on a given property (see GetIterator). There are
  // some basic requirements for implementation:
  // - Iterates through all relevant doc hits.
  // - Merges multiple SectionIds of doc hits with same DocumentId into a single
  //   SectionIdMask and constructs DocHitInfo.
  // - Returns DocHitInfo in descending DocumentId order.
  //
  // For example, relevant doc hits (DocumentId, SectionId) are [(2, 0), (4, 3),
  // (2, 1), (6, 2), (4, 2)]. Advance() and GetDocHitInfo() should return
  // DocHitInfo(6, SectionIdMask(2)), DocHitInfo(4, SectionIdMask(2, 3)) and
  // DocHitInfo(2, SectionIdMask(0, 1)).
  class Iterator {
   public:
    explicit Iterator(T key_lower, T key_upper)
        : key_lower_(key_lower), key_upper_(key_upper) {}

    virtual ~Iterator() = default;

    virtual libtextclassifier3::Status Advance() = 0;

    virtual DocHitInfo GetDocHitInfo() const = 0;

   protected:
    T key_lower_;
    T key_upper_;
  };

  virtual ~NumericIndex() = default;

  // Returns an Editor instance for adding new records into numeric index for a
  // given property, DocumentId and SectionId. See Editor for more details.
  virtual std::unique_ptr<Editor> Edit(std::string_view property_name,
                                       DocumentId document_id,
                                       SectionId section_id) = 0;

  // Returns a DocHitInfoIteratorNumeric (in DocHitInfoIterator interface type
  // format) for iterating through all docs which have the specified (numeric)
  // property contents in range [key_lower, key_upper].
  //
  // In general, different numeric index implementations require different data
  // iterator implementations, so class Iterator is an abstraction of the data
  // iterator and DocHitInfoIteratorNumeric can work with any implementation of
  // it. See Iterator and DocHitInfoIteratorNumeric for more details.
  //
  // Returns:
  //   - std::unique_ptr<DocHitInfoIterator> on success
  //   - NOT_FOUND_ERROR if there is no numeric index for property_name
  //   - INVALID_ARGUMENT_ERROR if key_lower > key_upper
  //   - Any other errors, depending on the actual implementation
  virtual libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
  GetIterator(std::string_view property_name, T key_lower,
              T key_upper) const = 0;

  // Clears all files created by the index. Returns OK if all files were
  // cleared.
  virtual libtextclassifier3::Status Reset() = 0;

  // Syncs all the data and metadata changes to disk.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on I/O errors
  virtual libtextclassifier3::Status PersistToDisk() = 0;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_NUMERIC_NUMERIC_INDEX_H_
