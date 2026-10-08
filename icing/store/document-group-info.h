// Copyright (C) 2026 Google LLC
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

#ifndef ICING_STORE_DOCUMENT_GROUP_INFO_H_
#define ICING_STORE_DOCUMENT_GROUP_INFO_H_

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "icing/proto/document.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// A structure that represents the key to group documents.
struct DocumentGroupKey {
  std::string schema_type_name;
  std::string name_space;

  bool operator==(const DocumentGroupKey& other) const {
    return schema_type_name == other.schema_type_name &&
           name_space == other.name_space;
  }

  struct Hasher {
    std::size_t operator()(const DocumentGroupKey& pair) const {
      return std::hash<std::string>()(pair.schema_type_name) ^
             std::hash<std::string>()(pair.name_space);
    }
  };
};

// A structure that represents a pair of document uri and document id.
struct DocumentUriIdPair {
  std::string uri;
  DocumentId document_id;
};

// A structure that represents the metadata of a document.
struct DocumentMetadata {
  std::string schema_type_name;
  std::string name_space;
  std::string uri;
  DocumentId document_id;
};

// A class that encapsulates a map for grouping documents by schema type name
// and namespace (DocumentGroupKey), along with a list of uri and document id
// (DocumentUriIdPair) for each document in the group.
//
// It is mostly used to return the information about deleted documents.
class DocumentGroupInfo {
 public:
  using MapType =
      std::unordered_map<DocumentGroupKey, std::vector<DocumentUriIdPair>,
                         DocumentGroupKey::Hasher>;

  // Adds a document to the map.
  void AddDocument(DocumentMetadata document_metadata);

  // Merges the given document group info map into this map.
  void Merge(DocumentGroupInfo&& other);

  // Returns the total number of documents in the map, which is the sum of the
  // number of documents in each group.
  int GetTotalNumDocs() const;

  // Returns all document ids in the map from all groups.
  std::unordered_set<DocumentId> GetAllDocumentIds() const;

  // Returns the document group info map as a proto.
  DocumentGroupInfoProto SerializeToProto() &&;

  // Returns the document group info map.
  const MapType& Get() const { return doc_group_info_map_; }

  bool empty() const { return doc_group_info_map_.empty(); }

  size_t size() const { return doc_group_info_map_.size(); }

 private:
  MapType doc_group_info_map_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_DOCUMENT_GROUP_INFO_H_
