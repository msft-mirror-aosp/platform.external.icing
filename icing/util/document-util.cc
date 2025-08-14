// Copyright (C) 2025 Google LLC
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

#include "third_party/icing/util/document-util.h"

#include <utility>

#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/proto/document_wrapper.proto.h"

namespace icing {
namespace lib {

namespace document_util {

DocumentWrapper CreateDocumentWrapper(DocumentProto document) {
  DocumentWrapper document_wrapper;
  *document_wrapper.mutable_document() = std::move(document);
  return document_wrapper;
}

}  // namespace document_util

}  // namespace lib
}  // namespace icing
