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

#ifndef ICING_TOOLS_DOCUMENT_STORE_DUMP_H_
#define ICING_TOOLS_DOCUMENT_STORE_DUMP_H_

#include <string>

#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/document-store.h"

namespace icing {
namespace lib {

// Utility function for dumping the complete document store content.
// This provides a human-readable representation of the document store, mainly
// provided for easier understandability for developers.
// The output of this class should only be available on cmdline-tool-level
// (with root access), or unit tests. In other words it should not be possible
// to trigger this on a release key device, for data protection reasons.
std::string GetDocumentStoreDump(const DocumentStore& document_store);

}  // namespace lib
}  // namespace icing
#endif  // ICING_TOOLS_DOCUMENT_STORE_DUMP_H_
