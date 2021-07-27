// Copyright (C) 2020 Google LLC
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

#ifndef ICING_STORE_CORPUS_ID_H_
#define ICING_STORE_CORPUS_ID_H_

#include <cstdint>

namespace icing {
namespace lib {

// Identifier for corpus, i.e. a <namespace, schema_type> pair>, in
// DocumentProto. Generated in DocumentStore.
using CorpusId = int32_t;

inline constexpr CorpusId kInvalidCorpusId = -1;

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_CORPUS_ID_H_
