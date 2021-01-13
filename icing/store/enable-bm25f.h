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

#ifndef ICING_STORE_ENABLE_BM25F_H_
#define ICING_STORE_ENABLE_BM25F_H_

namespace icing {
namespace lib {

inline bool enable_bm25f_ = false;

inline bool enableBm25f() { return enable_bm25f_; }

// Setter for testing purposes. It should never be called in production code.
inline void setEnableBm25f(bool enable_bm25f) { enable_bm25f_ = enable_bm25f; }

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_ENABLE_BM25F_H_
