// Copyright (C) 2024 Google LLC
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

#ifndef ICING_FEATURE_FLAGS_H_
#define ICING_FEATURE_FLAGS_H_

namespace icing {
namespace lib {

class FeatureFlags {
 public:
  explicit FeatureFlags(bool enable_scorable_properties,
                        bool enable_embedding_quantization)
      : enable_scorable_properties_(enable_scorable_properties),
        enable_embedding_quantization_(enable_embedding_quantization) {}

  bool enable_scorable_properties() const {
    return enable_scorable_properties_;
  }

  bool enable_embedding_quantization() const {
    return enable_embedding_quantization_;
  }

 private:
  bool enable_scorable_properties_;

  // Whether to enable quantization for embedding vectors. If false, all
  // embedding vectors will not be quantized. Otherwise, quantization will be
  // controlled by the quantization type specified in the schema.
  bool enable_embedding_quantization_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FEATURE_FLAGS_H_
