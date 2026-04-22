// Copyright (C) 2023 Google LLC
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

#ifndef ICING_MONKEY_TEST_MONKEY_TEST_UTIL_H_
#define ICING_MONKEY_TEST_MONKEY_TEST_UTIL_H_

#include <cstdint>
#include <functional>
#include <random>
#include <utility>
#include <vector>

namespace icing {
namespace lib {

using MonkeyTestRandomEngine = std::mt19937;

class IcingMonkeyTestRunner;

struct IcingMonkeyTestRunnerConfiguration {
  explicit IcingMonkeyTestRunnerConfiguration(uint32_t seed, int num_types,
                                              int num_namespaces, int num_uris,
                                              int index_merge_size,
                                              bool initialize_by_existing_data)
      : seed(seed),
        num_types(num_types),
        num_namespaces(num_namespaces),
        num_uris(num_uris),
        index_merge_size(index_merge_size),
        initialize_by_existing_data(initialize_by_existing_data) {}

  uint32_t seed;
  int num_types;
  int num_namespaces;
  int num_uris;
  int index_merge_size;

  // Whether to initialize Icing with existing data. If true, the test will
  // start from the state of the existing Icing testing data stored in
  // GetTestTempDir() + "/icing/monkey". Otherwise, the test will start with an
  // empty Icing.
  bool initialize_by_existing_data;

  // To ensure that the random schema is generated with the best quality, the
  // number of properties for each type will only be randomly picked from this
  // list, instead of picking it from a range. For example, a vector of
  // [1, 2, 3, 4] means each generated types have a 25% chance of getting 1
  // property, 2 properties, 3 properties and 4 properties.
  std::vector<int> possible_num_properties;

  // The possible number of tokens that may appear in a string property of
  // generated documents, with a noise factor from 0.5 to 1 applied.
  // - This number is also used for the number of qualified ids in join
  //   properties if the string property is qualified id joinable.
  // - However, unlike normal string tokens which will be concatenated together
  //   (separated by spaces) into a single string, qualified ids will be stored
  //   as multiple strings in the document.
  // - Also if the cardinality of the joinable property is optional or required,
  //   then only 1 qualified id will be generated. In this case,
  //   possible_num_tokens is ignored.
  std::vector<int> possible_num_tokens;

  // The possible number of embedding vectors that may appear in a repeated
  // vector property of generated documents.
  std::vector<int> possible_num_vectors;

  // The possible dimensions for the randomly generated embedding vectors.
  std::vector<int> possible_vector_dimensions;

  // The possible random spaces for generating qualified ids for join
  // properties. When generating a qualified id:
  // - Pick a random space from this list.
  // - Then generate a namespace from [namespace_l, namespace_r) and a uri from
  //   [uri_l, uri_r).
  //
  // This will ensure that a decent join ratio can be achieved.
  //
  // Note: join monkey tests should have at least one of these spaces defined.
  struct QualifiedIdRandomSpace {
    int namespace_l;
    int namespace_r;
    int uri_l;
    int uri_r;
  };
  std::vector<QualifiedIdRandomSpace> possible_ref_qualified_id_random_spaces;

  // An array of pairs of monkey test APIs with frequencies.
  // If f_sum is the sum of all the frequencies, an operation with frequency f
  // means for every f_sum iterations, the operation is expected to run f times.
  std::vector<std::pair<std::function<void(IcingMonkeyTestRunner*)>, uint32_t>>
      monkey_api_schedules;

  bool IsJoinEnabled() const {
    return !possible_ref_qualified_id_random_spaces.empty();
  }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_MONKEY_TEST_UTIL_H_
