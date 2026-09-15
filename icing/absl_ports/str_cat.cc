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

#include "icing/absl_ports/str_cat.h"

namespace icing {
namespace lib {
namespace absl_ports {

char* Append(char* out, std::string_view s) {
  if (!s.empty()) {
    memcpy(out, s.data(), s.length());
    out += s.length();
  }
  return out;
}

std::string StrCat(std::string_view a, std::string_view b) {
  std::string::size_type result_size = a.length() + b.length();
  std::string result;
  ResizeAndOverwrite(result, result_size,
                     [a, b](char* out, std::string::size_type n) {
    char* next = out;
    next = Append(next, a);
    next = Append(next, b);
    assert(next == out + n);
    return n;
  });
  return result;
}

std::string StrCat(std::string_view a, std::string_view b, std::string_view c) {
  std::string::size_type result_size = a.length() + b.length() + c.length();
  std::string result;
  ResizeAndOverwrite(result, result_size,
                     [a, b, c](char* out, std::string::size_type n) {
    char* next = out;
    next = Append(next, a);
    next = Append(next, b);
    next = Append(next, c);
    assert(next == out + n);
    return n;
  });
  return result;
}

std::string StrCat(std::string_view a, std::string_view b, std::string_view c,
                   std::string_view d) {
  std::string::size_type result_size =
      a.length() + b.length() + c.length() + d.length();
  std::string result;
  ResizeAndOverwrite(result, result_size,
                     [a, b, c, d](char* out, std::string::size_type n) {
    char* next = out;
    next = Append(next, a);
    next = Append(next, b);
    next = Append(next, c);
    next = Append(next, d);
    assert(next == out + n);
    return n;
  });
  return result;
}

std::string StrCatPieces(std::vector<std::string_view> pieces) {
  std::string::size_type result_size = 0;
  for (std::string_view s : pieces) {
    result_size += s.length();
  }
  std::string result;
  ResizeAndOverwrite(result, result_size,
                     [&pieces](char* out, std::string::size_type n) {
    char* next = out;
    for (std::string_view s : pieces) {
      next = Append(next, s);
    }
    assert(next == out + n);
    return n;
  });
  return result;
}

void StrAppend(std::string* dest, std::string_view a) {
  std::string::size_type old_size = dest->size();
  std::string::size_type new_size = old_size + a.length();
  ResizeAndOverwrite(*dest, new_size,
                     [old_size, a](char* out, std::string::size_type n) {
    char* next = out + old_size;
    next = Append(next, a);
    assert(next == out + n);
    return n;
  });
}

void StrAppend(std::string* dest, std::string_view a, std::string_view b) {
  std::string::size_type old_size = dest->size();
  std::string::size_type new_size = old_size + a.length() + b.length();
  ResizeAndOverwrite(*dest, new_size,
                     [old_size, a, b](char* out, std::string::size_type n) {
    char* next = out + old_size;
    next = Append(next, a);
    next = Append(next, b);
    assert(next == out + n);
    return n;
  });
}

void StrAppend(std::string* dest, std::string_view a, std::string_view b,
               std::string_view c) {
  std::string::size_type old_size = dest->size();
  std::string::size_type new_size =
      old_size + a.length() + b.length() + c.length();
  ResizeAndOverwrite(*dest, new_size,
                     [old_size, a, b, c](char* out, std::string::size_type n) {
    char* next = out + old_size;
    next = Append(next, a);
    next = Append(next, b);
    next = Append(next, c);
    assert(next == out + n);
    return n;
  });
}

void StrAppend(std::string* dest, std::string_view a, std::string_view b,
               std::string_view c, std::string_view d) {
  std::string::size_type old_size = dest->size();
  std::string::size_type new_size =
      old_size + a.length() + b.length() + c.length() + d.length();
  ResizeAndOverwrite(*dest, new_size, [old_size, a, b, c, d](char* out,
                                           std::string::size_type n) {
    char* next = out + old_size;
    next = Append(next, a);
    next = Append(next, b);
    next = Append(next, c);
    next = Append(next, d);
    assert(next == out + n);
    return n;
  });
}

void StrAppendPieces(std::string* dest, std::vector<std::string_view> pieces) {
  std::string::size_type old_size = dest->size();
  std::string::size_type total_size = old_size;
  for (std::string_view s : pieces) {
    total_size += s.length();
  }
  ResizeAndOverwrite(*dest, total_size,
                     [old_size, &pieces](char* out, std::string::size_type n) {
    char* next = out + old_size;
    for (std::string_view s : pieces) {
      next = Append(next, s);
    }
    assert(next == out + n);
    return n;
  });
}

}  // namespace absl_ports
}  // namespace lib
}  // namespace icing
