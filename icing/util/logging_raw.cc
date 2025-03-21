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

#include "icing/util/logging_raw.h"

#include <cstdio>
#include <string>

#include "icing/proto/debug.pb.h"

// NOTE: this file contains two implementations: one for Android, one for all
// other cases.  We always build exactly one implementation.
#if defined(__ANDROID__)

// Compiled as part of Android.
#include <android/log.h>

namespace icing {
namespace lib {

namespace {
// Converts LogSeverity to level for __android_log_write.
int GetAndroidLogLevel(LogSeverity::Code severity) {
  switch (severity) {
    case LogSeverity::VERBOSE:
      return ANDROID_LOG_VERBOSE;
    case LogSeverity::DBG:
      return ANDROID_LOG_DEBUG;
    case LogSeverity::INFO:
      return ANDROID_LOG_INFO;
    case LogSeverity::WARNING:
      return ANDROID_LOG_WARN;
    case LogSeverity::ERROR:
      return ANDROID_LOG_ERROR;
    case LogSeverity::FATAL:
      return ANDROID_LOG_FATAL;
  }
}
}  // namespace

void LowLevelLogging(LogSeverity::Code severity, const std::string& tag,
                     const std::string& message, const bool force_debug_logs) {
  const int android_log_level = GetAndroidLogLevel(severity);
#if __ANDROID_API__ >= 30
  if (!__android_log_is_loggable(android_log_level, tag.c_str(),
                                 /*default_prio=*/ANDROID_LOG_INFO)) {
    return;
  }
#endif  // __ANDROID_API__ >= 30
  // TODO(b/401363381): Remove this once we have a better way to log to
  // /dev/hvc2 in isolated storage.
  if (force_debug_logs) {
    // When isolated icing storage is enabled, the VM debug level determines
    // whether icing debug logs are delivered. We want the icing debug logs
    // to always be present. Thus force logging to /dev/hvc2.
    const char* file_logger_path = "/dev/hvc2";
    // "e" opens the file with the O_CLOEXEC flag. Icing should not be starting
    // any other processes, but it is added as a precaution.
    static FILE* stream = [&file_logger_path]() {
      FILE* f = fopen(file_logger_path, "ae");
      if (f != nullptr) {
        return f;
      }
      fprintf(stderr, "Failed to open /dev/hvc2 for logging. "
                      "Falling back to stderr.\n");
      return stderr;
    }();
    fprintf(stream, "%s: %s\n", tag.c_str(), message.c_str());
  } else {
    __android_log_write(android_log_level, tag.c_str(), message.c_str());
  }
}

}  // namespace lib
}  // namespace icing

#else  // if defined(__ANDROID__)

// Not on Android: implement LowLevelLogging to print to stderr (see below).
namespace icing {
namespace lib {

namespace {
// Converts LogSeverity to human-readable text.
const char *LogSeverityToString(LogSeverity::Code severity) {
  switch (severity) {
    case LogSeverity::VERBOSE:
      return "VERBOSE";
    case LogSeverity::DBG:
      return "DEBUG";
    case LogSeverity::INFO:
      return "INFO";
    case LogSeverity::WARNING:
      return "WARNING";
    case LogSeverity::ERROR:
      return "ERROR";
    case LogSeverity::FATAL:
      return "FATAL";
  }
}
}  // namespace

void LowLevelLogging(LogSeverity::Code severity, const std::string &tag,
                     const std::string &message, const bool force_debug_logs) {
  // TODO(b/146903474) Do not log to stderr for logs other than FATAL and ERROR.
  fprintf(stderr, "[%s] %s : %s\n", LogSeverityToString(severity), tag.c_str(),
          message.c_str());
  fflush(stderr);
}

}  // namespace lib
}  // namespace icing

#endif  // if defined(__ANDROID__)
