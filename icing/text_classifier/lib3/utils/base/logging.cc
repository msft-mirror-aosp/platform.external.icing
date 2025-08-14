#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/logging.h"

#include <stdlib.h>

#include <exception>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/logging_raw.h"

namespace libtextclassifier3 {
namespace logging {

namespace {
// Returns pointer to beginning of last /-separated token from file_name.
// file_name should be a pointer to a zero-terminated array of chars.
// E.g., "foo/bar.cc" -> "bar.cc", "foo/" -> "", "foo" -> "foo".
const char *JumpToBasename(const char *file_name) {
  if (file_name == nullptr) {
    return nullptr;
  }

  // Points to the beginning of the last encountered token.
  const char *last_token_start = file_name;
  while (*file_name != '\0') {
    if (*file_name == '/') {
      // Found token separator.  A new (potentially empty) token starts after
      // this position.  Notice that if file_name is a valid zero-terminated
      // string, file_name + 1 is a valid pointer (there is at least one char
      // after address file_name, the zero terminator).
      last_token_start = file_name + 1;
    }
    file_name++;
  }
  return last_token_start;
}
}  // namespace

LogMessage::LogMessage(LogSeverity severity, const char *file_name,
                       int line_number)
    : severity_(severity) {
  stream_ << JumpToBasename(file_name) << ":" << line_number << ": ";
}

LogMessage::~LogMessage() {
  LowLevelLogging(severity_, /* tag = */ "txtClsf", stream_.message);
  if (severity_ == FATAL) {
    std::terminate();  // Will print a stacktrace (stdout or logcat).
  }
}

}  // namespace logging
}  // namespace libtextclassifier3
