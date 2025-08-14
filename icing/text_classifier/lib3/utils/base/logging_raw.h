#ifndef KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_LOGGING_RAW_H_
#define KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_LOGGING_RAW_H_

#include <string>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/logging_levels.h"

namespace libtextclassifier3 {
namespace logging {

// Low-level logging primitive.  Logs a message, with the indicated log
// severity.  From android/log.h: "the tag normally corresponds to the component
// that emits the log message, and should be reasonably small".
void LowLevelLogging(LogSeverity severity, const std::string &tag,
                     const std::string &message);

}  // namespace logging
}  // namespace libtextclassifier3

#endif  // KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_LOGGING_RAW_H_
