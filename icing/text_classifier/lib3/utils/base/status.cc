#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"

#include <string>
#include <utility>

namespace libtextclassifier3 {

const Status& Status::OK = *new Status(StatusCode::OK, "");
const Status& Status::UNKNOWN = *new Status(StatusCode::UNKNOWN, "");

Status::Status() : code_(StatusCode::OK) {}
Status::Status(StatusCode error, std::string message)
    : code_(error), message_(std::move(message)) {}

logging::LoggingStringStream& operator<<(logging::LoggingStringStream& stream,
                                         const Status& status) {
  stream << status.error_code();
  return stream;
}

}  // namespace libtextclassifier3
