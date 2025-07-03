#ifndef ISOLATED_STORAGE_SERVICE_MACROS_H_
#define ISOLATED_STORAGE_SERVICE_MACROS_H_

namespace android::isolated_storage_service {

// Accepts a `std::vector<uint8_t>` and a `proto2::MessageLite&` and
// deserializes the bytes into the proto. Returns a bad AStatus if the
// deserialization fails.
#define DESERIALIZE_OR_RETURN(bytes_in, proto_out)                      \
  static_assert(std::is_lvalue_reference<decltype((bytes_in))>::value,  \
                "bytes_in must be an lvalue");                          \
  static_assert(std::is_lvalue_reference<decltype((proto_out))>::value, \
                "proto_out must be an lvalue");                         \
  if (!proto_out.ParseFromArray(bytes_in.data(), bytes_in.size())) {    \
    return ndk::ScopedAStatus::fromExceptionCodeWithMessage(            \
        EX_ILLEGAL_ARGUMENT, "Failed to deserialize proto");            \
  }

// Accepts a `proto2::Message` and a `std::optional<std::vector<uint8_t>>*` and
// serializes the proto as bytes into the vector. Returns a bad AStatus if the
// serialization fails.
#define SERIALIZE_AND_RETURN_ASTATUS(proto_in, bytes_out)               \
  static_assert(std::is_lvalue_reference<decltype((proto_in))>::value,  \
                "proto_in must be an lvalue");                          \
  static_assert(std::is_lvalue_reference<decltype((bytes_out))>::value, \
                "bytes_out must be an lvalue");                         \
  bytes_out->emplace(proto_in.ByteSizeLong());                          \
  if (proto_in.SerializeToArray(bytes_out->value().data(),              \
                                bytes_out->value().size())) {           \
    return ndk::ScopedAStatus::ok();                                    \
  } else {                                                              \
    return ndk::ScopedAStatus::fromExceptionCodeWithMessage(            \
        EX_ILLEGAL_ARGUMENT, "Failed to serialize proto");              \
  }

// Accepts a `std::unique_ptr<icing::lib::IcingSearchEngine>` and returns a bad
// AStatus if the pointer is null.
#define CHECK_ICING_INIT(icing)                                   \
  if (!icing) {                                                   \
    return ndk::ScopedAStatus::fromExceptionCodeWithMessage(      \
        EX_ILLEGAL_STATE, "Icing connection is not initialized"); \
  }

}  // namespace android::isolated_storage_service

#endif  // ISOLATED_STORAGE_SERVICE_MACROS_H_
