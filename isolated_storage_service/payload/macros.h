#ifndef ISOLATED_STORAGE_SERVICE_MACROS_H_
#define ISOLATED_STORAGE_SERVICE_MACROS_H_

namespace android::isolated_storage_service {

// Accepts a `std::vector<uint8_t>` and a `google::protobuf::MessageLite&` and
// deserializes the bytes into the proto. Returns a bad AStatus if the
// deserialization fails.
#define DESERIALIZE_OR_RETURN(bytes_in, proto_out)                   \
  if (!proto_out.ParseFromArray(bytes_in.data(), bytes_in.size())) { \
    return ndk::ScopedAStatus::fromExceptionCodeWithMessage(         \
        EX_ILLEGAL_ARGUMENT, "Failed to deserialize proto");         \
  }

// Accepts a `google::protobuf::Message` and a
// `std::optional<std::vector<uint8_t>>*` and serializes the proto as bytes into
// the vector. Returns a bad AStatus if the serialization fails.
#define SERIALIZE_AND_RETURN_ASTATUS(proto_in, bytes_out)     \
  *bytes_out = std::vector<uint8_t>(proto_in.ByteSizeLong()); \
  if (proto_in.SerializeToArray(bytes_out->value().data(),    \
                                bytes_out->value().size())) { \
    return ndk::ScopedAStatus::ok();                          \
  } else {                                                    \
    return ndk::ScopedAStatus::fromExceptionCodeWithMessage(  \
        EX_ILLEGAL_ARGUMENT, "Failed to serialize proto");    \
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
