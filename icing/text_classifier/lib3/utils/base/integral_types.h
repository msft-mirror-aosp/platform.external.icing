// Basic integer type definitions.

#ifndef KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_INTEGRAL_TYPES_H_
#define KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_INTEGRAL_TYPES_H_


namespace libtextclassifier3 {

typedef unsigned int uint;
typedef unsigned int uint32;
typedef unsigned long long uint64;

#ifndef SWIG
typedef int int32;
typedef unsigned char uint8;    // NOLINT
typedef signed char int8;       // NOLINT
typedef unsigned short uint16;  // NOLINT
typedef signed short int16;     // NOLINT

// A type to represent a Unicode code-point value. As of Unicode 4.0,
// such values require up to 21 bits.
// (For type-checking on pointers, make this explicitly signed,
// and it should always be the signed version of whatever int32 is.)
typedef signed int char32;
#endif  // SWIG

#ifdef COMPILER_MSVC
typedef __int64 int64;
#else
typedef long long int64;  // NOLINT
#endif  // COMPILER_MSVC

// Some compile-time assertions that our new types have the intended size.
static_assert(sizeof(int) == 4, "Our typedefs depend on int being 32 bits");
static_assert(sizeof(uint32) == 4, "wrong size");
static_assert(sizeof(int32) == 4, "wrong size");
static_assert(sizeof(uint8) == 1, "wrong size");
static_assert(sizeof(int8) == 1, "wrong size");
static_assert(sizeof(uint16) == 2, "wrong size");
static_assert(sizeof(int16) == 2, "wrong size");
static_assert(sizeof(char32) == 4, "wrong size");
static_assert(sizeof(int64) == 8, "wrong size");

// There are still some requirements that we build these headers in
// C-compatibility mode. Unfortunately, -Wall doesn't like c-style
// casts, and C doesn't know how to read braced-initialization for
// integers.
#if defined(__cplusplus)
const uint16 kuint16max{0xFFFF};
const int16 kint16max{0x7FFF};
const int16 kint16min{~0x7FFF};
const uint32 kuint32max{0xFFFFFFFF};
const int32 kint32max{0x7FFFFFFF};
#else   // not __cplusplus, this branch exists only for C-compat
static const uint16 kuint16max = ((uint16)0xFFFF);
static const int16 kint16min = ((int16)~0x7FFF);
static const int16 kint16max = ((int16)0x7FFF);
static const uint32 kuint32max = ((uint32)0xFFFFFFFF);
static const int32 kint32max = ((int32)0x7FFFFFFF);
#endif  // __cplusplus

}  // namespace libtextclassifier3

#endif  // KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_INTEGRAL_TYPES_H_
