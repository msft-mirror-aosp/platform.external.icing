// Various portability macros, type definitions, and inline functions.

#ifndef KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_PORT_H_
#define KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_PORT_H_

namespace libtextclassifier3 {

#if defined(__GNUC__) && \
    (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 1))

// For functions we want to force inline.
// Introduced in gcc 3.1.
#define TC3_ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))

// For functions we don't want to inline, e.g., to keep code size small.
#define TC3_ATTRIBUTE_NOINLINE __attribute__((noinline))

#elif defined(_MSC_VER)
#define TC3_ATTRIBUTE_ALWAYS_INLINE __forceinline
#else

// Other compilers will have to figure it out for themselves.
#define TC3_ATTRIBUTE_ALWAYS_INLINE
#define TC3_ATTRIBUTE_NOINLINE
#endif

}  // namespace libtextclassifier3

#endif  // KNOWLEDGE_CEREBRA_SENSE_TEXT_CLASSIFIER_LIB3_UTILS_BASE_PORT_H_
