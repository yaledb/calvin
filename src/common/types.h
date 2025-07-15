// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// Defininitions of some common types and constants used in the system.

#ifndef _DB_COMMON_TYPES_H_
#define _DB_COMMON_TYPES_H_

#include <stdint.h>

#include <string>

using std::string;

// Abbreviated signed int types.
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

// Abbreviated unsigned int types.
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

// 'bytes' is an arbitrary sequence of bytes, represented as a string.
typedef string bytes;

// Convenience functions for converting between (signed) int and 'bytes' types.
static inline bytes PackInt8 (int8  x) { return bytes((const char*)&x, 1); }
static inline bytes PackInt16(int16 x) { return bytes((const char*)&x, 2); }
static inline bytes PackInt32(int32 x) { return bytes((const char*)&x, 4); }
static inline bytes PackInt64(int64 x) { return bytes((const char*)&x, 8); }
static inline int8  UnpackInt8 (bytes s) { return *((int8 *)(s.data())); }
static inline int16 UnpackInt16(bytes s) { return *((int16*)(s.data())); }
static inline int32 UnpackInt32(bytes s) { return *((int32*)(s.data())); }
static inline int64 UnpackInt64(bytes s) { return *((int64*)(s.data())); }

// Convenience functions for converting between unsigned int and 'bytes' types.
static inline bytes PackUInt8 (uint8  x) { return bytes((const char*)&x, 1); }
static inline bytes PackUInt16(uint16 x) { return bytes((const char*)&x, 2); }
static inline bytes PackUInt32(uint32 x) { return bytes((const char*)&x, 4); }
static inline bytes PackUInt64(uint64 x) { return bytes((const char*)&x, 8); }
static inline uint8  UnpackUInt8 (bytes s) { return *((uint8 *)(s.data())); }
static inline uint16 UnpackUInt16(bytes s) { return *((uint16*)(s.data())); }
static inline uint32 UnpackUInt32(bytes s) { return *((uint32*)(s.data())); }
static inline uint64 UnpackUInt64(bytes s) { return *((uint64*)(s.data())); }

// Key type for database objects.
// Note: if this changes from bytes, the types need to be updated for the
// following fields in .proto files:
//    proto/txn.proto:
//      TxnProto::'read_set'
//      TxnProto::'write_set'
typedef bytes Key;

// Value type for database objects.
typedef bytes Value;

#endif  // _DB_COMMON_TYPES_H_
