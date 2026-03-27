#ifndef STD_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define internal static
#define global static

#define UNUSED(v) (v == v)

#define PACKED __attribute__((__packed__))

#define STATIC_ASSERT(C) _Static_assert(C, "")

#define KIBIBYTES(v) ((v) * 1024)
#define MEBIBYTES(v) (1024 * KIBIBYTES(v))

#define ARRAY_LENGTH(a) (sizeof(a) / sizeof(*(a)))

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define DECIMAL_BASE 10

typedef int32_t bool32;
const bool32 false = 0;
const bool32 true = 1;

typedef uint8_t byte;

typedef struct
{
  size_t length;
  const void *data;
} ByteSlice;

internal void assert(bool32 condition)
{
  if (!condition)
  {
    // Write to null to crash the program
    volatile int8_t *v = NULL;
    *v = 0;
  }
}

typedef enum
{
  ALLOCATE_OK,
  ALLOCATE_OUT_OF_MEMORY,
} AllocateError;

internal AllocateError allocate(void **memory, size_t length)
{
  assert(memory != NULL);
  assert(*memory == NULL);

  void *ptr = malloc(length);
  if (ptr == NULL)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  *memory = ptr;
  return ALLOCATE_OK;
}

internal void deallocate(void *memory, size_t length)
{
  UNUSED(length);
  if (memory == NULL)
  {
    return;
  }
  free(memory);
}

// TODO: Optimise
// TODO Try using a macro to cover types
internal void
memory_copy_forward(void *destination, const void *source, size_t length)
{
  assert(destination != NULL);
  assert(source != NULL || length == 0);

  for (size_t i = 0; i < length; ++i)
  {
    uint8_t *d = &destination[i];
    const uint8_t *s = &source[i];
    *d = *s;
  }
}

// TODO: Optimise
// TODO Try using a macro to cover types
internal void
memory_copy_backward(void *destination, const void *source, size_t length)
{
  assert(destination != NULL);
  assert(source != NULL || length == 0);

  for (int64_t i = 0; i < length; ++i)
  {
    uint8_t *d = destination - i;
    const uint8_t *s = source - i;
    *d = *s;
  }
}

// TODO: Optimise
// TODO Try using a macro to cover types
internal bool32 memory_compare(const void *a, const void *b, size_t length)
{
  assert(a != NULL);
  assert(b != NULL);

  for (size_t i = 0; i < length; ++i)
  {
    const uint8_t *av = a + i;
    const uint8_t *bv = b + i;
    if (*av != *bv)
    {
      return false;
    }
  }

  return true;
}

// TODO: Use unsigned char instead of void
internal AllocateError reallocate_update_length(
    size_t object_size, void **memory, size_t *length, size_t new_length)
{
  assert(object_size > 0);
  assert(memory != NULL);
  assert(length != NULL);

  void *new_memory = NULL;

  if (allocate(&new_memory, object_size * new_length) == ALLOCATE_OUT_OF_MEMORY)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  if (*memory != NULL)
  {
    memory_copy_forward(
        new_memory, *memory, object_size * MIN(*length, new_length));

    deallocate(*memory, object_size * *length);
  }

  *memory = new_memory;
  *length = new_length;
  return ALLOCATE_OK;
}

internal AllocateError
reallocate(size_t object_size, void **memory, size_t length, size_t new_length)
{
  return reallocate_update_length(object_size, memory, &length, new_length);
}

typedef enum
{
  CMP_SMALLER,
  CMP_EQUAL,
  CMP_GREATER,
} CompareRelation;

// A string, unlike StringSlice, this owns the memory
typedef struct
{
  size_t length;
  char *data;
} String;

// A slice to string, NOT null terminated
typedef struct
{
  size_t length;
  const char *data;
} StringSlice;

typedef enum
{
  STRING_TO_INTEGER_OK,
  STRING_TO_INTEGER_TOO_LARGE,
  STRING_TO_INTEGER_INVALID_CHARACTER,
} StringToIntegerError;

typedef struct
{
  uint32_t integer;
  StringToIntegerError error;
} UInt32FromStringSliceResult;

internal UInt32FromStringSliceResult
uint32_from_string_slice(StringSlice string)
{
  uint32_t integer = 0;

  for (size_t i = 0; i < string.length; ++i)
  {
    switch (string.data[i])
    {
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
    {
      uint32_t delta = string.data[i] - '0';
      if (integer > (UINT32_MAX / DECIMAL_BASE)
          || delta > (UINT32_MAX / DECIMAL_BASE - integer))
      {
        return (UInt32FromStringSliceResult){
            .error = STRING_TO_INTEGER_TOO_LARGE,
        };
      }

      integer = (integer * DECIMAL_BASE) + string.data[i] - '0';
    }
    break;

    default:
      return (UInt32FromStringSliceResult){
          .error = STRING_TO_INTEGER_INVALID_CHARACTER,
      };
    }
  }

  return (UInt32FromStringSliceResult){
      .integer = integer,
      .error = STRING_TO_INTEGER_OK,
  };
}

size_t append_uint64_as_string(uint64_t number, char *string, size_t length)
{
  size_t divisor = 1;
  for (; number / divisor >= DECIMAL_BASE; divisor *= DECIMAL_BASE) {}

  size_t i = 0;
  for (i = 0; divisor > 0 && i < length; ++i, divisor /= DECIMAL_BASE)
  {
    uint64_t remainder = number / divisor;
    assert(remainder < DECIMAL_BASE);

    number -= divisor * remainder;

    string[i] = '0' + remainder;
  }

  return i;
}

internal AllocateError
string_from_string_slice(StringSlice slice, String *string)
{
  char *data = NULL;
  if (allocate((void **)&data, slice.length) != ALLOCATE_OK)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  memory_copy_forward(data, slice.data, slice.length);

  *string = (String){
      .data = data,
      .length = slice.length,
  };

  return ALLOCATE_OK;
}

internal void string_destroy(String *string)
{
  assert(string != NULL);
  assert(string->length != -1);
  deallocate(string->data, string->length);
  string->data = NULL;
  string->length = -1;
}

internal AllocateError string_append(String *string, StringSlice slice)
{
  size_t old_length = string->length;
  if (reallocate_update_length(
          sizeof(*string->data),
          (void **)&string->data,
          &string->length,
          string->length + slice.length))
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  memory_copy_forward(&string->data[old_length], slice.data, slice.length);

  return ALLOCATE_OK;
}

internal StringSlice string_slice_from_string(String string)
{
  return (StringSlice){
      .data = string.data,
      .length = string.length,
  };
}

internal StringSlice string_slice_from_ptr(const char *ptr)
{
  assert(ptr != NULL);

  const char *end = ptr;
  for (; *end != '\0'; ++end) {}

  return (StringSlice){
      .data = ptr,
      .length = (size_t)(end - ptr),
  };
}

internal bool32 string_slice_prefix_eq(StringSlice string, StringSlice prefix)
{
  if (string.length < prefix.length)
  {
    return false;
  }

  size_t i = 0;
  for (; i < prefix.length && string.data[i] == prefix.data[i]; ++i) {}

  return i == prefix.length;
}

typedef struct
{
  bool32 found;
  StringSlice before;
  StringSlice after;
} StringSliceSplitResult;

internal StringSliceSplitResult
string_slice_split(StringSlice string, StringSlice delimiter)
{
  size_t i = 0;
  for (; i < string.length; ++i)
  {
    if (string_slice_prefix_eq(
            (StringSlice){
                .data = string.data + i,
                .length = string.length - 1,
            },
            delimiter))
    {
      break;
    }
  }

  size_t offset_to_after = MIN(i + delimiter.length, string.length);

  return (StringSliceSplitResult){
      .found = i != string.length,
      .before =
          {
              .data = string.data,
              .length = i,
          },
      .after =
          {
              .data = string.data + offset_to_after,
              .length = string.length - offset_to_after,
          },
  };
}

// Returns a StringSlice past the specified character, if the character is not
// found StringSlice.data == NULL
internal StringSlice string_slice_find_past(StringSlice string, char character)
{
  size_t i = 0;
  for (; i < string.length && string.data[i] != character; ++i) {}
  i += 1;

  return (StringSlice){
      .data = i == string.length ? NULL : string.data + i,
      .length = string.length - i,
  };
}

internal bool32 string_slice_eq(StringSlice a, StringSlice b)
{
  if (a.length != b.length)
  {
    return false;
  }

  size_t i = 0;
  for (; i < a.length && a.data[i] == b.data[i]; ++i) {}

  return i == a.length;
}

internal size_t string_slice_concat(
    char *into,
    size_t index,
    size_t length,
    StringSlice a,
    bool32 insert_null_terminator)
{
  const size_t last_index = index + a.length + (insert_null_terminator ? 1 : 0);
  assert(last_index < length);

  memory_copy_forward(into + index, a.data, a.length);

  if (insert_null_terminator)
  {
    into[index + a.length] = '\0';
  }

  return last_index;
}

#define STD_H
#endif
