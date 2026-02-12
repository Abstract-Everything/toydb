#ifndef STD_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define UNUSED(v) (v == v)

#define KIBIBYTES(v) ((v) * 1024)

#define ARRAY_LENGTH(a) (sizeof(a) / sizeof(*(a)))

#define MIN(a, b) ((a) < (b) ? (a) : (b))

typedef int32_t bool32;
const bool32 false = 0;
const bool32 true = 1;

typedef enum
{
  ALLOCATE_OK,
  ALLOCATE_OUT_OF_MEMORY,
} AllocateError;

#define STATIC_ASSERT(C) _Static_assert(C, "")

static void assert(bool32 condition)
{
  if (!condition)
  {
    // Write to null to crash the program
    volatile int8_t *v = NULL;
    *v = 0;
  }
}

// A slice to string, NOT null terminated
typedef struct
{
  size_t length;
  const char *data;
} StringSlice;

static StringSlice string_slice_from_ptr(const char *ptr)
{
  assert(ptr != NULL);

  const char *end = ptr;
  for (; *end != '\0'; ++end) {}

  return (StringSlice){
      .data = ptr,
      .length = (size_t)(end - ptr),
  };
}

static bool32 string_slice_prefix_eq(StringSlice string, StringSlice prefix)
{
  if (string.length < prefix.length)
  {
    return false;
  }

  size_t i = 0;
  for (; i < prefix.length && string.data[i] == prefix.data[i]; ++i) {}

  return i == prefix.length;
}

static bool32 string_slice_eq(StringSlice a, StringSlice b)
{
  if (a.length != b.length)
  {
    return false;
  }

  size_t i = 0;
  for (; i < a.length && a.data[i] == b.data[i]; ++i) {}

  return i == a.length;
}

bool32 string_contains(
    const StringSlice *haystack, size_t length, const StringSlice needle)
{
  for (size_t i = 0; i < length; ++i)
  {
    if (string_slice_eq(needle, haystack[i]))
    {
      return true;
    }
  }

  return false;
}

static AllocateError allocate(void **memory, size_t length)
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

static void deallocate(void *memory, size_t length)
{
  if (memory == NULL)
  {
    return;
  }
  free(memory);
}

// TODO: Optimise
// TODO Try using a macro to cover types
static void
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
static void
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

// TODO: Use unsigned char instead of void
static AllocateError reallocate_update_length(
    size_t object_size, void **memory, size_t *length, size_t new_length)
{
  assert(object_size > 0);
  assert(memory != NULL);
  assert(length != NULL);
  assert(*memory == NULL || *length > 0);

  void *new_memory = NULL;
  if (allocate(&new_memory, object_size * new_length) == ALLOCATE_OUT_OF_MEMORY)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  memory_copy_forward(
      new_memory, *memory, object_size * MIN(*length, new_length));

  deallocate(*memory, object_size * *length);

  *memory = new_memory;
  *length = new_length;
  return ALLOCATE_OK;
}

static AllocateError
reallocate(size_t object_size, void **memory, size_t length, size_t new_length)
{
  return reallocate_update_length(object_size, memory, &length, new_length);
}

#define STD_H
#endif
