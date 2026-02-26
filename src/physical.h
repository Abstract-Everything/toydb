#ifndef PHYSICAL_H

#include "linux.h"
#include "std.h"

// ----- Disk buffer pool -----

typedef size_t BlockIndex;

// TODO: Support having a single buffer opened multiple times, this reduces
// memory usage when multiple buffers read the same block from the same relation
struct MappedBuffer;
typedef struct MappedBuffer MappedBuffer;

typedef struct
{
  char save_path_buffer[LINUX_PATH_MAX];
  StringSlice save_path;
  size_t buffers_length;
  MappedBuffer *buffers;
  void *buffer_pages;
} DiskBufferPool;

void disk_buffer_pool_new(
    DiskBufferPool *pool, StringSlice path, void *data, size_t length);

// ----- Disk buffer pool -----

// ----- Relation -----

typedef int64_t RelationId;

// Limit number of columns to 2^16
typedef int16_t ColumnsLength;

// ----- Store types -----
typedef int64_t StoreInteger;

typedef int8_t StoreBoolean;

typedef struct
{
  int16_t offset;
  int16_t length;
} StoreString;

typedef union
{
  StoreInteger integer;
  StoreBoolean boolean;
  StoreString string;
} StoredValue;
// ----- Store types -----

typedef enum
{
  COLUMN_TYPE_INTEGER,
  COLUMN_TYPE_STRING,
  COLUMN_TYPE_BOOLEAN,
} ColumnType;

typedef StoreInteger MemoryInteger;

typedef struct
{
  size_t offset;
  size_t length;
} MemorySlice;

typedef union
{
  StoreInteger integer;
  StoreBoolean boolean;
  StringSlice string;
} ColumnValue;

// TODO: Merge this and ColumnValue
typedef union
{
  MemoryInteger integer;
  MemorySlice string;
} ColumnValue2;

typedef enum
{
  RELATION_CREATE_OK,
  RELATION_CREATE_FAILED_TO_CREATE,
  RELATION_CREATE_FAILED_TO_STAT,
  RELATION_CREATE_ALREADY_EXISTS,
  RELATION_CREATE_PROGRAM_ERROR,
  RELATION_CREATE_FAILED_TO_WRITE,
  RELATION_CREATE_NO_PRIMARY_KEY,
} RelationCreateError;

RelationCreateError relation_create(
    DiskBufferPool *pool,
    RelationId id,
    const bool32 *primary_keys,
    ColumnsLength tuple_length,
    bool32 expect_new);

void relation_delete(DiskBufferPool *pool, RelationId id);

typedef enum
{
  RELATION_INSERT_TUPLE_OK,
  RELATION_INSERT_TUPLE_SAVING,
  RELATION_INSERT_TUPLE_OPENING_BUFFER,
  RELATION_INSERT_TUPLE_BUFFER_POOL_FULL,
  RELATION_INSERT_TUPLE_TOO_BIG,
  RELATION_INSERT_TUPLE_PRIMARY_KEY_VIOLATION,
} RelationInsertTupleError;

// TODO: This assumes that the column types do not change between inserts
RelationInsertTupleError relation_insert_tuple(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    const ColumnValue *values,
    const bool32 *primary_keys,
    ColumnsLength tuple_length);

void relation_delete_tuples(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index,
    ColumnValue value);

typedef enum
{
  RELATION_ITERATOR_STATUS_OK,
  RELATION_ITERATOR_STATUS_NO_MORE_TUPLES,
  RELATION_ITERATOR_STATUS_ERROR,
} RelationIteratorStatus;

typedef struct
{
  DiskBufferPool *pool;
  RelationId relation_id;
  size_t buffer_index;
  size_t tuple_index;
  RelationIteratorStatus status;
} RelationIterator;

RelationIterator relation_iterate(DiskBufferPool *pool, RelationId id);

void relation_iterator_next(RelationIterator *it);

ColumnValue relation_iterator_get(
    RelationIterator *it,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index);

void relation_iterator_close(RelationIterator *it);

// ----- Relation -----

#define PHYSICAL_H
#endif
