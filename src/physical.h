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

// ----- Physical Relation -----

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
  PHYSICAL_RELATION_CREATE_OK,
  PHYSICAL_RELATION_CREATE_FAILED_TO_CREATE,
  PHYSICAL_RELATION_CREATE_FAILED_TO_STAT,
  PHYSICAL_RELATION_CREATE_ALREADY_EXISTS,
  PHYSICAL_RELATION_CREATE_PROGRAM_ERROR,
  PHYSICAL_RELATION_CREATE_FAILED_TO_WRITE,
} PhysicalRelationCreateError;

PhysicalRelationCreateError physical_relation_create(
    DiskBufferPool *pool, RelationId id, bool32 expect_new);

void physical_relation_delete(DiskBufferPool *pool, RelationId id);

typedef enum
{
  PHYSICAL_RELATION_INSERT_TUPLE_OK,
  PHYSICAL_RELATION_INSERT_TUPLE_SAVING,
  PHYSICAL_RELATION_INSERT_TUPLE_OPENING_BUFFER,
  PHYSICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL,
  PHYSICAL_RELATION_INSERT_TUPLE_TOO_BIG,
} PhysicalRelationInsertTupleError;

// TODO: This assumes that the column types do not change between inserts
PhysicalRelationInsertTupleError physical_relation_insert_tuple(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    const ColumnValue *values,
    const bool32 *primary_keys,
    ColumnsLength tuple_length);

typedef enum
{
  PHYSICAL_RELATION_DELETE_TUPLES_OK,
  PHYSICAL_RELATION_DELETE_TUPLES_READING,
  PHYSICAL_RELATION_DELETE_TUPLES_WRITING,
  PHYSICAL_RELATION_DELETE_TUPLES_BUFFER_POOL_FULL,
} PhysicalRelationDeleteTuplesError;

PhysicalRelationDeleteTuplesError physical_relation_delete_tuples(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index,
    ColumnValue value);

typedef enum
{
  PHYSICAL_RELATION_ITERATOR_STATUS_OK,
  PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES,
  PHYSICAL_RELATION_ITERATOR_STATUS_ERROR,
} PhysicalRelationIteratorStatus;

typedef struct
{
  DiskBufferPool *pool;
  RelationId relation_id;
  size_t buffer_index;
  size_t tuple_index;
  PhysicalRelationIteratorStatus status;
} PhysicalRelationIterator;

PhysicalRelationIterator
physical_relation_iterate(DiskBufferPool *pool, RelationId id);

void physical_relation_iterator_next(PhysicalRelationIterator *it);

ColumnValue physical_relation_iterator_get(
    PhysicalRelationIterator *it,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index);

void physical_relation_iterator_close(PhysicalRelationIterator *it);

// ----- Physical Relation -----

#define PHYSICAL_H
#endif
