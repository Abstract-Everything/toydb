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

typedef enum
{
  DISK_RESOURCE_CREATE_OK,
  DISK_RESOURCE_CREATE_OPENING,
  DISK_RESOURCE_CREATE_STAT,
  DISK_RESOURCE_CREATE_ALREADY_EXISTS,
  DISK_RESOURCE_CREATE_PROGRAM_ERROR,
  DISK_RESOURCE_CREATE_TRUNCATING,
  DISK_RESOURCE_CREATE_CLOSING,
} DiskResourceCreate;

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

bool32 column_value_eq(ColumnType type, ColumnValue lhs, ColumnValue rhs);

typedef struct
{
  ColumnsLength length;
  const ColumnType *types;
  const void *fixed_data;
  const void *variable_data;
} Tuple;

int16_t tuple_data_length(
    ColumnsLength tuple_length,
    const ColumnType *types,
    const ColumnValue *values);

Tuple tuple_from_data(
    ColumnsLength tuple_length,
    const ColumnType *types,
    size_t data_length,
    void *data,
    const ColumnValue *values);

ColumnValue tuple_get(Tuple tuple, ColumnsLength index);

StoreInteger tuple_get_integer(Tuple tuple, ColumnsLength index);
StoreBoolean tuple_get_boolean(Tuple tuple, ColumnsLength index);
StringSlice tuple_get_string(Tuple tuple, ColumnsLength index);

DiskResourceCreate physical_relation_create(
    DiskBufferPool *pool, RelationId id, bool32 expect_new);

void physical_relation_delete(DiskBufferPool *pool, RelationId id);

typedef enum
{
  PHYSICAL_RELATION_INSERT_TUPLE_OK,
  PHYSICAL_RELATION_INSERT_TUPLE_TOO_BIG,
} PhysicalRelationInsertTupleError;

typedef enum
{
  PHYSICAL_RELATION_ITERATOR_STATUS_OK,
  PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS,
  PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE,
  PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL,
  PHYSICAL_RELATION_ITERATOR_STATUS_IO,
} PhysicalRelationIteratorStatus;

typedef struct
{
  DiskBufferPool *pool;
  RelationId relation_id;
  size_t buffer_index;
  int16_t tuple_index;
  PhysicalRelationIteratorStatus status;

  ColumnsLength tuple_length;
  const ColumnType *types;
  int16_t tuple_fixed_size;

  int16_t deleted_records;
  int16_t deleted_variable_data;
} PhysicalRelationIterator;

PhysicalRelationIterator physical_relation_iterate_tuples(
    DiskBufferPool *pool,
    RelationId id,
    ColumnsLength tuple_length,
    const ColumnType *types);

PhysicalRelationIterator physical_relation_iterate_blocks(
    DiskBufferPool *pool,
    RelationId id,
    ColumnsLength tuple_length,
    const ColumnType *types);

void physical_relation_iterator_next_tuple(PhysicalRelationIterator *it);

void physical_relation_iterator_next_block(PhysicalRelationIterator *it);

void physical_relation_iterator_new_block(PhysicalRelationIterator *it);

Tuple physical_relation_iterator_get(PhysicalRelationIterator *it);

bool32 physical_relation_iterator_is_block_empty(PhysicalRelationIterator *it);

PhysicalRelationInsertTupleError
physical_relation_iterator_insert(PhysicalRelationIterator *it, Tuple tuple);

void physical_relation_iterator_delete(PhysicalRelationIterator *it);

void physical_relation_iterator_close(PhysicalRelationIterator *it);

// ----- Physical Relation -----

#define PHYSICAL_H
#endif
