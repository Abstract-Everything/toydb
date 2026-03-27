// Improvement: Implement disk_buffer_pool_replace which closes an open buffer
// and opens a new one instead, this removes the possibility of
// BUFFER_POOL_FULL error

#include "physical.h"

#define PAGE_SIZE (size_t)KIBIBYTES(8)

#define RESOURCE_RELATION_SUFFIX ".relation"
#define RESOURCE_SOFT_DELETED_SUFFIX ".deleted"
#define RESOURCE_WRITE_AHEAD_LOG_SUFFIX ".wal"

// ----- Write Ahead Log -----

const LogSequenceNumber LSN_UNINITIALIZED = (LogSequenceNumber){
    .segment_id = 0,
    .segment_offset = 0,
};

const LogSequenceNumber LSN_MINIMUM = (LogSequenceNumber){
    .segment_id = 1,
    .segment_offset = 0,
};

// ----- Write Ahead Log -----

// ----- Disk buffer pool -----

bool32 disk_resource_eq(DiskResource *a, DiskResource *b)
{
  if (a->type != b->type)
  {
    return false;
  }

  switch (a->type)
  {
  case RESOURCE_TYPE_RELATION:
    return a->id == b->id;
  }
}

internal void disk_buffer_pool_close(DiskBufferPool *pool, size_t buffer_index);

typedef enum
{
  DISK_BUFFER_POOL_OPEN_OK,
  DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL,
  DISK_BUFFER_POOL_OPEN_OPENING_FILE,
  DISK_BUFFER_POOL_OPEN_SEEKING_FILE,
  DISK_BUFFER_POOL_OPEN_READING_FILE,
  DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL,
} DiskBufferPoolOpenError;

typedef struct
{
  size_t buffer_index;
  DiskBufferPoolOpenError error;
} DiskBufferPoolOpenResult;

internal DiskBufferPoolOpenResult disk_buffer_pool_open(
    DiskBufferPool *pool, DiskResource resource, BlockIndex block);

typedef enum
{
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK,
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_BUFFER_POOL_FULL,
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OPENING_FILE,
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE,
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READ_FILE_SIZE,
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_SEEKING_FILE,
  DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READING_FILE,
} DiskBufferPoolNewBlockOpenError;

typedef struct
{
  size_t buffer_index;
  DiskBufferPoolNewBlockOpenError error;
} DiskBufferPoolNewBlockOpenResult;

internal DiskBufferPoolNewBlockOpenResult
disk_buffer_pool_new_block_open(DiskBufferPool *pool, DiskResource resource);

internal MappedBuffer *
disk_buffer_pool_mapped_buffer(DiskBufferPool *pool, size_t buffer_index);

internal int16_t mapped_buffer_size();

internal const void *mapped_buffer_read(MappedBuffer *buffer);

internal void
mapped_buffer_update_lsn(MappedBuffer *buffer, LogSequenceNumber lsn);

internal void *mapped_buffer_write(MappedBuffer *buffer);

internal BlockIndex mapped_buffer_block(MappedBuffer *buffer);

internal LogSequenceNumber mapped_buffer_lsn(MappedBuffer *buffer);

// ----- Disk buffer pool -----

// ----- Relation -----

internal size_t column_type_fixed_size(ColumnType type)
{
  StoredValue value;
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
    return sizeof(value.integer);

  case COLUMN_TYPE_STRING:
    return sizeof(value.string);

  // TODO: PACK boolean values
  case COLUMN_TYPE_BOOLEAN:
    return sizeof(value.boolean);
  }
}

internal int16_t column_byte_offset(
    ColumnsLength tuple_length, const ColumnType *types, ColumnsLength index)
{
  assert(index < tuple_length);

  int16_t offset = 0;
  for (size_t i = 0; i < index; ++i)
  {
    offset += column_type_fixed_size(types[i]);
  }
  return offset;
}

bool32 column_value_eq(ColumnType type, ColumnValue lhs, ColumnValue rhs)
{
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
    return lhs.integer == rhs.integer;

  case COLUMN_TYPE_BOOLEAN:
    return lhs.boolean == rhs.boolean;

  case COLUMN_TYPE_STRING:
    return string_slice_eq(lhs.string, rhs.string);
  }
}

internal int16_t
tuple_fixed_length(ColumnsLength tuple_length, const ColumnType *types)
{
  int16_t size = 0;
  for (size_t i = 0; i < tuple_length; ++i)
  {
    size += column_type_fixed_size(types[i]);
  }
  return size;
}

internal int16_t tuple_values_variable_length(
    ColumnsLength tuple_length,
    const ColumnType *types,
    const ColumnValue *values)
{
  int16_t size = 0;
  for (ColumnsLength i = 0; i < tuple_length; ++i)
  {
    switch (types[i])
    {
    case COLUMN_TYPE_INTEGER:
    case COLUMN_TYPE_BOOLEAN:
      size += 0;
      break;

    case COLUMN_TYPE_STRING:
      size += values[i].string.length;
      break;
    }
  }
  return size;
}

int16_t tuple_data_length(
    ColumnsLength tuple_length,
    const ColumnType *types,
    const ColumnValue *values)
{
  return tuple_fixed_length(tuple_length, types)
         + tuple_values_variable_length(tuple_length, types, values);
}

internal int16_t tuple_variable_length(
    ColumnsLength tuple_length, const ColumnType *types, const void *fixed_data)
{
  int16_t size = 0;
  for (ColumnsLength i = 0; i < tuple_length; ++i)
  {
    const StoredValue *value =
        fixed_data + column_byte_offset(tuple_length, types, i);

    switch (types[i])
    {
    case COLUMN_TYPE_INTEGER:
    case COLUMN_TYPE_BOOLEAN:
      size += 0;
      break;

    case COLUMN_TYPE_STRING:
      size += value->string.length;
    }
  }
  return size;
}

Tuple tuple_from_data(
    ColumnsLength tuple_length,
    const ColumnType *types,
    size_t data_length,
    void *data,
    const ColumnValue *values)
{
  assert(data_length == tuple_data_length(tuple_length, types, values));

  void *fixed_data = data;
  void *variable_data = fixed_data + tuple_fixed_length(tuple_length, types);

  void *next_fixed_byte = fixed_data;
  void *next_variable_byte = variable_data;

  for (ColumnsLength column = 0; column < tuple_length; ++column)
  {
    StoredValue *field = next_fixed_byte;

    switch (types[column])
    {

    case COLUMN_TYPE_INTEGER:
      field->integer = values[column].integer;
      break;

    case COLUMN_TYPE_BOOLEAN:
      field->boolean = values[column].boolean;
      break;

    case COLUMN_TYPE_STRING:
    {
      StringSlice string = values[column].string;
      assert(string.length < PAGE_SIZE);
      field->string = (StoreString){
          .length = string.length,
          .offset = next_variable_byte - variable_data,
      };

      memory_copy_forward(next_variable_byte, string.data, string.length);
      next_variable_byte += string.length;
    }
    break;
    }

    next_fixed_byte += column_type_fixed_size(types[column]);
  }

  assert(next_fixed_byte == variable_data);

  assert(
      next_fixed_byte - fixed_data == tuple_fixed_length(tuple_length, types));

  assert(
      next_variable_byte - next_fixed_byte
      == tuple_values_variable_length(tuple_length, types, values));

  return (Tuple){
      .length = tuple_length,
      .types = types,
      .fixed_data = fixed_data,
      .variable_data = variable_data,
  };
}

ColumnValue tuple_get(Tuple tuple, ColumnsLength index)
{
  assert(index < tuple.length);

  const StoredValue *value =
      tuple.fixed_data + column_byte_offset(tuple.length, tuple.types, index);

  switch (tuple.types[index])
  {
  case COLUMN_TYPE_INTEGER:
    return (ColumnValue){.integer = value->integer};

  case COLUMN_TYPE_STRING:
    return (ColumnValue){
        .string =
            (StringSlice){
                .length = value->string.length,
                .data = tuple.variable_data + value->string.offset,
            },
    };

  case COLUMN_TYPE_BOOLEAN:
    return (ColumnValue){.boolean = value->boolean};
  }
}

StoreInteger tuple_get_integer(Tuple tuple, ColumnsLength index)
{
  assert(tuple.types[index] == COLUMN_TYPE_INTEGER);
  return tuple_get(tuple, index).integer;
}

StoreBoolean tuple_get_boolean(Tuple tuple, ColumnsLength index)
{
  assert(tuple.types[index] == COLUMN_TYPE_BOOLEAN);
  return tuple_get(tuple, index).boolean;
}

StringSlice tuple_get_string(Tuple tuple, ColumnsLength index)
{
  assert(tuple.types[index] == COLUMN_TYPE_STRING);
  return tuple_get(tuple, index).string;
}

typedef struct
{
  int16_t allocated_records;
  int16_t variable_data_start;
} RelationHeader;

typedef struct
{
  int16_t variable_data_start;
} TupleHeader;

internal const RelationHeader *relation_header_read(MappedBuffer *buffer)
{
  return mapped_buffer_read(buffer);
}

internal int16_t relation_tuple_size(int16_t fixed_size)
{
  return sizeof(TupleHeader) + fixed_size;
}

typedef struct
{
  const TupleHeader *header;
  const void *data;
  Tuple tuple;
} StoredTupleRead;

internal StoredTupleRead relation_tuple(
    MappedBuffer *buffer,
    ColumnsLength tuple_length,
    const ColumnType *types,
    int16_t index)
{
  assert(types != NULL);
  assert(tuple_length > 0);

  const void *pointer =
      mapped_buffer_read(buffer) + sizeof(RelationHeader)
      + (relation_tuple_size(tuple_fixed_length(tuple_length, types)) * index);

  const TupleHeader *header = pointer;

  return (StoredTupleRead){
      .header = header,
      .data = pointer,
      .tuple =
          (Tuple){
              .length = tuple_length,
              .types = types,
              .fixed_data = pointer + sizeof(*header),
              .variable_data =
                  mapped_buffer_read(buffer) + header->variable_data_start,
          },
  };
}

internal RelationHeader *relation_header_write(MappedBuffer *buffer)
{
  return mapped_buffer_write(buffer);
}

internal void *relation_tuple_data_write(
    MappedBuffer *buffer, int16_t fixed_size, int16_t index)
{
  return mapped_buffer_write(buffer) + sizeof(RelationHeader)
         + (relation_tuple_size(fixed_size) * index);
}

internal TupleHeader *relation_tuple_header_write(
    MappedBuffer *buffer, int16_t fixed_size, int16_t index)
{
  return (TupleHeader *)(relation_tuple_data_write(buffer, fixed_size, index));
}

internal void *relation_tuple_fixed_data_write(
    MappedBuffer *buffer, int16_t fixed_size, int16_t index)
{
  TupleHeader *header = relation_tuple_header_write(buffer, fixed_size, index);
  void *data = relation_tuple_data_write(buffer, fixed_size, index);
  return data + sizeof(*header);
}

internal size_t relation_free_space(MappedBuffer *buffer, int16_t fixed_size)
{
  const RelationHeader *header = relation_header_read(buffer);
  return header->variable_data_start
         - (relation_tuple_size(fixed_size) * header->allocated_records);
}

internal void overwrite_last_deleted_tuple_with_valid_tuple(
    PhysicalRelationIterator *it, int16_t tuple_index)
{
  if (it->deleted_records == 0)
  {
    return;
  }

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);

  const RelationHeader *header = relation_header_read(buffer);

  StoredTupleRead stored_tuple =
      relation_tuple(buffer, it->tuple_length, it->types, tuple_index);

  int16_t tuple_variable_data_length = tuple_variable_length(
      it->tuple_length, it->types, stored_tuple.tuple.fixed_data);

  memory_copy_backward(
      mapped_buffer_write(buffer) + stored_tuple.header->variable_data_start
          + it->deleted_variable_data + tuple_variable_data_length - 1,
      stored_tuple.tuple.variable_data + tuple_variable_data_length - 1,
      tuple_variable_data_length);

  memory_copy_forward(
      relation_tuple_data_write(
          buffer, it->tuple_fixed_size, tuple_index - it->deleted_records),
      stored_tuple.data,
      relation_tuple_size(it->tuple_fixed_size));

  relation_tuple_header_write(
      buffer, it->tuple_fixed_size, tuple_index - it->deleted_records)
      ->variable_data_start += it->deleted_variable_data;
}

/// Some tuple may have been deleted from the block, this results in the block
/// having some 'holes' in the data, before closing the buffer we should replace
/// these holes with actual tuples, otherwise iteration will return garbage data
internal void overwrite_deleted_tuples(PhysicalRelationIterator *it)
{
  if (it->deleted_records == 0)
  {
    assert(it->deleted_variable_data == 0);
    return;
  }

  assert(it->tuple_index >= it->deleted_records);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);

  RelationHeader *header = relation_header_write(buffer);

  for (int16_t tuple_index = it->tuple_index;
       tuple_index < header->allocated_records;
       ++tuple_index)
  {
    overwrite_last_deleted_tuple_with_valid_tuple(it, tuple_index);
  }

  header->allocated_records -= it->deleted_records;
  header->variable_data_start += it->deleted_variable_data;

  it->deleted_records = 0;
  it->deleted_variable_data = 0;
}

internal void close_buffer_if_open(PhysicalRelationIterator *it)
{
  if (it->buffer_index == it->pool->buffers_length)
  {
    return;
  }

  overwrite_deleted_tuples(it);
  disk_buffer_pool_close(it->pool, it->buffer_index);
  it->buffer_index = it->pool->buffers_length;
}

PhysicalRelationIterator physical_relation_iterator(
    DiskBufferPool *pool,
    RelationId id,
    ColumnsLength tuple_length,
    const ColumnType *types)
{
  assert(pool != NULL);
  assert(types != NULL);
  assert(tuple_length > 0);

  return (PhysicalRelationIterator){
      .pool = pool,
      .relation_id = id,
      .buffer_index = pool->buffers_length,
      .tuple_index = 0,

      .tuple_length = tuple_length,
      .types = types,
      .tuple_fixed_size = tuple_fixed_length(tuple_length, types),

      .deleted_records = 0,
      .deleted_variable_data = 0,
  };
}

internal void
write_new_relation_header(DiskBufferPool *pool, size_t buffer_index)
{
  MappedBuffer *buffer = disk_buffer_pool_mapped_buffer(pool, buffer_index);

  assert(mapped_buffer_lsn(buffer).segment_id == LSN_UNINITIALIZED.segment_id);

  mapped_buffer_update_lsn(buffer, LSN_MINIMUM);

  *relation_header_write(buffer) = (RelationHeader){
      .allocated_records = 0,
      .variable_data_start = mapped_buffer_size(),
  };
}

PhysicalRelationIteratorStatus
physical_relation_iterator_open(PhysicalRelationIterator *it, BlockIndex index)
{
  close_buffer_if_open(it);

  DiskBufferPoolOpenResult result = disk_buffer_pool_open(
      it->pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .id = it->relation_id,
      },
      index);

  switch (result.error)
  {
  case DISK_BUFFER_POOL_OPEN_OK:
  {
    MappedBuffer *buffer =
        disk_buffer_pool_mapped_buffer(it->pool, result.buffer_index);

    if (mapped_buffer_lsn(buffer).segment_id == LSN_UNINITIALIZED.segment_id)
    {
      write_new_relation_header(it->pool, result.buffer_index);
    }

    it->buffer_index = result.buffer_index;
    it->tuple_index = 0;
  }
    return PHYSICAL_RELATION_ITERATOR_STATUS_OK;

  case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
    return PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS;

  case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
  case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
  case DISK_BUFFER_POOL_OPEN_READING_FILE:
    return PHYSICAL_RELATION_ITERATOR_STATUS_IO;

  case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
    return PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL;
  }
}

PhysicalRelationIteratorStatus
physical_relation_iterate_blocks(PhysicalRelationIterator *it)
{
  return physical_relation_iterator_open(it, 0);
}

PhysicalRelationIteratorStatus
physical_relation_iterate_tuples(PhysicalRelationIterator *it)
{
  PhysicalRelationIteratorStatus status =
      physical_relation_iterator_open(it, 0);
  if (status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
      && physical_relation_iterator_is_block_empty(it))
  {
    return physical_relation_iterator_next_tuple(it);
  }
  return status;
}

PhysicalRelationIteratorStatus
physical_relation_iterator_next_block(PhysicalRelationIterator *it)
{
  BlockIndex block = mapped_buffer_block(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index));
  return physical_relation_iterator_open(it, block + 1);
}

PhysicalRelationIteratorStatus
physical_relation_iterator_new_block(PhysicalRelationIterator *it)
{
  close_buffer_if_open(it);

  DiskBufferPoolNewBlockOpenResult result = disk_buffer_pool_new_block_open(
      it->pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .id = it->relation_id,
      });

  switch (result.error)
  {
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK:
  {
    write_new_relation_header(it->pool, result.buffer_index);

    it->buffer_index = result.buffer_index;
    it->tuple_index = 0;
    return PHYSICAL_RELATION_ITERATOR_STATUS_OK;
  }
  break;

  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_BUFFER_POOL_FULL:
    return PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL;

  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OPENING_FILE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READ_FILE_SIZE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_SEEKING_FILE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READING_FILE:
    return PHYSICAL_RELATION_ITERATOR_STATUS_IO;
  }
}

PhysicalRelationIteratorStatus
physical_relation_iterator_next_tuple(PhysicalRelationIterator *it)
{
  assert(it != NULL);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);

  it->tuple_index += 1;
  if (it->tuple_index < relation_header_read(buffer)->allocated_records)
  {
    overwrite_last_deleted_tuple_with_valid_tuple(it, it->tuple_index);
    return PHYSICAL_RELATION_ITERATOR_STATUS_OK;
  }

  PhysicalRelationIteratorStatus status =
      physical_relation_iterator_next_block(it);

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
         && physical_relation_iterator_is_block_empty(it);
       status = physical_relation_iterator_next_block(it))
  {
  }

  return status;
}

BlockIndex physical_relation_iterator_block_index(PhysicalRelationIterator *it)
{
  assert(it != NULL);
  return mapped_buffer_block(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index));
}

LogSequenceNumber
physical_relation_iterator_block_lsn(PhysicalRelationIterator *it)
{
  assert(it != NULL);
  return mapped_buffer_lsn(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index));
}

Tuple physical_relation_iterator_get(PhysicalRelationIterator *it)
{
  assert(it != NULL);

  const RelationHeader *header = relation_header_read(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index));

  assert(it->tuple_index < header->allocated_records);

  return relation_tuple(
             disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index),
             it->tuple_length,
             it->types,
             it->tuple_index)
      .tuple;
}

bool32 physical_relation_iterator_is_block_empty(PhysicalRelationIterator *it)
{
  assert(it != NULL);

  const RelationHeader *header = relation_header_read(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index));
  return header->allocated_records == 0;
}

bool32 physical_relation_iterator_insert_tuple_fits(
    PhysicalRelationIterator *it, Tuple tuple)
{
  assert(it != NULL);

  const int16_t tuple_variable_byte_length =
      tuple_variable_length(tuple.length, tuple.types, tuple.fixed_data);

  const size_t required_space =
      relation_tuple_size(it->tuple_fixed_size)
      + tuple_variable_length(tuple.length, tuple.types, tuple.fixed_data);

  const size_t free_space = relation_free_space(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index),
      it->tuple_fixed_size);

  return required_space <= free_space;
}

void physical_relation_iterator_insert(
    PhysicalRelationIterator *it, LogSequenceNumber lsn, Tuple tuple)
{
  assert(it != NULL);
  assert(physical_relation_iterator_insert_tuple_fits(it, tuple));

  const int16_t tuple_variable_byte_length =
      tuple_variable_length(tuple.length, tuple.types, tuple.fixed_data);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);

  mapped_buffer_update_lsn(buffer, lsn);

  RelationHeader *header = relation_header_write(buffer);

  header->variable_data_start -= tuple_variable_byte_length;

  *relation_tuple_header_write(
      buffer, it->tuple_fixed_size, header->allocated_records) = (TupleHeader){
      .variable_data_start = header->variable_data_start,
  };

  memory_copy_forward(
      mapped_buffer_write(buffer) + header->variable_data_start,
      tuple.variable_data,
      tuple_variable_byte_length);

  memory_copy_forward(
      relation_tuple_fixed_data_write(
          buffer, it->tuple_fixed_size, header->allocated_records),
      tuple.fixed_data,
      it->tuple_fixed_size);

  header->allocated_records += 1;

  assert(
      relation_tuple_size(it->tuple_fixed_size) * header->allocated_records
      <= header->variable_data_start);
}

PhysicalRelationIteratorStatus physical_relation_iterator_delete(
    PhysicalRelationIterator *it, LogSequenceNumber lsn)
{
  assert(it != NULL);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);
  mapped_buffer_update_lsn(buffer, lsn);

  mapped_buffer_write(buffer);

  it->deleted_records += 1;
  it->deleted_variable_data += tuple_variable_length(
      it->tuple_length,
      it->types,
      physical_relation_iterator_get(it).fixed_data);

  return physical_relation_iterator_next_tuple(it);
}

void physical_relation_iterator_close(PhysicalRelationIterator *it)
{
  assert(it != NULL);

  close_buffer_if_open(it);
}

// ----- Relation -----

// ----- Write Ahead Log -----

#define LOG_SEGMENT_SIZE (size_t)MEBIBYTES(16)

#define LOG_SEGMENT_DATA_SIZE (LOG_SEGMENT_SIZE - sizeof(WalSegmentHeader))

#define UNINITIALIZED_LAST_ENTRY_OFFSET LOG_SEGMENT_DATA_SIZE

STATIC_ASSERT(LOG_SEGMENT_DATA_SIZE > 0);

const WalSegmentHeader UNINITIALIZED_WAL_SEGMENT_HEADER = (WalSegmentHeader){
    .last_entry_offset = UNINITIALIZED_LAST_ENTRY_OFFSET,
};

CompareRelation lsn_cmp(LogSequenceNumber lhs, LogSequenceNumber rhs)
{
  assert(lhs.segment_id > 0);
  assert(rhs.segment_id > 0);

  if (lhs.segment_id < rhs.segment_id)
  {
    return CMP_SMALLER;
  }

  if (lhs.segment_id > rhs.segment_id)
  {
    return CMP_GREATER;
  }

  if (lhs.segment_offset < rhs.segment_offset)
  {
    return CMP_SMALLER;
  }

  if (lhs.segment_offset > rhs.segment_offset)
  {
    return CMP_GREATER;
  }

  return CMP_EQUAL;
}

internal LogSequenceNumber lsn_sub(LogSequenceNumber lsn, SegmentOffset offset)
{
  if (lsn.segment_offset >= offset)
  {
    lsn.segment_offset -= offset;
    return lsn;
  }

  offset -= lsn.segment_offset;

  int32_t ids = (int32_t)(offset / LOG_SEGMENT_DATA_SIZE) + 1;
  assert(ids > 0);

  lsn.segment_id -= ids;
  lsn.segment_offset = LOG_SEGMENT_DATA_SIZE - (offset % LOG_SEGMENT_DATA_SIZE);
  return lsn;
}

internal LogSequenceNumber lsn_add(LogSequenceNumber lsn, SegmentOffset offset)
{
  lsn.segment_id += offset / LOG_SEGMENT_DATA_SIZE;
  lsn.segment_offset += offset % LOG_SEGMENT_DATA_SIZE;
  if (lsn.segment_offset >= LOG_SEGMENT_DATA_SIZE)
  {
    lsn.segment_id += 1;
    lsn.segment_offset -= LOG_SEGMENT_DATA_SIZE;
  }
  return lsn;
}

internal size_t lsn_distance(LogSequenceNumber from, LogSequenceNumber to)
{
  assert(lsn_cmp(from, to) != CMP_GREATER);

  return ((to.segment_id - from.segment_id) * LOG_SEGMENT_DATA_SIZE)
         + to.segment_offset - from.segment_offset;
}

internal bool32
wal_entry_payload_eq(WalEntryTag tag, WalEntryPayload a, WalEntryPayload b)
{
  // Change this operator if the structure changes
  STATIC_ASSERT(sizeof(WalEntryPayload) == 24);

  switch (tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
  case WAL_ENTRY_START:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
    return true;

  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
    return a.relation_id == b.relation_id;

  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
    return a.tuple.block == b.tuple.block && a.tuple.length == b.tuple.length
           && a.tuple.relation_id == b.tuple.relation_id;

  case WAL_ENTRY_UNDO:
    return a.undo.tag == b.undo.tag
           && lsn_cmp(a.undo.lsn, b.undo.lsn) == CMP_EQUAL;
    break;
  }
}

internal StringSlice wal_segment_id_to_path(
    StringSlice save_path, SegmentId segment_id, char *path, size_t length)
{
  assert(path != NULL);
  assert(length > 0);

  size_t index = string_slice_concat(path, 0, length, save_path, false);
  index = string_slice_concat(
      path, index, length, string_slice_from_ptr("/"), false);

  index += append_uint64_as_string(segment_id, path + index, length - index);

  index = string_slice_concat(
      path,
      index,
      length,
      string_slice_from_ptr(RESOURCE_WRITE_AHEAD_LOG_SUFFIX),
      true);

  return (StringSlice){.data = path, .length = index};
}

typedef enum
{
  WAL_WRITE_SEGMENT_OK,
  WAL_WRITE_SEGMENT_DENIED,
  WAL_WRITE_SEGMENT_TEMPORARY_FAILURE,
  WAL_WRITE_SEGMENT_PROGRAM_ERROR,
  WAL_WRITE_SEGMENT_NO_MEMORY,
  WAL_WRITE_SEGMENT_NO_SPACE,
} WalWriteSegmentError;

// TODO: Only save data until next_entry_lsn
// TODO: Only save data since last_persisted_lsn
internal WalWriteSegmentError wal_write_segment(
    StringSlice save_path,
    SegmentId segment_id,
    WalSegmentHeader header,
    void *memory)
{
  // TODO: Use arena
  char path[LINUX_PATH_MAX];
  wal_segment_id_to_path(save_path, segment_id, path, LINUX_PATH_MAX);

  LinuxOpenResult open_result = linux_open(
      path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

  bool32 already_exists = false;

  WalWriteSegmentError error = WAL_WRITE_SEGMENT_OK;
  switch (open_result.error)
  {
  case LINUX_OPEN_OK:
    already_exists = false;
    break;

  case LINUX_OPEN_ALREADY_EXISTS:
    already_exists = true;
    break;

  case LINUX_OPEN_ACCESS:
  case LINUX_OPEN_OPERATION_NOT_SUPPORTED:
  case LINUX_OPEN_PERMISSIONS:
  case LINUX_OPEN_READ_ONLY_FILESYSTEM:
    error = WAL_WRITE_SEGMENT_DENIED;
    break;

  case LINUX_OPEN_NO_SPACE_ON_DEVICE:
    error = WAL_WRITE_SEGMENT_NO_SPACE;
    break;

  case LINUX_OPEN_BUSY:
  case LINUX_OPEN_QUOTA:
  case LINUX_OPEN_INTERRUPT:
  case LINUX_OPEN_TOO_MANY_FD_OPEN:
  case LINUX_OPEN_SYSTEM_OPEN_FILE_LIMIT:
  case LINUX_OPEN_FILE_BUSY:
    error = WAL_WRITE_SEGMENT_TEMPORARY_FAILURE;
    break;

  case LINUX_OPEN_NO_MEMORY:
    error = WAL_WRITE_SEGMENT_NO_MEMORY;
    break;

  case LINUX_OPEN_PATH_SEG_FAULT:
  case LINUX_OPEN_PATH_FILE_TOO_BIG:
  case LINUX_OPEN_FLAGS_MISUSE:
  case LINUX_OPEN_IS_DIRECTORY:
  case LINUX_OPEN_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_OPEN_PATH_TOO_LONG:
  case LINUX_OPEN_NO_DEVICE:
  case LINUX_OPEN_NOT_FOUND:
  case LINUX_OPEN_NOT_DIRECTORY:
  case LINUX_OPEN_NO_DEVICE_OR_ADDRESS:
  case LINUX_OPEN_WOULD_BLOCK:
  case LINUX_OPEN_UNKNOWN:
    error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
    break;
  }

  if (error == WAL_WRITE_SEGMENT_OK)
  {
    if (already_exists)
    {
      switch (
          linux_seek(open_result.fd, sizeof(WalSegmentHeader), SEEK_SET).error)
      {
      case LINUX_SEEK_OK:
        break;

      case LINUX_SEEK_BAD_FD:
      case LINUX_SEEK_WHENCE_INVALID:
      case LINUX_SEEK_INVALID_OFFSET:
      case LINUX_SEEK_FILE_OFFSET_TOO_BIG:
      case LINUX_SEEK_NOT_A_FILE:
      case LINUX_SEEK_UNKNOWN:
        error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
        break;
      }
    }
    else
    {
      switch (linux_write(
                  open_result.fd,
                  &UNINITIALIZED_WAL_SEGMENT_HEADER,
                  sizeof(UNINITIALIZED_WAL_SEGMENT_HEADER))
                  .error)
      {
      case LINUX_WRITE_OK:
        break;

      case LINUX_WRITE_QUOTA:
      case LINUX_WRITE_INTERRUPT:
      case LINUX_WRITE_IO:
        error = WAL_WRITE_SEGMENT_TEMPORARY_FAILURE;
        break;

      case LINUX_WRITE_PERMISSIONS:
        error = WAL_WRITE_SEGMENT_DENIED;
        break;

      case LINUX_WRITE_NO_SPACE:
        error = WAL_WRITE_SEGMENT_NO_SPACE;
        break;

      case LINUX_WRITE_WOULD_BLOCK:
      case LINUX_WRITE_BAD_FD:
      case LINUX_WRITE_BUFFER_SEG_FAULT:
      case LINUX_WRITE_INVALID_PEER_ADDRESS:
      case LINUX_WRITE_LENGTH_TOO_BIG:
      case LINUX_WRITE_INVALID:
      case LINUX_WRITE_PIPE_CLOSED:
      case LINUX_WRITE_UNKNOWN:
        error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
        break;
      }
    }
  }

  if (error == WAL_WRITE_SEGMENT_OK)
  {
    switch (linux_write(open_result.fd, memory, LOG_SEGMENT_DATA_SIZE).error)
    {
    case LINUX_WRITE_OK:
      break;

    case LINUX_WRITE_PERMISSIONS:
      error = WAL_WRITE_SEGMENT_DENIED;
      break;

    case LINUX_WRITE_QUOTA:
    case LINUX_WRITE_INTERRUPT:
    case LINUX_WRITE_IO:
      error = WAL_WRITE_SEGMENT_TEMPORARY_FAILURE;
      break;

    case LINUX_WRITE_NO_SPACE:
      error = WAL_WRITE_SEGMENT_NO_SPACE;
      break;

    case LINUX_WRITE_WOULD_BLOCK:
    case LINUX_WRITE_BAD_FD:
    case LINUX_WRITE_BUFFER_SEG_FAULT:
    case LINUX_WRITE_INVALID_PEER_ADDRESS:
    case LINUX_WRITE_LENGTH_TOO_BIG:
    case LINUX_WRITE_INVALID:
    case LINUX_WRITE_PIPE_CLOSED:
    case LINUX_WRITE_UNKNOWN:
      error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
      break;
    }
  }

  // Write the header after the data, otherwise a crash might leave the header
  // pointing to a last entry that was never written!
  if (error == WAL_WRITE_SEGMENT_OK)
  {
    switch (linux_seek(open_result.fd, 0, SEEK_SET).error)
    {
    case LINUX_SEEK_OK:
      break;

    case LINUX_SEEK_BAD_FD:
    case LINUX_SEEK_WHENCE_INVALID:
    case LINUX_SEEK_INVALID_OFFSET:
    case LINUX_SEEK_FILE_OFFSET_TOO_BIG:
    case LINUX_SEEK_NOT_A_FILE:
    case LINUX_SEEK_UNKNOWN:
      error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
      break;
    }

    switch (linux_write(open_result.fd, &header, sizeof(header)).error)
    {
    case LINUX_WRITE_OK:
      break;

    case LINUX_WRITE_QUOTA:
    case LINUX_WRITE_INTERRUPT:
    case LINUX_WRITE_IO:
      error = WAL_WRITE_SEGMENT_TEMPORARY_FAILURE;
      break;

    case LINUX_WRITE_PERMISSIONS:
      error = WAL_WRITE_SEGMENT_DENIED;
      break;

    case LINUX_WRITE_NO_SPACE:
      error = WAL_WRITE_SEGMENT_NO_SPACE;
      break;

    case LINUX_WRITE_WOULD_BLOCK:
    case LINUX_WRITE_BAD_FD:
    case LINUX_WRITE_BUFFER_SEG_FAULT:
    case LINUX_WRITE_INVALID_PEER_ADDRESS:
    case LINUX_WRITE_LENGTH_TOO_BIG:
    case LINUX_WRITE_INVALID:
    case LINUX_WRITE_PIPE_CLOSED:
    case LINUX_WRITE_UNKNOWN:
      error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
      break;
    }
  }

  if (error == WAL_WRITE_SEGMENT_OK)
  {
    switch (linux_fdatasync(open_result.fd))
    {
    case LINUX_FDATASYNC_OK:
      break;

    case LINUX_FDATASYNC_READ_ONLY_FILESYSTEM:
      error = WAL_WRITE_SEGMENT_DENIED;
      break;

    case LINUX_FDATASYNC_IO:
    case LINUX_FDATASYNC_QUOTA:
      error = WAL_WRITE_SEGMENT_TEMPORARY_FAILURE;
      break;

    case LINUX_FDATASYNC_NO_SPACE:
      error = WAL_WRITE_SEGMENT_NO_SPACE;
      break;

    case LINUX_FDATASYNC_BAD_FD:
    case LINUX_FDATASYNC_INTERRUPT:
    case LINUX_FDATASYNC_FD_NO_SYNC_SUPPORT:
    case LINUX_FDATASYNC_UNKNOWN:
      error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
      break;
    }
  }

  switch (linux_close(open_result.fd))
  {
  case LINUX_CLOSE_OK:
    break;

  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
    error = WAL_WRITE_SEGMENT_TEMPORARY_FAILURE;
    break;

  case LINUX_CLOSE_BAD_FD:
  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_UNKNOWN:
    error = WAL_WRITE_SEGMENT_PROGRAM_ERROR;
    break;
  }

  return error;
}

internal LogSequenceNumber wal_memory_lsn(WriteAheadLog *log)
{
  return (LogSequenceNumber){
      .segment_id = log->memory_segment_id,
      .segment_offset = 0,
  };
}

internal WalEntry *wal_last_entry(WriteAheadLog *log)
{
  assert(log != NULL);
  assert(
      lsn_distance(wal_memory_lsn(log), log->last_entry_lsn)
      < log->memory_length);

  WalEntry *last_entry =
      log->memory + lsn_distance(wal_memory_lsn(log), log->last_entry_lsn);
  return last_entry;
}

internal WalWriteSegmentError wal_sync(WriteAheadLog *log)
{
  if (lsn_cmp(log->last_persisted_lsn, LSN_MINIMUM) != CMP_EQUAL
      && lsn_cmp(log->last_persisted_lsn, log->last_entry_lsn) == CMP_EQUAL)
  {
    return WAL_WRITE_SEGMENT_OK;
  }

  assert(3 * LOG_SEGMENT_DATA_SIZE == log->memory_length);
  for (size_t i = 2; i > 0; --i)
  {
    // Write the segments in reverse order so that if an entry is partially
    // written, the segment which registers it as the last entry won't be
    // written leading the log to not know about it as opposed to have it be
    // partially written
    if (lsn_distance(wal_memory_lsn(log), log->next_entry_lsn)
        > i * LOG_SEGMENT_DATA_SIZE)
    {
      WalWriteSegmentError error = wal_write_segment(
          log->save_path,
          log->memory_segment_id + i,
          UNINITIALIZED_WAL_SEGMENT_HEADER,
          log->memory + (i * LOG_SEGMENT_DATA_SIZE));

      if (error != WAL_WRITE_SEGMENT_OK)
      {
        return error;
      }
    }
  }

  assert(
      log->last_persisted_lsn.segment_id == log->last_entry_lsn.segment_id - 1
      || log->last_persisted_lsn.segment_id == log->last_entry_lsn.segment_id);

  assert(log->last_entry_lsn.segment_id == log->memory_segment_id);

  WalWriteSegmentError error = wal_write_segment(
      log->save_path,
      log->memory_segment_id,
      (WalSegmentHeader){
          .last_entry_offset = log->last_entry_lsn.segment_offset,
      },
      log->memory);

  if (error != WAL_WRITE_SEGMENT_OK)
  {
    return error;
  }

  log->last_persisted_lsn = log->last_entry_lsn;

  return WAL_WRITE_SEGMENT_OK;
}

internal WalWriteEntryError wal_write_segment_to_memory(
    WriteAheadLog *log, LogSequenceNumber *head, ByteSlice slice)
{
  assert(log != NULL);
  assert(head != NULL);

  if (lsn_distance(log->next_entry_lsn, lsn_add(*head, slice.length))
      >= LOG_SEGMENT_DATA_SIZE)
  {
    return WAL_WRITE_ENTRY_TOO_BIG;
  }

  assert(
      lsn_distance(wal_memory_lsn(log), lsn_add(*head, slice.length))
      <= log->memory_length);

  memory_copy_forward(
      log->memory + lsn_distance(wal_memory_lsn(log), *head),
      slice.data,
      slice.length);

  *head = lsn_add(*head, slice.length);

  return WAL_WRITE_ENTRY_OK;
}

typedef enum
{
  WAL_READ_SEGMENT_OK,
  WAL_READ_SEGMENT_TOO_BIG,
  WAL_READ_SEGMENT_NOT_FOUND,
  WAL_READ_SEGMENT_DENIED,
  WAL_READ_SEGMENT_TEMPORARY_FAILURE,
  WAL_READ_SEGMENT_PROGRAM_ERROR,
  WAL_READ_SEGMENT_NO_MEMORY,
} WalReadSegmentError;

typedef struct
{
  size_t data_read;
  WalReadSegmentError error;
} WalReadSegmentResult;

internal WalReadSegmentResult wal_read_segment_from_disk(
    StringSlice save_path,
    LogSequenceNumber lsn,
    WalSegmentHeader *header,
    void *memory,
    size_t length)
{
  assert(memory != NULL);
  assert(length > 0);
  assert(LOG_SEGMENT_DATA_SIZE - lsn.segment_offset <= length);

  // TODO: Use arena
  char path[LINUX_PATH_MAX];
  wal_segment_id_to_path(save_path, lsn.segment_id, path, LINUX_PATH_MAX);

  LinuxOpenResult open_result = linux_open(path, O_RDONLY, 0);
  switch (open_result.error)
  {
  case LINUX_OPEN_OK:
    break;

  case LINUX_OPEN_NOT_FOUND:
    return (WalReadSegmentResult){.error = WAL_READ_SEGMENT_NOT_FOUND};

  case LINUX_OPEN_ACCESS:
  case LINUX_OPEN_PERMISSIONS:
  case LINUX_OPEN_READ_ONLY_FILESYSTEM:
    return (WalReadSegmentResult){.error = WAL_READ_SEGMENT_DENIED};

  case LINUX_OPEN_BUSY:
  case LINUX_OPEN_QUOTA:
  case LINUX_OPEN_INTERRUPT:
  case LINUX_OPEN_SYSTEM_OPEN_FILE_LIMIT:
  case LINUX_OPEN_NO_SPACE_ON_DEVICE:
  case LINUX_OPEN_FILE_BUSY:
  case LINUX_OPEN_WOULD_BLOCK:
    return (WalReadSegmentResult){.error = WAL_READ_SEGMENT_TEMPORARY_FAILURE};

  case LINUX_OPEN_ALREADY_EXISTS:
  case LINUX_OPEN_PATH_SEG_FAULT:
  case LINUX_OPEN_PATH_FILE_TOO_BIG:
  case LINUX_OPEN_FLAGS_MISUSE:
  case LINUX_OPEN_IS_DIRECTORY:
  case LINUX_OPEN_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_OPEN_TOO_MANY_FD_OPEN:
  case LINUX_OPEN_PATH_TOO_LONG:
  case LINUX_OPEN_NO_DEVICE:
  case LINUX_OPEN_NOT_DIRECTORY:
  case LINUX_OPEN_NO_DEVICE_OR_ADDRESS:
  case LINUX_OPEN_OPERATION_NOT_SUPPORTED:
  case LINUX_OPEN_UNKNOWN:
    return (WalReadSegmentResult){.error = WAL_READ_SEGMENT_PROGRAM_ERROR};

  case LINUX_OPEN_NO_MEMORY:
    return (WalReadSegmentResult){.error = WAL_READ_SEGMENT_NO_MEMORY};
  }

  WalReadSegmentError error = WAL_READ_SEGMENT_OK;

  if (header)
  {
    LinuxReadResult read_result =
        linux_read(open_result.fd, header, sizeof(*header));
    switch (read_result.error)
    {
    case LINUX_READ_OK:
      // Do not update data_read as those bytes are not written into memory
      break;

    case LINUX_READ_IO:
    case LINUX_READ_INTERRUPT:
      error = WAL_READ_SEGMENT_TEMPORARY_FAILURE;
      break;

    case LINUX_READ_WOULD_BLOCK:
    case LINUX_READ_BAD_FD:
    case LINUX_READ_BUFFER_SEG_FAULT:
    case LINUX_READ_INVALID:
    case LINUX_READ_IS_DIR:
    case LINUX_READ_UNKNOWN:
      error = WAL_READ_SEGMENT_PROGRAM_ERROR;
      break;
    }
  }

  if (error == WAL_READ_SEGMENT_OK)
  {
    switch (linux_seek(
                open_result.fd, lsn.segment_offset + sizeof(*header), SEEK_SET)
                .error)
    {
    case LINUX_SEEK_OK:
      break;

    case LINUX_SEEK_BAD_FD:
    case LINUX_SEEK_WHENCE_INVALID:
    case LINUX_SEEK_INVALID_OFFSET:
    case LINUX_SEEK_FILE_OFFSET_TOO_BIG:
    case LINUX_SEEK_NOT_A_FILE:
    case LINUX_SEEK_UNKNOWN:
      error = WAL_READ_SEGMENT_PROGRAM_ERROR;
      break;
    }
  }

  size_t data_read = 0;
  if (error == WAL_READ_SEGMENT_OK)
  {
    LinuxReadResult read_result = linux_read(
        open_result.fd, memory, LOG_SEGMENT_DATA_SIZE - lsn.segment_offset);
    switch (read_result.error)
    {
    case LINUX_READ_OK:
      data_read += read_result.count;
      break;

    case LINUX_READ_IO:
    case LINUX_READ_INTERRUPT:
      error = WAL_READ_SEGMENT_TEMPORARY_FAILURE;
      break;

    case LINUX_READ_WOULD_BLOCK:
    case LINUX_READ_BAD_FD:
    case LINUX_READ_BUFFER_SEG_FAULT:
    case LINUX_READ_INVALID:
    case LINUX_READ_IS_DIR:
    case LINUX_READ_UNKNOWN:
      error = WAL_READ_SEGMENT_PROGRAM_ERROR;
      break;
    }
  }

  switch (linux_close(open_result.fd))
  {
  case LINUX_CLOSE_OK:
    break;

  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
    error = WAL_READ_SEGMENT_TEMPORARY_FAILURE;
    break;

  case LINUX_CLOSE_BAD_FD:
  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_UNKNOWN:
    error = WAL_READ_SEGMENT_PROGRAM_ERROR;
    break;
  }

  return (WalReadSegmentResult){
      .error = error,
      .data_read = data_read,
  };
}

internal WalReadSegmentResult wal_iterator_read_next_segment(WalIterator *it)
{
  LogSequenceNumber lsn = lsn_add(it->memory_lsn, it->bytes_read_into_memory);
  void *memory = it->memory + it->bytes_read_into_memory;

  assert(it->bytes_read_into_memory < it->memory_length);

  if (lsn.segment_id < it->log->memory_segment_id)
  {
    return wal_read_segment_from_disk(
        it->log->save_path,
        lsn,
        NULL,
        it->memory + it->bytes_read_into_memory,
        it->memory_length - it->bytes_read_into_memory);
  }

  if (lsn.segment_id == it->log->memory_segment_id)
  {
    size_t to_read =
        MIN(lsn_distance(lsn, it->log->next_entry_lsn),
            it->memory_length - it->bytes_read_into_memory);

    memory_copy_forward(
        it->memory + it->bytes_read_into_memory,
        it->log->memory + lsn.segment_offset,
        to_read);

    return (WalReadSegmentResult){
        .error = WAL_READ_SEGMENT_OK,
        .data_read = to_read,
    };
  }

  assert(false);
  return (WalReadSegmentResult){.error = WAL_READ_SEGMENT_PROGRAM_ERROR};
}

internal WalIteratorStatus
wal_iterator_read_entry(WalIterator *it, LogSequenceNumber lsn)
{
  // TODO: pass a predict previous or predict next flag so that we either
  // read the memory ending with the LSN or starting with it, if we always
  // assume one way then the other will be very slow causing a disk read
  // with every previous/ next call

  // TODO: We do not need to reread the segments from disk if the
  // current buffer already contains all the contents of the lsn

  assert(it != NULL);

  // HACK
  LogSequenceNumber old_current = it->current;

  it->current = lsn;
  it->bytes_read_into_memory = 0;
  it->memory_lsn = it->current;

  WalReadSegmentResult read_result = wal_iterator_read_next_segment(it);
  switch (read_result.error)
  {
  case WAL_READ_SEGMENT_OK:
    it->current = lsn;
    it->bytes_read_into_memory = 0;
    it->memory_lsn = it->current;
    break;

  case WAL_READ_SEGMENT_NOT_FOUND:
    return wal_iterator_read_entry(it, old_current);

  case WAL_READ_SEGMENT_TOO_BIG:
  case WAL_READ_SEGMENT_DENIED:
  case WAL_READ_SEGMENT_TEMPORARY_FAILURE:
  case WAL_READ_SEGMENT_PROGRAM_ERROR:
  case WAL_READ_SEGMENT_NO_MEMORY:
    return WAL_ITERATOR_STATUS_ERROR;
  }
  it->bytes_read_into_memory += read_result.data_read;

  if (read_result.error == WAL_READ_SEGMENT_OK
      && it->bytes_read_into_memory < sizeof(WalEntryHeader))
  {
    read_result = wal_iterator_read_next_segment(it);
    it->bytes_read_into_memory += read_result.data_read;
  }

  WalEntryHeader *header = it->memory;
  if (read_result.error == WAL_READ_SEGMENT_OK
      && it->bytes_read_into_memory < header->entry_length)
  {
    read_result = wal_iterator_read_next_segment(it);
    it->bytes_read_into_memory += read_result.data_read;
  }

  switch (read_result.error)
  {
  case WAL_READ_SEGMENT_OK:
    assert(
        it->bytes_read_into_memory >= sizeof(WalEntryHeader)
        && it->bytes_read_into_memory >= header->entry_length);

    it->bytes_read_into_memory += read_result.data_read;
    return WAL_ITERATOR_STATUS_OK;

  case WAL_READ_SEGMENT_NOT_FOUND:
  case WAL_READ_SEGMENT_TOO_BIG:
  case WAL_READ_SEGMENT_DENIED:
  case WAL_READ_SEGMENT_TEMPORARY_FAILURE:
  case WAL_READ_SEGMENT_PROGRAM_ERROR:
  case WAL_READ_SEGMENT_NO_MEMORY:
    return WAL_ITERATOR_STATUS_ERROR;
  }
}

WalIterator wal_iterate(WriteAheadLog *log, void *memory, size_t memory_length)
{
  return (WalIterator){
      .log = log,
      .current = LSN_UNINITIALIZED,
      .memory_lsn =
          (LogSequenceNumber){
              .segment_id = 0,
              .segment_offset = 0,
          },
      .bytes_read_into_memory = 0,
      .memory_length = memory_length,
      .memory = memory,
  };
}

size_t wal_entry_payload_size(WalEntryTag tag)
{
  WalEntry entry;
  switch (tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
    return 0;

  case WAL_ENTRY_START:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
    return 0;

  case WAL_ENTRY_UNDO:
    return sizeof(entry.payload.undo);

  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
    return sizeof(entry.payload.relation_id);

  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
    return sizeof(entry.payload.tuple);
  }
}

size_t wal_entry_length(WalEntry entry, size_t bytes_length, ByteSlice *bytes)
{
  size_t length = sizeof(entry.header);
  length += wal_entry_payload_size(entry.header.tag);
  for (size_t i = 0; i < bytes_length; ++i) { length += bytes[i].length; }
  return length;
}

WalWriteResult wal_write_entry(
    WriteAheadLog *log, WalEntry entry, size_t bytes_length, ByteSlice *bytes)
{
  assert(log != NULL);

  LogSequenceNumber head = log->next_entry_lsn;
  entry.header.previous_entry_offset = lsn_distance(log->last_entry_lsn, head);
  entry.header.entry_length = wal_entry_length(entry, bytes_length, bytes);

  WalWriteEntryError error = wal_write_segment_to_memory(
      log,
      &head,
      (ByteSlice){
          .data = &entry.header,
          .length = sizeof(entry.header),
      });
  switch (error)
  {
  case WAL_WRITE_ENTRY_OK:
    break;

  case WAL_WRITE_ENTRY_TOO_BIG:
  case WAL_WRITE_ENTRY_WRITING_SEGMENT:
  case WAL_WRITE_ENTRY_PROGRAM_ERROR:
  case WAL_WRITE_ENTRY_READING_SEGMENT:
    return (WalWriteResult){
        .error = error,
    };
  }

  switch (entry.header.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
    assert(false);
    return (WalWriteResult){.error = WAL_WRITE_ENTRY_PROGRAM_ERROR};

  case WAL_ENTRY_START:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
    break;

  case WAL_ENTRY_UNDO:
    assert(lsn_cmp(entry.payload.undo.lsn, log->next_entry_lsn) == CMP_SMALLER);
    switch (entry.payload.undo.tag)
    {
    case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
    case WAL_ENTRY_START:
    case WAL_ENTRY_COMMIT:
    case WAL_ENTRY_ABORT:
    case WAL_ENTRY_UNDO:
      assert(false);
      break;

    case WAL_ENTRY_CREATE_RELATION_FILE:
    case WAL_ENTRY_DELETE_RELATION_FILE:
    case WAL_ENTRY_INSERT_TUPLE:
    case WAL_ENTRY_DELETE_TUPLE:
      break;
    }

    error = wal_write_segment_to_memory(
        log,
        &head,
        (ByteSlice){
            .data = &entry.payload.undo,
            .length = sizeof(entry.payload.undo),
        });
    break;

  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
    error = wal_write_segment_to_memory(
        log,
        &head,
        (ByteSlice){
            .data = &entry.payload.relation_id,
            .length = sizeof(entry.payload.relation_id),
        });
    break;

  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
    error = wal_write_segment_to_memory(
        log,
        &head,
        (ByteSlice){
            .data = &entry.payload.tuple,
            .length = sizeof(entry.payload.tuple),
        });
    break;
  }

  switch (entry.header.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
    assert(false);
    return (WalWriteResult){.error = WAL_WRITE_ENTRY_PROGRAM_ERROR};

  case WAL_ENTRY_START:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
    if (bytes_length != 0 || bytes != NULL)
    {
      assert(false);
      error = WAL_WRITE_ENTRY_PROGRAM_ERROR;
    }
    break;

  case WAL_ENTRY_UNDO:
  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
    assert(bytes_length > 0);
    assert(bytes != NULL);

    for (size_t i = 0; i < bytes_length && error == WAL_WRITE_ENTRY_OK; ++i)
    {
      error = wal_write_segment_to_memory(log, &head, bytes[i]);
    }
    break;
  }

  switch (error)
  {
  case WAL_WRITE_ENTRY_OK:
  {
    assert(
        lsn_distance(log->next_entry_lsn, head) == entry.header.entry_length);

    log->last_entry_lsn = log->next_entry_lsn;
    log->next_entry_lsn = head;

    if (log->last_entry_lsn.segment_id == log->memory_segment_id + 1)
    {
      memory_copy_forward(
          log->memory,
          log->memory + LOG_SEGMENT_DATA_SIZE,
          2 * LOG_SEGMENT_DATA_SIZE);

      log->memory_segment_id += 1;
    }

    if (log->last_entry_lsn.segment_id != log->next_entry_lsn.segment_id)
    {
      wal_sync(log);
    }

    return (WalWriteResult){
        .lsn = log->last_entry_lsn,
        .error = WAL_WRITE_ENTRY_OK,
    };
  }

  case WAL_WRITE_ENTRY_TOO_BIG:
  case WAL_WRITE_ENTRY_WRITING_SEGMENT:
  case WAL_WRITE_ENTRY_PROGRAM_ERROR:
  case WAL_WRITE_ENTRY_READING_SEGMENT:
    return (WalWriteResult){
        .error = error,
    };
  }
}

WalWriteResult wal_write_tuple_entry(
    WriteAheadLog *log,
    WalEntryTag entry,
    RelationId relation_id,
    BlockIndex block,
    Tuple tuple)
{
  assert(log != NULL);
  switch (entry)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
  case WAL_ENTRY_START:
  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
  case WAL_ENTRY_UNDO:
    assert(false);
    return (WalWriteResult){.error = WAL_WRITE_ENTRY_PROGRAM_ERROR};

  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
    break;
  }

  ByteSlice slices[] = {
      {
          .length = tuple_fixed_length(tuple.length, tuple.types),
          .data = tuple.fixed_data,
      },
      {
          .length = tuple_variable_length(
              tuple.length, tuple.types, tuple.fixed_data),
          .data = tuple.variable_data,
      }};

  return wal_write_entry(
      log,
      (WalEntry){
          .header.tag = entry,
          .payload.tuple =
              {
                  .relation_id = relation_id,
                  .block = block,
                  .length = tuple.length,
              },
      },
      ARRAY_LENGTH(slices),
      slices);
}

WalWriteResult wal_commit_transaction(WriteAheadLog *log)
{
  WalWriteResult result =
      wal_write_entry(log, (WalEntry){.header.tag = WAL_ENTRY_COMMIT}, 0, NULL);
  if (result.error != WAL_WRITE_ENTRY_OK)
  {
    return result;
  }

  switch (wal_sync(log))
  {
  case WAL_WRITE_SEGMENT_OK:
    return (WalWriteResult){
        .error = WAL_WRITE_ENTRY_OK,
        .lsn = result.lsn,
    };

  case WAL_WRITE_SEGMENT_DENIED:
  case WAL_WRITE_SEGMENT_TEMPORARY_FAILURE:
  case WAL_WRITE_SEGMENT_PROGRAM_ERROR:
  case WAL_WRITE_SEGMENT_NO_MEMORY:
  case WAL_WRITE_SEGMENT_NO_SPACE:
    return (WalWriteResult){
        .error = WAL_WRITE_ENTRY_WRITING_SEGMENT,
    };
  }
}

internal WalWriteResult wal_write_undo(
    WriteAheadLog *log,
    LogSequenceNumber lsn,
    void *memory,
    size_t memory_length)
{
  LogSequenceNumber first_undo_lsn = log->next_entry_lsn;

  WalIterator it = wal_iterate(log, memory, memory_length);
  switch (wal_iterator_open(&it, lsn))
  {
  case WAL_ITERATOR_STATUS_OK:
    break;

  case WAL_ITERATOR_STATUS_NO_MORE_ENTRIES:
  case WAL_ITERATOR_STATUS_ERROR:
    return (WalWriteResult){.error = WAL_WRITE_ENTRY_READING_SEGMENT};
    break;
  }

  WalIteratorStatus status = wal_iterator_previous(&it);
  for (; status == WAL_ITERATOR_STATUS_OK; status = wal_iterator_previous(&it))
  {
    WalEntry *entry = wal_iterator_get(&it);
    {
      bool32 break_loop = false;
      switch (entry->header.tag)
      {
      case WAL_ENTRY_START:
        break_loop = true;
        break;

      case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
      case WAL_ENTRY_COMMIT:
      case WAL_ENTRY_ABORT:
      case WAL_ENTRY_UNDO:
        assert(false);
        break;

      case WAL_ENTRY_CREATE_RELATION_FILE:
      case WAL_ENTRY_DELETE_RELATION_FILE:
      case WAL_ENTRY_INSERT_TUPLE:
      case WAL_ENTRY_DELETE_TUPLE:
        break;
      }

      if (break_loop)
      {
        break;
      }
    }

    ByteSlice slice = (ByteSlice){
        .length = entry->header.entry_length - sizeof(entry->header),
        .data = (void *)entry + sizeof(entry->header),
    };

    WalWriteResult result = wal_write_entry(
        log,
        (WalEntry){
            .header.tag = WAL_ENTRY_UNDO,
            .payload.undo =
                {
                    .tag = entry->header.tag,
                    .lsn = it.current,
                },
        },
        1,
        &slice);

    if (result.error != WAL_WRITE_ENTRY_OK)
    {
      return result;
    }
  }

  if (status != WAL_ITERATOR_STATUS_OK)
  {
    return (WalWriteResult){.error = WAL_WRITE_ENTRY_READING_SEGMENT};
  }

  return (WalWriteResult){
      .error = WAL_WRITE_ENTRY_OK,
      .lsn = first_undo_lsn,
  };
}

WalWriteResult
wal_abort_transaction(WriteAheadLog *log, void *memory, size_t memory_length)
{
  WalWriteResult result =
      wal_write_entry(log, (WalEntry){.header.tag = WAL_ENTRY_ABORT}, 0, NULL);

  if (result.error == WAL_WRITE_ENTRY_OK)
  {
    result = wal_write_undo(log, result.lsn, memory, memory_length);
  }

  return result;
}

WalIteratorStatus wal_iterator_open(WalIterator *it, LogSequenceNumber lsn)
{
  if (lsn_cmp(lsn, it->log->last_entry_lsn) == CMP_GREATER)
  {
    return WAL_ITERATOR_STATUS_NO_MORE_ENTRIES;
  }

  WalIteratorStatus status = wal_iterator_read_entry(it, lsn);
  switch (wal_iterator_read_entry(it, lsn))
  {
  case WAL_ITERATOR_STATUS_OK:
    break;

  case WAL_ITERATOR_STATUS_NO_MORE_ENTRIES:
  case WAL_ITERATOR_STATUS_ERROR:
    return status;
  }

  switch (wal_iterator_get(it)->header.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
    return WAL_ITERATOR_STATUS_NO_MORE_ENTRIES;

  case WAL_ENTRY_START:
  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
  case WAL_ENTRY_UNDO:
    return status;
  }
}

WalIteratorStatus wal_iterator_next(WalIterator *it)
{
  assert(it != NULL);
  assert(it->current.segment_id != 0);

  WalEntry *entry = wal_iterator_get(it);
  LogSequenceNumber next = lsn_add(it->current, entry->header.entry_length);

  if (lsn_cmp(next, it->log->last_entry_lsn) == CMP_GREATER)
  {
    return WAL_ITERATOR_STATUS_NO_MORE_ENTRIES;
  }

  return wal_iterator_read_entry(it, next);
}

WalIteratorStatus wal_iterator_previous(WalIterator *it)
{
  assert(it != NULL);
  assert(it->current.segment_id != 0);

  LogSequenceNumber old_current = it->current;

  WalEntry *entry = wal_iterator_get(it);
  assert(entry->header.previous_entry_offset > 0);
  LogSequenceNumber previous =
      lsn_sub(it->current, entry->header.previous_entry_offset);

  WalIteratorStatus status = wal_iterator_read_entry(it, previous);
  switch (status)
  {
  case WAL_ITERATOR_STATUS_OK:
    break;

  case WAL_ITERATOR_STATUS_NO_MORE_ENTRIES:
  case WAL_ITERATOR_STATUS_ERROR:
    return status;
  }

  switch (wal_iterator_get(it)->header.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
    status = wal_iterator_read_entry(it, old_current);
    switch (status)
    {
    case WAL_ITERATOR_STATUS_OK:
      return WAL_ITERATOR_STATUS_NO_MORE_ENTRIES;

    case WAL_ITERATOR_STATUS_NO_MORE_ENTRIES:
    case WAL_ITERATOR_STATUS_ERROR:
      return status;
    }

  case WAL_ENTRY_START:
  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
  case WAL_ENTRY_UNDO:
    return status;
  }
}

WalEntry *wal_iterator_get(WalIterator *it)
{
  assert(it != NULL);
  assert(it->current.segment_id != 0);

  WalEntry *entry = it->memory;

  assert(it->bytes_read_into_memory >= entry->header.entry_length);

  return entry;
}

WalUndoEntry wal_iterator_get_undo_entry(WalIterator *it)
{
  assert(it != NULL);

  WalEntry *entry = wal_iterator_get(it);
  assert(entry->header.tag == WAL_ENTRY_UNDO);

  WalEntryPayload *undo_payload = (void *)entry
                                  + offsetof(WalEntry, payload.undo)
                                  + sizeof(entry->payload.undo);

  void *data =
      (void *)undo_payload + wal_entry_payload_size(entry->payload.undo.tag);

  return (WalUndoEntry){
      .payload = undo_payload,
      .data = data,
  };
}

Tuple wal_iterator_get_tuple(WalIterator *it, ColumnType *types)
{
  assert(it != NULL);
  assert(types != NULL);

  WalEntry *entry = wal_iterator_get(it);
  switch (entry->header.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
  case WAL_ENTRY_START:
  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
    assert(false);
    return (Tuple){};
    break;

  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
  {
    void *data = (void *)entry + offsetof(WalEntry, payload)
                 + wal_entry_payload_size(entry->header.tag);

    return (Tuple){
        .length = entry->payload.tuple.length,
        .types = types,
        .fixed_data = data,
        .variable_data =
            data + tuple_fixed_length(entry->payload.tuple.length, types),
    };
  }
  break;

  case WAL_ENTRY_UNDO:
  {
    WalUndoEntry undo_entry = wal_iterator_get_undo_entry(it);

    return (Tuple){
        .length = undo_entry.payload->tuple.length,
        .types = types,
        .fixed_data = undo_entry.data,
        .variable_data =
            undo_entry.data
            + tuple_fixed_length(undo_entry.payload->tuple.length, types),
    };
  }
  break;
  }
}

typedef enum
{
  WAL_FIND_SEGMENT_ID_RANGE_FOUND,
  WAL_FIND_SEGMENT_ID_RANGE_NOT_FOUND,
  WAL_FIND_SEGMENT_ID_RANGE_DENIED,
  WAL_FIND_SEGMENT_ID_RANGE_TEMPORARY_FAILURE,
  WAL_FIND_SEGMENT_ID_RANGE_PROGRAM_ERROR,
  WAL_FIND_SEGMENT_ID_RANGE_NO_MEMORY,
  WAL_FIND_SEGMENT_ID_RANGE_NO_SPACE,
  WAL_FIND_SEGMENT_ID_RANGE_ENUMERATING_DIRECTORY_ENTRIES,
  WAL_FIND_SEGMENT_ID_RANGE_PARSING_ENTRY_NAME,
} WalFindSegmentIdRangeError;

typedef struct
{
  WalFindSegmentIdRangeError error;
  SegmentId smallest;
  SegmentId largest;
} WalFindSegmentIdRangeResult;

internal WalFindSegmentIdRangeResult
wal_find_segment_id_range(StringSlice save_path)
{
  char path[LINUX_PATH_MAX];
  string_slice_concat(path, 0, ARRAY_LENGTH(path), save_path, true);

  LinuxOpenResult open_result = linux_open(path, O_RDONLY | O_DIRECTORY, 0);
  switch (open_result.error)
  {
  case LINUX_OPEN_OK:
    break;

  case LINUX_OPEN_ACCESS:
  case LINUX_OPEN_PERMISSIONS:
  case LINUX_OPEN_READ_ONLY_FILESYSTEM:
    return (WalFindSegmentIdRangeResult){
        .error = WAL_FIND_SEGMENT_ID_RANGE_DENIED,
    };

  case LINUX_OPEN_BUSY:
  case LINUX_OPEN_QUOTA:
  case LINUX_OPEN_INTERRUPT:
  case LINUX_OPEN_FILE_BUSY:
    return (WalFindSegmentIdRangeResult){
        .error = WAL_FIND_SEGMENT_ID_RANGE_TEMPORARY_FAILURE,
    };

  case LINUX_OPEN_NO_MEMORY:
    return (WalFindSegmentIdRangeResult){
        .error = WAL_FIND_SEGMENT_ID_RANGE_NO_MEMORY,
    };

  case LINUX_OPEN_NO_SPACE_ON_DEVICE:
    return (WalFindSegmentIdRangeResult){
        .error = WAL_FIND_SEGMENT_ID_RANGE_NO_SPACE,
    };

  case LINUX_OPEN_ALREADY_EXISTS:
  case LINUX_OPEN_PATH_SEG_FAULT:
  case LINUX_OPEN_PATH_FILE_TOO_BIG:
  case LINUX_OPEN_FLAGS_MISUSE:
  case LINUX_OPEN_IS_DIRECTORY:
  case LINUX_OPEN_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_OPEN_TOO_MANY_FD_OPEN:
  case LINUX_OPEN_PATH_TOO_LONG:
  case LINUX_OPEN_SYSTEM_OPEN_FILE_LIMIT:
  case LINUX_OPEN_NO_DEVICE:
  case LINUX_OPEN_NOT_FOUND:
  case LINUX_OPEN_NOT_DIRECTORY:
  case LINUX_OPEN_NO_DEVICE_OR_ADDRESS:
  case LINUX_OPEN_OPERATION_NOT_SUPPORTED:
  case LINUX_OPEN_WOULD_BLOCK:
  case LINUX_OPEN_UNKNOWN:
    assert(false);
    return (WalFindSegmentIdRangeResult){
        .error = WAL_FIND_SEGMENT_ID_RANGE_PROGRAM_ERROR,
    };
  }

  SegmentId largest = 0;
  SegmentId smallest = UINT32_MAX;

  WalFindSegmentIdRangeError error = WAL_FIND_SEGMENT_ID_RANGE_NOT_FOUND;

  byte buffer[4096];
  while (error == WAL_FIND_SEGMENT_ID_RANGE_FOUND
         || error == WAL_FIND_SEGMENT_ID_RANGE_NOT_FOUND)
  {
    LinuxGetDirectoryEntriesResult get_directories_result =
        linux_get_directory_entries(
            open_result.fd, buffer, ARRAY_LENGTH(buffer));

    switch (get_directories_result.error)
    {
    case LINUX_GET_DIRECTORY_ENTRIES_OK:
      break;

    case LINUX_GET_DIRECTORY_ENTRIES_BAD_FD:
    case LINUX_GET_DIRECTORY_ENTRIES_FAULT:
    case LINUX_GET_DIRECTORY_ENTRIES_INVALID:
    case LINUX_GET_DIRECTORY_ENTRIES_NO_ENTRY:
    case LINUX_GET_DIRECTORY_ENTRIES_NOT_A_DIRECTORY:
    case LINUX_GET_DIRECTORY_ENTRIES_UNKNOWN:
      error = WAL_FIND_SEGMENT_ID_RANGE_ENUMERATING_DIRECTORY_ENTRIES;
      break;
    }

    if (get_directories_result.error != LINUX_GET_DIRECTORY_ENTRIES_OK)
    {
      break;
    }

    if (get_directories_result.read == 0)
    {
      break;
    }

    for (struct linux_dirent64 *entry = (struct linux_dirent64 *)(buffer);
         (byte *)entry - buffer < get_directories_result.read;
         entry = (void *)entry + entry->d_reclen)
    {
      StringSliceSplitResult split_result = string_slice_split(
          string_slice_from_ptr(entry->d_name),
          string_slice_from_ptr(RESOURCE_WRITE_AHEAD_LOG_SUFFIX));
      if (!split_result.found)
      {
        continue;
      }

      UInt32FromStringSliceResult convert_result =
          uint32_from_string_slice(split_result.before);
      switch (convert_result.error)
      {
      case STRING_TO_INTEGER_OK:
        smallest = MIN(smallest, convert_result.integer);
        largest = MAX(largest, convert_result.integer);
        error = WAL_FIND_SEGMENT_ID_RANGE_FOUND;
        break;

      case STRING_TO_INTEGER_TOO_LARGE:
      case STRING_TO_INTEGER_INVALID_CHARACTER:
        error = WAL_FIND_SEGMENT_ID_RANGE_PARSING_ENTRY_NAME;
        break;
      }
    }
  }

  switch (linux_close(open_result.fd))
  {
  case LINUX_CLOSE_OK:
  case LINUX_CLOSE_BAD_FD:
  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
  case LINUX_CLOSE_UNKNOWN:
    // TODO: Log result
    break;
  }

  return (WalFindSegmentIdRangeResult){
      .error = error,
      .smallest = smallest,
      .largest = largest,
  };
}

WalNewError
wal_new(WriteAheadLog *log, StringSlice save_path, void *memory, size_t *length)
{
  assert(log != NULL);

  const size_t log_memory_length = 3 * LOG_SEGMENT_DATA_SIZE;
  assert(*length >= log_memory_length);
  *length -= log_memory_length;

  WalFindSegmentIdRangeResult range_result =
      wal_find_segment_id_range(save_path);
  switch (range_result.error)
  {
  case WAL_FIND_SEGMENT_ID_RANGE_FOUND:
  {
    SegmentId memory_segment_id = range_result.largest;

    WalSegmentHeader segment_header = {};
    WalReadSegmentResult read_result = wal_read_segment_from_disk(
        save_path,
        (LogSequenceNumber){.segment_id = memory_segment_id},
        &segment_header,
        memory,
        LOG_SEGMENT_DATA_SIZE);

    switch (read_result.error)
    {
    case WAL_READ_SEGMENT_OK:
      break;

    case WAL_READ_SEGMENT_DENIED:
    case WAL_READ_SEGMENT_TEMPORARY_FAILURE:
    case WAL_READ_SEGMENT_PROGRAM_ERROR:
    case WAL_READ_SEGMENT_NO_MEMORY:
    case WAL_READ_SEGMENT_TOO_BIG:
    case WAL_READ_SEGMENT_NOT_FOUND:
      return WAL_NEW_ERROR;
    }

    size_t bytes_read = read_result.data_read;

    // If the last entry is not present in this segment read the
    // previous segment
    if (segment_header.last_entry_offset == UNINITIALIZED_LAST_ENTRY_OFFSET)
    {
      memory_copy_forward(
          memory + LOG_SEGMENT_DATA_SIZE, memory, LOG_SEGMENT_DATA_SIZE);

      memory_segment_id -= 1;

      read_result = wal_read_segment_from_disk(
          save_path,
          (LogSequenceNumber){.segment_id = memory_segment_id},
          &segment_header,
          memory,
          LOG_SEGMENT_DATA_SIZE);

      switch (read_result.error)
      {
      case WAL_READ_SEGMENT_OK:
        break;

      case WAL_READ_SEGMENT_DENIED:
      case WAL_READ_SEGMENT_TEMPORARY_FAILURE:
      case WAL_READ_SEGMENT_PROGRAM_ERROR:
      case WAL_READ_SEGMENT_NO_MEMORY:
      case WAL_READ_SEGMENT_TOO_BIG:
      case WAL_READ_SEGMENT_NOT_FOUND:
        return WAL_NEW_ERROR;
      }

      bytes_read += read_result.data_read;

      assert(bytes_read == LOG_SEGMENT_DATA_SIZE);
    }

    assert(segment_header.last_entry_offset != UNINITIALIZED_LAST_ENTRY_OFFSET);

    WalEntry *last_entry = memory + segment_header.last_entry_offset;

    LogSequenceNumber last_lsn = (LogSequenceNumber){
        .segment_id = memory_segment_id,
        .segment_offset = segment_header.last_entry_offset,
    };

    *log = (WriteAheadLog){
        .save_path = save_path,
        .last_persisted_lsn =
            (LogSequenceNumber){
                .segment_id = range_result.largest,
                .segment_offset = segment_header.last_entry_offset,
            },
        .memory_segment_id = memory_segment_id,
        .last_entry_lsn = last_lsn,
        .next_entry_lsn = lsn_add(last_lsn, last_entry->header.entry_length),
        .memory_length = log_memory_length,
        .memory = memory,
    };
    return WAL_NEW_OK;
  }

  case WAL_FIND_SEGMENT_ID_RANGE_NOT_FOUND:
  {
    *log = (WriteAheadLog){
        .save_path = save_path,
        .last_persisted_lsn = LSN_MINIMUM,
        .memory_segment_id = LSN_MINIMUM.segment_id,
        .last_entry_lsn = LSN_MINIMUM,
        .next_entry_lsn = LSN_MINIMUM,
        .memory_length = log_memory_length,
        .memory = memory,
    };

    WalEntry first_entry = {};
    first_entry.header.entry_length = wal_entry_length(first_entry, 0, NULL);

    switch (wal_write_segment_to_memory(
        log,
        &log->next_entry_lsn,
        (ByteSlice){
            .data = &first_entry,
            .length = first_entry.header.entry_length,
        }))
    {
    case WAL_WRITE_ENTRY_OK:
      return WAL_NEW_OK;

    case WAL_WRITE_ENTRY_PROGRAM_ERROR:
    case WAL_WRITE_ENTRY_TOO_BIG:
    case WAL_WRITE_ENTRY_WRITING_SEGMENT:
    case WAL_WRITE_ENTRY_READING_SEGMENT:
      assert(false);
      return WAL_NEW_ERROR;
    }
  }

  case WAL_FIND_SEGMENT_ID_RANGE_PARSING_ENTRY_NAME:
  case WAL_FIND_SEGMENT_ID_RANGE_DENIED:
  case WAL_FIND_SEGMENT_ID_RANGE_TEMPORARY_FAILURE:
  case WAL_FIND_SEGMENT_ID_RANGE_PROGRAM_ERROR:
  case WAL_FIND_SEGMENT_ID_RANGE_NO_MEMORY:
  case WAL_FIND_SEGMENT_ID_RANGE_NO_SPACE:
  case WAL_FIND_SEGMENT_ID_RANGE_ENUMERATING_DIRECTORY_ENTRIES:
    return WAL_NEW_ERROR;
  }
}

WalWriteResult
wal_recover(WriteAheadLog *log, void *memory, size_t memory_length)
{
  assert(memory_length == 2 * LOG_SEGMENT_DATA_SIZE);

  WalEntry *last_entry = wal_last_entry(log);
  switch (last_entry->header.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
  case WAL_ENTRY_COMMIT:
    return (WalWriteResult){
        .error = WAL_WRITE_ENTRY_OK,
        .lsn = log->last_entry_lsn,
    };

  case WAL_ENTRY_ABORT:
    return wal_write_undo(log, log->last_entry_lsn, memory, memory_length);

  case WAL_ENTRY_UNDO:
    return wal_write_undo(
        log, last_entry->payload.undo.lsn, memory, memory_length);

  case WAL_ENTRY_START:
  case WAL_ENTRY_CREATE_RELATION_FILE:
  case WAL_ENTRY_DELETE_RELATION_FILE:
  case WAL_ENTRY_INSERT_TUPLE:
  case WAL_ENTRY_DELETE_TUPLE:
    return wal_abort_transaction(log, memory, memory_length);
  }
}

// ----- Write Ahead Log -----

// ----- Disk buffer pool -----

// NOTE: The disk buffer pool was defined last so that previous code does not
// access the internals of the buffers. This will be useful when the
// responsibility of flushing to disk is moved to the buffer pool

typedef enum
{
  MAPPED_BUFFER_STATUS_ALLOCATED,
  MAPPED_BUFFER_STATUS_FREE,
  MAPPED_BUFFER_STATUS_MODIFIED,
} MappedBufferStatus;

struct MappedBuffer
{
  MappedBufferStatus status;
  DiskResource resource;
  size_t handle_count;
  // TODO: Share fds between buffers, don't create an fd per buffer due to
  // limits
  int fd;
  BlockIndex block;
  // NOTE: The total length of buffers is ALWAYS PAGE_SIZE
  void *page;
};

typedef struct
{
  LogSequenceNumber last_write_lsn;
} MappedBufferHeader;

MappedBuffer mapped_buffer_free()
{
  return (MappedBuffer){
      .status = MAPPED_BUFFER_STATUS_FREE,
      // NOTE: use an invalid fd so any usage of it will result in errors, not
      //         // silently continuing
      .fd = -1,
  };
}

int16_t mapped_buffer_size()
{
  return PAGE_SIZE - sizeof(MappedBufferHeader);
}

const void *mapped_buffer_read(MappedBuffer *buffer)
{
  return buffer->page + sizeof(MappedBufferHeader);
}

void mapped_buffer_update_lsn(MappedBuffer *buffer, LogSequenceNumber lsn)
{
  assert(lsn_cmp(lsn, LSN_MINIMUM) != CMP_SMALLER);

  buffer->status = MAPPED_BUFFER_STATUS_MODIFIED;

  MappedBufferHeader *header = buffer->page;
  header->last_write_lsn = lsn;
}

void *mapped_buffer_write(MappedBuffer *buffer)
{
  // Changes to the buffer should modify the lsn first
  assert(buffer->status == MAPPED_BUFFER_STATUS_MODIFIED);
  return buffer->page + sizeof(MappedBufferHeader);
}

BlockIndex mapped_buffer_block(MappedBuffer *buffer)
{
  return buffer->block;
}

LogSequenceNumber mapped_buffer_lsn(MappedBuffer *buffer)
{
  MappedBufferHeader *header = buffer->page;
  return header->last_write_lsn;
}

bool32 mapped_buffer_close(MappedBuffer *buffer)
{
  switch (linux_close(buffer->fd))
  {
  case LINUX_CLOSE_OK:
    *buffer = mapped_buffer_free();
    return true;

  case LINUX_CLOSE_BAD_FD:
  case LINUX_CLOSE_UNKNOWN:
    assert(false);

  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
    break;
  }

  return false;
}

internal StringSlice resource_to_path(
    StringSlice save_path,
    DiskResource resource,
    char *path,
    size_t length,
    bool32 soft_deleted)
{
  size_t index = string_slice_concat(path, 0, length, save_path, false);
  index = string_slice_concat(
      path, index, length, string_slice_from_ptr("/"), false);

  switch (resource.type)
  {
  case RESOURCE_TYPE_RELATION:
  {
    size_t start_index = index;
    if (resource.id == 0)
    {
      path[index++] = '0';
    }

    index += append_uint64_as_string(resource.id, path + index, length - index);

    index = string_slice_concat(
        path,
        index,
        length,
        string_slice_from_ptr(RESOURCE_RELATION_SUFFIX),
        !soft_deleted);

    if (soft_deleted)
    {
      index = string_slice_concat(
          path,
          index,
          length,
          string_slice_from_ptr(RESOURCE_SOFT_DELETED_SUFFIX),
          true);
    }
  }
  break;
  }

  return (StringSlice){.data = path, .length = index};
}

DiskResourceCreateError disk_buffer_pool_resource_create(
    DiskBufferPool *pool, DiskResource resource, bool32 expect_new)
{
  assert(pool != NULL);

  // TODO: Use arena
  char path[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, path, ARRAY_LENGTH(path), false);

  LinuxOpenResult open_result = linux_open(
      path,
      (expect_new ? O_EXCL : 0) | O_CREAT | O_WRONLY,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

  switch (open_result.error)
  {
  case LINUX_OPEN_OK:
    break;

  case LINUX_OPEN_ACCESS:
  case LINUX_OPEN_BUSY:
  case LINUX_OPEN_QUOTA:
  case LINUX_OPEN_INTERRUPT:
  case LINUX_OPEN_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_OPEN_TOO_MANY_FD_OPEN:
  case LINUX_OPEN_SYSTEM_OPEN_FILE_LIMIT:
  case LINUX_OPEN_NO_DEVICE:
  case LINUX_OPEN_NO_MEMORY:
  case LINUX_OPEN_NO_SPACE_ON_DEVICE:
  case LINUX_OPEN_NOT_DIRECTORY:
  case LINUX_OPEN_NO_DEVICE_OR_ADDRESS:
  case LINUX_OPEN_OPERATION_NOT_SUPPORTED:
  case LINUX_OPEN_PERMISSIONS:
  case LINUX_OPEN_READ_ONLY_FILESYSTEM:
  case LINUX_OPEN_FILE_BUSY:
  case LINUX_OPEN_WOULD_BLOCK:
    return DISK_RESOURCE_CREATE_OPENING;

  case LINUX_OPEN_ALREADY_EXISTS:
    return expect_new ? DISK_RESOURCE_CREATE_ALREADY_EXISTS
                      : DISK_RESOURCE_CREATE_OK;

  case LINUX_OPEN_PATH_SEG_FAULT:
  case LINUX_OPEN_PATH_FILE_TOO_BIG:
  case LINUX_OPEN_FLAGS_MISUSE:
  case LINUX_OPEN_IS_DIRECTORY:
  case LINUX_OPEN_PATH_TOO_LONG:
  case LINUX_OPEN_NOT_FOUND:
  case LINUX_OPEN_UNKNOWN:
    return DISK_RESOURCE_CREATE_PROGRAM_ERROR;
  }

  if (!expect_new)
  {
    LinuxFStatResult result = linux_fstat(open_result.fd);
    if (result.error != LINUX_FSTAT_OK)
    {
      linux_close(open_result.fd);
      return DISK_RESOURCE_CREATE_STAT;
    }

    if (result.stat.st_size > 0)
    {
      linux_close(open_result.fd);
      return DISK_RESOURCE_CREATE_OK;
    }
  }

  switch (linux_ftruncate(open_result.fd, 0))
  {
  case LINUX_FTRUNCATE_OK:
    break;

  case LINUX_FTRUNCATE_ACCESS:
  case LINUX_FTRUNCATE_INTERRUPT:
  case LINUX_FTRUNCATE_IO:
  case LINUX_FTRUNCATE_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_FTRUNCATE_PERMISSIONS:
  case LINUX_FTRUNCATE_READ_ONLY_FILESYSTEM:
  case LINUX_FTRUNCATE_BUSY:
    return DISK_RESOURCE_CREATE_TRUNCATING;

  case LINUX_FTRUNCATE_PATH_SEG_FAULT:
  case LINUX_FTRUNCATE_LENGTH_INVALID:
  case LINUX_FTRUNCATE_LENGTH_OR_FD_INVALID:
  case LINUX_FTRUNCATE_IS_DIRECTORY:
  case LINUX_FTRUNCATE_PATH_TOO_LONG:
  case LINUX_FTRUNCATE_FILE_NOT_FOUND:
  case LINUX_FTRUNCATE_INVALID_PATH:
  case LINUX_FTRUNCATE_BAD_FD:
    return DISK_RESOURCE_CREATE_PROGRAM_ERROR;

  case LINUX_FTRUNCATE_UNKNOWN:
    break;
  }

  switch (linux_close(open_result.fd))
  {
  case LINUX_CLOSE_OK:
    return DISK_RESOURCE_CREATE_OK;

  case LINUX_CLOSE_BAD_FD:
    return DISK_RESOURCE_CREATE_PROGRAM_ERROR;

  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
  case LINUX_CLOSE_UNKNOWN:
    return DISK_RESOURCE_CREATE_CLOSING;
  }
}

DiskResourceSoftDeleteError disk_buffer_pool_resource_soft_delete(
    DiskBufferPool *pool, DiskResource resource)
{
  // TODO: Use arena
  char from[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, from, ARRAY_LENGTH(from), false);

  // TODO: Use arena
  char to[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, to, ARRAY_LENGTH(to), true);

  switch (linux_rename(from, to))
  {
  case LINUX_RENAME_OK:
    return DISK_RESOURCE_SOFT_DELETE_OK;

  case LINUX_RENAME_BUSY:
  case LINUX_RENAME_QUOTA:
    return DISK_RESOURCE_SOFT_DELETE_TEMPORARAY_FAILURE;

  case LINUX_RENAME_NO_SPACE:
    return DISK_RESOURCE_SOFT_DELETE_DISK_FULL;

  case LINUX_RENAME_NO_MEMORY:
    return DISK_RESOURCE_SOFT_DELETE_NO_MEMORY;

  case LINUX_RENAME_ACCESS:
  case LINUX_RENAME_READ_ONLY_FILESYSTEM:
    return DISK_RESOURCE_SOFT_DELETE_DENIED;

  case LINUX_RENAME_LINK:
  case LINUX_RENAME_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_RENAME_INVALID:
  case LINUX_RENAME_IS_DIRECTORY:
  case LINUX_RENAME_NO_ENTRY:
  case LINUX_RENAME_UNKNOWN:
  case LINUX_RENAME_DIFFERENT_FILESYSTEM:
  case LINUX_RENAME_NOT_DIRECTORY:
  case LINUX_RENAME_NAME_TOO_LONG:
  case LINUX_RENAME_EXISTS:
  case LINUX_RENAME_FAULT:
    assert(false);
    return DISK_RESOURCE_SOFT_DELETE_PROGRAM_ERROR;
  }
}

DiskResourceDeleteError disk_buffer_pool_resource_delete(
    DiskBufferPool *pool, DiskResource resource, bool32 deleted)
{
  // TODO: Use arena
  char path[LINUX_PATH_MAX] = {};
  resource_to_path(
      pool->save_path, resource, path, ARRAY_LENGTH(path), deleted);

  switch (linux_unlink(path))
  {
  case LINUX_UNLINK_OK:
    return DISK_RESOURCE_DELETE_OK;

  case LINUX_UNLINK_BUSY:
  case LINUX_UNLINK_IO:
    return DISK_RESOURCE_DELETE_TEMPORARAY_FAILURE;

  case LINUX_UNLINK_NO_MEMORY:
    return DISK_RESOURCE_DELETE_NO_MEMORY;

  case LINUX_UNLINK_ACCESS:
  case LINUX_UNLINK_PERMISSIONS:
  case LINUX_UNLINK_READ_ONLY_FILESYSTEM:
    return DISK_RESOURCE_DELETE_DENIED;

  case LINUX_UNLINK_NAME_TOO_LONG:
  case LINUX_UNLINK_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_UNLINK_IS_DIRECTORY:
  case LINUX_UNLINK_FILE_NOT_FOUND:
  case LINUX_UNLINK_INVALID_FLAGS:
  case LINUX_UNLINK_NOT_DIRECTORY:
  case LINUX_UNLINK_BAD_FD:
  case LINUX_UNLINK_PATH_SEG_FAULT:
  case LINUX_UNLINK_UNKNOWN:
    assert(false);
    return DISK_RESOURCE_DELETE_PROGRAM_ERROR;
  }
}

DiskResourceRestoreError
disk_buffer_pool_resource_restore(DiskBufferPool *pool, DiskResource resource)
{
  // TODO: Use arena
  char from[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, from, ARRAY_LENGTH(from), true);

  // TODO: Use arena
  char to[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, to, ARRAY_LENGTH(to), false);

  switch (linux_rename(from, to))
  {
  case LINUX_RENAME_OK:
    return DISK_RESOURCE_RESTORE_OK;

  case LINUX_RENAME_BUSY:
  case LINUX_RENAME_QUOTA:
    return DISK_RESOURCE_RESTORE_TEMPORARAY_FAILURE;

  case LINUX_RENAME_NO_SPACE:
    return DISK_RESOURCE_RESTORE_DISK_FULL;

  case LINUX_RENAME_NO_MEMORY:
    return DISK_RESOURCE_RESTORE_NO_MEMORY;

  case LINUX_RENAME_ACCESS:
  case LINUX_RENAME_READ_ONLY_FILESYSTEM:
    return DISK_RESOURCE_RESTORE_DENIED;

  case LINUX_RENAME_LINK:
  case LINUX_RENAME_TOO_MANY_SYMBOLIC_LINKS:
  case LINUX_RENAME_INVALID:
  case LINUX_RENAME_IS_DIRECTORY:
  case LINUX_RENAME_NO_ENTRY:
  case LINUX_RENAME_UNKNOWN:
  case LINUX_RENAME_DIFFERENT_FILESYSTEM:
  case LINUX_RENAME_NOT_DIRECTORY:
  case LINUX_RENAME_NAME_TOO_LONG:
  case LINUX_RENAME_EXISTS:
  case LINUX_RENAME_FAULT:
    assert(false);
    return DISK_RESOURCE_RESTORE_PROGRAM_ERROR;
  }
}

void disk_buffer_pool_new(
    DiskBufferPool *pool, StringSlice path, void *data, size_t length)
{
  assert(pool != NULL);
  assert(data != NULL);

  size_t buffers_length = length / (PAGE_SIZE + sizeof(MappedBuffer));
  assert(length == buffers_length * (PAGE_SIZE + sizeof(MappedBuffer)));
  assert(length > 0);

  *pool = (DiskBufferPool){
      .save_path = {},
      .buffers_length = buffers_length,
      .buffers = data,
      .buffer_pages = data + (buffers_length * sizeof(MappedBuffer)),
  };

  memory_copy_forward(pool->save_path_buffer, path.data, path.length);

  pool->save_path = (StringSlice){
      .data = pool->save_path_buffer,
      .length = path.length,
  };

  for (size_t i = 0; i < pool->buffers_length; ++i)
  {
    pool->buffers[i] = mapped_buffer_free();
  }
}

internal void disk_buffer_pool_close(DiskBufferPool *pool, size_t buffer_index)
{
  MappedBuffer *buffer = pool->buffers + buffer_index;
  assert(buffer->handle_count > 0);

  buffer->handle_count -= 1;

  switch (buffer->status)
  {
  case MAPPED_BUFFER_STATUS_FREE:
    assert(false);

  case MAPPED_BUFFER_STATUS_ALLOCATED:
    // We still let the buffer exist in case another query needs it in the
    // future
  case MAPPED_BUFFER_STATUS_MODIFIED:
    break;
  }
}

internal MappedBuffer *mapped_buffer(DiskBufferPool *pool, size_t buffer_index)
{
  assert(buffer_index < pool->buffers_length);
  MappedBuffer *buffer = pool->buffers + buffer_index;
  return buffer;
}

DiskBufferPoolSaveError
disk_buffer_pool_save(DiskBufferPool *pool, WriteAheadLog *log)
{
  LogSequenceNumber smallest_unsaved_lsn = (LogSequenceNumber){
      .segment_id = UINT32_MAX,
      .segment_offset = 0,
  };

  for (size_t i = 0; i < pool->buffers_length; ++i)
  {
    MappedBuffer *buffer = mapped_buffer(pool, i);

    DiskBufferPoolSaveError error = DISK_BUFFER_POOL_SAVE_OK;
    switch (buffer->status)
    {
    case MAPPED_BUFFER_STATUS_FREE:
    case MAPPED_BUFFER_STATUS_ALLOCATED:
      continue;

    case MAPPED_BUFFER_STATUS_MODIFIED:
    {
      LogSequenceNumber buffer_lsn = mapped_buffer_lsn(buffer);
      switch (lsn_cmp(mapped_buffer_lsn(buffer), log->last_persisted_lsn))
      {
      case CMP_SMALLER:
      case CMP_EQUAL:
        break;

      case CMP_GREATER:
        // If a page has an lsn that currently resides in log memory and
        // still not persisted to disk we also skip persisting the page
        // to disk, otherwise it will contain changes outside of the
        // WriteAheadLog
        continue;
      }

      switch (linux_seek(buffer->fd, PAGE_SIZE * buffer->block, SEEK_SET).error)
      {
      case LINUX_SEEK_OK:
        break;

      case LINUX_SEEK_BAD_FD:
      case LINUX_SEEK_WHENCE_INVALID:
      case LINUX_SEEK_INVALID_OFFSET:
      case LINUX_SEEK_FILE_OFFSET_TOO_BIG:
      case LINUX_SEEK_NOT_A_FILE:
      case LINUX_SEEK_UNKNOWN:
        // TODO: Log failures
        assert(false);
        return DISK_BUFFER_POOL_SAVE_PROGRAM_ERROR;
      }

      LinuxWriteResult write_result =
          linux_write(buffer->fd, buffer->page, PAGE_SIZE);

      switch (write_result.error)
      {
      case LINUX_WRITE_OK:
        assert(write_result.count == PAGE_SIZE);
        break;

      case LINUX_WRITE_WOULD_BLOCK:
      case LINUX_WRITE_QUOTA:
      case LINUX_WRITE_INTERRUPT:
      case LINUX_WRITE_IO:
        error = DISK_BUFFER_POOL_SAVE_TEMPORARAY_FAILURE;
        break;

      case LINUX_WRITE_PERMISSIONS:
        return DISK_BUFFER_POOL_SAVE_DENIED;

      case LINUX_WRITE_NO_SPACE:
        return DISK_BUFFER_POOL_SAVE_NO_SPACE;

      case LINUX_WRITE_INVALID:
      case LINUX_WRITE_UNKNOWN:
      case LINUX_WRITE_PIPE_CLOSED:
      case LINUX_WRITE_LENGTH_TOO_BIG:
      case LINUX_WRITE_INVALID_PEER_ADDRESS:
      case LINUX_WRITE_BUFFER_SEG_FAULT:
      case LINUX_WRITE_BAD_FD:
        return DISK_BUFFER_POOL_SAVE_PROGRAM_ERROR;
      }

      switch (linux_fdatasync(buffer->fd))
      {
      case LINUX_FDATASYNC_OK:
        break;

      case LINUX_FDATASYNC_IO:
      case LINUX_FDATASYNC_INTERRUPT:
      case LINUX_FDATASYNC_QUOTA:
        error = DISK_BUFFER_POOL_SAVE_TEMPORARAY_FAILURE;
        break;

      case LINUX_FDATASYNC_NO_SPACE:
        return DISK_BUFFER_POOL_SAVE_NO_SPACE;

      case LINUX_FDATASYNC_READ_ONLY_FILESYSTEM:
        return DISK_BUFFER_POOL_SAVE_DENIED;

      case LINUX_FDATASYNC_FD_NO_SYNC_SUPPORT:
      case LINUX_FDATASYNC_BAD_FD:
      case LINUX_FDATASYNC_UNKNOWN:
        assert(false);
        return DISK_BUFFER_POOL_SAVE_PROGRAM_ERROR;
      }

      buffer->status = MAPPED_BUFFER_STATUS_ALLOCATED;
    }
    break;
    }

    if (error != DISK_BUFFER_POOL_SAVE_OK)
    {
      if (lsn_cmp(smallest_unsaved_lsn, mapped_buffer_lsn(buffer))
          == CMP_GREATER)
      {
        smallest_unsaved_lsn = mapped_buffer_lsn(buffer);
      }
    }
  }

  if (smallest_unsaved_lsn.segment_id != UINT32_MAX)
  {
    WalFindSegmentIdRangeResult range_result =
        wal_find_segment_id_range(pool->save_path);

    switch (range_result.error)
    {
    case WAL_FIND_SEGMENT_ID_RANGE_FOUND:
      break;

    case WAL_FIND_SEGMENT_ID_RANGE_NOT_FOUND:
    case WAL_FIND_SEGMENT_ID_RANGE_DENIED:
    case WAL_FIND_SEGMENT_ID_RANGE_TEMPORARY_FAILURE:
    case WAL_FIND_SEGMENT_ID_RANGE_PROGRAM_ERROR:
    case WAL_FIND_SEGMENT_ID_RANGE_NO_MEMORY:
    case WAL_FIND_SEGMENT_ID_RANGE_NO_SPACE:
    case WAL_FIND_SEGMENT_ID_RANGE_ENUMERATING_DIRECTORY_ENTRIES:
    case WAL_FIND_SEGMENT_ID_RANGE_PARSING_ENTRY_NAME:
      assert(false);
      return DISK_BUFFER_POOL_SAVE_IO;
    }

    for (SegmentId i = range_result.smallest;
         i < log->last_persisted_lsn.segment_id
         && i < smallest_unsaved_lsn.segment_id;
         ++i)
    {
      char path[LINUX_PATH_MAX];
      wal_segment_id_to_path(
          pool->save_path, range_result.smallest, path, LINUX_PATH_MAX);

      switch (linux_unlink(path))
      {
      case LINUX_UNLINK_OK:
        break;

      case LINUX_UNLINK_BUSY:
      case LINUX_UNLINK_IO:
        return DISK_BUFFER_POOL_SAVE_TEMPORARAY_FAILURE;

      case LINUX_UNLINK_ACCESS:
      case LINUX_UNLINK_READ_ONLY_FILESYSTEM:
      case LINUX_UNLINK_PERMISSIONS:
        return DISK_BUFFER_POOL_SAVE_DENIED;

      case LINUX_UNLINK_NO_MEMORY:
        return DISK_BUFFER_POOL_SAVE_NO_MEMORY;

      case LINUX_UNLINK_NAME_TOO_LONG:
      case LINUX_UNLINK_TOO_MANY_SYMBOLIC_LINKS:
      case LINUX_UNLINK_PATH_SEG_FAULT:
      case LINUX_UNLINK_IS_DIRECTORY:
      case LINUX_UNLINK_FILE_NOT_FOUND:
      case LINUX_UNLINK_INVALID_FLAGS:
      case LINUX_UNLINK_NOT_DIRECTORY:
      case LINUX_UNLINK_BAD_FD:
      case LINUX_UNLINK_UNKNOWN:
        assert(false);
        return DISK_BUFFER_POOL_SAVE_PROGRAM_ERROR;
      }
    }
  }

  return DISK_BUFFER_POOL_SAVE_OK;
}

typedef struct
{
  size_t index;
  bool32 found;
} DiskBufferPoolFindFreeBufferResult;

internal DiskBufferPoolFindFreeBufferResult
disk_buffer_pool_find_free_buffer(DiskBufferPool *pool)
{
  for (size_t i = 0; i < pool->buffers_length; ++i)
  {
    switch (pool->buffers[i].status)
    {
    case MAPPED_BUFFER_STATUS_FREE:
      return (DiskBufferPoolFindFreeBufferResult){
          .index = i,
          .found = true,
      };

    case MAPPED_BUFFER_STATUS_ALLOCATED:
    case MAPPED_BUFFER_STATUS_MODIFIED:
      continue;
    }
  }

  for (size_t i = 0; i < pool->buffers_length; ++i)
  {
    MappedBuffer *buffer = pool->buffers + i;
    switch (buffer->status)
    {
    case MAPPED_BUFFER_STATUS_FREE:
    case MAPPED_BUFFER_STATUS_MODIFIED:
      continue;

    case MAPPED_BUFFER_STATUS_ALLOCATED:
    {
      if (mapped_buffer_close(buffer))
      {
        return (DiskBufferPoolFindFreeBufferResult){
            .index = i,
            .found = true,
        };
      }
    }
    break;
    }
  }

  return (DiskBufferPoolFindFreeBufferResult){
      .index = 0,
      .found = false,
  };
}

// TODO: Test cleanup
internal DiskBufferPoolOpenResult disk_buffer_pool_open(
    DiskBufferPool *pool, DiskResource resource, BlockIndex block)
{
  for (size_t i = 0; i < pool->buffers_length; ++i)
  {
    MappedBuffer *buffer = pool->buffers + i;
    switch (buffer->status)
    {
    case MAPPED_BUFFER_STATUS_FREE:
      continue;
    case MAPPED_BUFFER_STATUS_ALLOCATED:
    case MAPPED_BUFFER_STATUS_MODIFIED:
      break;
    }

    if (buffer->block == block
        && disk_resource_eq(&buffer->resource, &resource))
    {
      buffer->handle_count += 1;
      return (DiskBufferPoolOpenResult){
          .buffer_index = i,
          .error = DISK_BUFFER_POOL_OPEN_OK,
      };
    }
  }

  DiskBufferPoolFindFreeBufferResult free_buffer_result =
      disk_buffer_pool_find_free_buffer(pool);

  if (!free_buffer_result.found)
  {
    return (DiskBufferPoolOpenResult){
        .error = DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL};
  }

  void *page = &pool->buffer_pages[PAGE_SIZE * free_buffer_result.index];

  int fd = 0;
  {
    // TODO: Use arena
    char path[LINUX_PATH_MAX] = {};
    resource_to_path(
        pool->save_path, resource, path, ARRAY_LENGTH(path), false);

    LinuxOpenResult open_result =
        linux_open(path, LINUX_OPEN_DIRECT | LINUX_OPEN_READ_WRITE, 0);

    if (open_result.error != LINUX_OPEN_OK)
    {
      return (DiskBufferPoolOpenResult){.error =
                                            DISK_BUFFER_POOL_OPEN_OPENING_FILE};
    }
    fd = open_result.fd;
  }

  if (linux_seek(fd, PAGE_SIZE * block, LINUX_SEEK_SET).error != LINUX_SEEK_OK)
  {
    assert(linux_close(fd) == LINUX_CLOSE_OK);
    return (DiskBufferPoolOpenResult){.error =
                                          DISK_BUFFER_POOL_OPEN_SEEKING_FILE};
  }

  LinuxReadResult read_result = linux_read(fd, page, PAGE_SIZE);
  if (read_result.error != LINUX_READ_OK)
  {
    assert(linux_close(fd) == LINUX_CLOSE_OK);
    return (DiskBufferPoolOpenResult){.error =
                                          DISK_BUFFER_POOL_OPEN_READING_FILE};
  }

  if (read_result.count != PAGE_SIZE)
  {
    assert(linux_close(fd) == LINUX_CLOSE_OK);
    return (DiskBufferPoolOpenResult){.error =
                                          DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL};
  }

  pool->buffers[free_buffer_result.index] = (MappedBuffer){
      .status = MAPPED_BUFFER_STATUS_ALLOCATED,
      .handle_count = 1,
      .resource = resource,
      .fd = fd,
      .block = block,
      .page = page,
  };

  return (DiskBufferPoolOpenResult){
      .buffer_index = free_buffer_result.index,
      .error = DISK_BUFFER_POOL_OPEN_OK,
  };
}

// TODO: Pass the next block, we often know it
internal DiskBufferPoolNewBlockOpenResult
disk_buffer_pool_new_block_open(DiskBufferPool *pool, DiskResource resource)
{
  DiskBufferPoolFindFreeBufferResult free_buffer_result =
      disk_buffer_pool_find_free_buffer(pool);

  if (!free_buffer_result.found)
  {
    return (DiskBufferPoolNewBlockOpenResult){
        .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_BUFFER_POOL_FULL};
  }

  void *page = &pool->buffer_pages[PAGE_SIZE * free_buffer_result.index];

  int fd = 0;
  {
    // TODO: Use arena
    char path[LINUX_PATH_MAX] = {};
    resource_to_path(
        pool->save_path, resource, path, ARRAY_LENGTH(path), false);

    LinuxOpenResult open_result =
        linux_open(path, LINUX_OPEN_DIRECT | LINUX_OPEN_READ_WRITE, 0);
    if (open_result.error != LINUX_OPEN_OK)
    {
      return (DiskBufferPoolNewBlockOpenResult){
          .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OPENING_FILE};
    }
    fd = open_result.fd;
  }

  LinuxFStatResult result = linux_fstat(fd);
  if (result.error != LINUX_FSTAT_OK)
  {
    return (DiskBufferPoolNewBlockOpenResult){
        .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READ_FILE_SIZE};
  }

  assert((result.stat.st_size % PAGE_SIZE) == 0);

  size_t block = (result.stat.st_size / PAGE_SIZE);
  if (linux_ftruncate(fd, PAGE_SIZE * (block + 1)) != LINUX_FTRUNCATE_OK)
  {
    return (DiskBufferPoolNewBlockOpenResult){
        .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE};
  }

  if (linux_seek(fd, PAGE_SIZE * block, LINUX_SEEK_SET).error != LINUX_SEEK_OK)
  {
    assert(linux_close(fd) == LINUX_CLOSE_OK);
    return (DiskBufferPoolNewBlockOpenResult){
        .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_SEEKING_FILE};
  }

  LinuxReadResult read_result = linux_read(fd, page, PAGE_SIZE);
  if (read_result.error != LINUX_READ_OK)
  {
    assert(linux_close(fd) == LINUX_CLOSE_OK);
    return (DiskBufferPoolNewBlockOpenResult){
        .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READING_FILE};
  }

  assert(read_result.count == PAGE_SIZE);

  pool->buffers[free_buffer_result.index] = (MappedBuffer){
      .status = MAPPED_BUFFER_STATUS_ALLOCATED,
      .resource = resource,
      .handle_count = 1,
      .fd = fd,
      .block = block,
      .page = page,
  };

  return (DiskBufferPoolNewBlockOpenResult){
      .buffer_index = free_buffer_result.index,
      .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK,
  };
}

MappedBuffer *
disk_buffer_pool_mapped_buffer(DiskBufferPool *pool, size_t buffer_index)
{
  MappedBuffer *buffer = mapped_buffer(pool, buffer_index);
  assert(buffer->handle_count > 0);
  assert(buffer->status != MAPPED_BUFFER_STATUS_FREE);
  return buffer;
}

// ----- Disk buffer pool -----
