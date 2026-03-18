#include "physical.h"

#define PAGE_SIZE (size_t)KIBIBYTES(8)

// ----- Disk buffer pool -----

typedef enum
{
  RESOURCE_TYPE_RELATION,
} DiskResourceType;

typedef struct
{
  DiskResourceType type;
  union
  {
    RelationId relation_id;
  };
} DiskResource;

bool32 disk_resource_eq(DiskResource *a, DiskResource *b)
{
  if (a->type != b->type)
  {
    return false;
  }

  switch (a->type)
  {
  case RESOURCE_TYPE_RELATION:
    return a->relation_id == b->relation_id;
  }
}

internal DiskResourceCreate disk_buffer_pool_resource_create(
    DiskBufferPool *pool, DiskResource resource, bool32 expect_new);

internal void
disk_buffer_pool_resource_delete(DiskBufferPool *pool, DiskResource resource);

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

typedef enum
{
  DISK_BUFFER_POOL_SAVE_OK,
  DISK_BUFFER_POOL_SAVE_SEEKING_FILE,
  DISK_BUFFER_POOL_SAVE_WRITING_FILE,
  DISK_BUFFER_POOL_SAVE_SYNC,
} DiskBufferPoolSaveError;

internal DiskBufferPoolSaveError
disk_buffer_pool_save_buffer(DiskBufferPool *pool, size_t buffer_index);

MappedBuffer *
disk_buffer_pool_mapped_buffer(DiskBufferPool *pool, size_t buffer_index);

const void *mapped_buffer_read(MappedBuffer *buffer);

void *mapped_buffer_write(MappedBuffer *buffer);

BlockIndex mapped_buffer_block(MappedBuffer *buffer);

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
      assert(string.length < PAGE_SIZE); // TODO
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

internal TupleHeader *relation_tuple_fixed_data_write(
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

DiskResourceCreate
physical_relation_create(DiskBufferPool *pool, RelationId id, bool32 expect_new)
{
  assert(pool != NULL);
  return disk_buffer_pool_resource_create(
      pool,
      (DiskResource){.type = RESOURCE_TYPE_RELATION, .relation_id = id},
      expect_new);
}

void physical_relation_delete(DiskBufferPool *pool, RelationId id)
{
  disk_buffer_pool_resource_delete(
      pool, (DiskResource){.type = RESOURCE_TYPE_RELATION, .relation_id = id});
}

internal void overwrite_last_deleted_tuple_with_valid_tuple(
    PhysicalRelationIterator *it, int16_t tuple_index)
{
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);
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
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

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
  if (it->status != PHYSICAL_RELATION_ITERATOR_STATUS_OK)
  {
    return;
  }

  overwrite_deleted_tuples(it);
  disk_buffer_pool_close(it->pool, it->buffer_index);
}

PhysicalRelationIterator physical_relation_iterate_blocks(
    DiskBufferPool *pool,
    RelationId id,
    ColumnsLength tuple_length,
    const ColumnType *types)
{
  assert(pool != NULL);
  assert(types != NULL);
  assert(tuple_length > 0);

  int16_t tuple_fixed_size = tuple_fixed_length(tuple_length, types);

  DiskBufferPoolOpenResult result = disk_buffer_pool_open(
      pool,
      (DiskResource){.type = RESOURCE_TYPE_RELATION, .relation_id = id},
      0);

  switch (result.error)
  {
  case DISK_BUFFER_POOL_OPEN_OK:
    return (PhysicalRelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = result.buffer_index,
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_OK,

        .tuple_length = tuple_length,
        .types = types,
        .tuple_fixed_size = tuple_fixed_size,

        .deleted_records = 0,
        .deleted_variable_data = 0,
    };

  case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
    return (PhysicalRelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = {},
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS,

        .tuple_length = tuple_length,
        .types = types,
        .tuple_fixed_size = tuple_fixed_size,

        .deleted_records = 0,
        .deleted_variable_data = 0,
    };

  case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
  case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
  case DISK_BUFFER_POOL_OPEN_READING_FILE:
  case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
    return (PhysicalRelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = {},
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE,

        .tuple_length = tuple_length,
        .types = types,
        .tuple_fixed_size = tuple_fixed_size,

        .deleted_records = 0,
        .deleted_variable_data = 0,
    };
  }
}

PhysicalRelationIterator physical_relation_iterate_tuples(
    DiskBufferPool *pool,
    RelationId id,
    ColumnsLength tuple_length,
    const ColumnType *types)
{
  PhysicalRelationIterator it =
      physical_relation_iterate_blocks(pool, id, tuple_length, types);

  if (it.status != PHYSICAL_RELATION_ITERATOR_STATUS_OK)
  {
    return it;
  }

  if (physical_relation_iterator_is_block_empty(&it))
  {
    physical_relation_iterator_next_tuple(&it);
  }

  return it;
}

void physical_relation_iterator_next_block(PhysicalRelationIterator *it)
{
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

  int16_t block = mapped_buffer_block(
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index));
  close_buffer_if_open(it);

  DiskBufferPoolOpenResult result = disk_buffer_pool_open(
      it->pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .relation_id = it->relation_id,
      },
      block + 1);

  switch (result.error)
  {
  case DISK_BUFFER_POOL_OPEN_OK:
    it->buffer_index = result.buffer_index;
    it->tuple_index = 0;
    break;

  case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS;
    break;

  case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
  case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
  case DISK_BUFFER_POOL_OPEN_READING_FILE:
  case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE;
    break;
  }
}

void physical_relation_iterator_new_block(PhysicalRelationIterator *it)
{
  close_buffer_if_open(it);

  DiskBufferPoolNewBlockOpenResult result = disk_buffer_pool_new_block_open(
      it->pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .relation_id = it->relation_id,
      });

  switch (result.error)
  {
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK:
  {
    MappedBuffer *buffer =
        disk_buffer_pool_mapped_buffer(it->pool, result.buffer_index);

    *relation_header_write(buffer) = (RelationHeader){
        .allocated_records = 0,
        .variable_data_start = PAGE_SIZE,
    };

    it->buffer_index = result.buffer_index;
    it->tuple_index = 0;
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_OK;
  }
  break;

  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_BUFFER_POOL_FULL:
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL;
    break;

  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OPENING_FILE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READ_FILE_SIZE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_SEEKING_FILE:
  case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READING_FILE:
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_IO;
    break;
  }
}

void physical_relation_iterator_next_tuple(PhysicalRelationIterator *it)
{
  assert(it != NULL);
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);

  it->tuple_index += 1;
  if (it->tuple_index < relation_header_read(buffer)->allocated_records)
  {
    overwrite_last_deleted_tuple_with_valid_tuple(it, it->tuple_index);
    return;
  }

  do
  {
    physical_relation_iterator_next_block(it);
  } while (it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
           && physical_relation_iterator_is_block_empty(it));
}

Tuple physical_relation_iterator_get(PhysicalRelationIterator *it)
{
  assert(it != NULL);
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

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
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);
  return relation_header_read(buffer)->allocated_records == 0;
}

PhysicalRelationInsertTupleError
physical_relation_iterator_insert(PhysicalRelationIterator *it, Tuple tuple)
{
  assert(it != NULL);
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

  const int16_t tuple_variable_byte_length =
      tuple_variable_length(tuple.length, tuple.types, tuple.fixed_data);

  const size_t required_space =
      relation_tuple_size(it->tuple_fixed_size)
      + tuple_variable_length(tuple.length, tuple.types, tuple.fixed_data);

  if (required_space > relation_free_space(
          disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index),
          it->tuple_fixed_size))
  {
    return PHYSICAL_RELATION_INSERT_TUPLE_TOO_BIG;
  }

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);
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

  return PHYSICAL_RELATION_INSERT_TUPLE_OK;
}

void physical_relation_iterator_delete(PhysicalRelationIterator *it)
{
  assert(it != NULL);
  assert(it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK);

  it->deleted_records += 1;
  it->deleted_variable_data += tuple_variable_length(
      it->tuple_length,
      it->types,
      physical_relation_iterator_get(it).fixed_data);
}

void physical_relation_iterator_close(PhysicalRelationIterator *it)
{
  assert(it != NULL);

  close_buffer_if_open(it);
}

// ----- Relation -----

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

// TODO: Support having a single buffer opened multiple times, this reduces
// memory usage when multiple buffers read the same block from the same relation
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

MappedBuffer mapped_buffer_free()
{
  return (MappedBuffer){
      .status = MAPPED_BUFFER_STATUS_FREE,
      // NOTE: use an invalid fd so any usage of it will result in errors, not
      //         // silently continuing
      .fd = -1,
  };
}

const void *mapped_buffer_read(MappedBuffer *buffer)
{
  return buffer->page;
}

void *mapped_buffer_write(MappedBuffer *buffer)
{
  buffer->status = MAPPED_BUFFER_STATUS_MODIFIED;
  return buffer->page;
}

BlockIndex mapped_buffer_block(MappedBuffer *buffer)
{
  return buffer->block;
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
    StringSlice save_path, DiskResource resource, char *path, size_t length)
{
  size_t index = string_slice_concat(path, 0, length, save_path, false);
  index = string_slice_concat(
      path, index, length, string_slice_from_ptr("/"), false);

  switch (resource.type)
  {
  case RESOURCE_TYPE_RELATION:
  {
    size_t start_index = index;
    if (resource.relation_id == 0)
    {
      path[index++] = '0';
    }

    // TODO: Simplify
    const RelationId base = 10;
    for (RelationId id = resource.relation_id; id > 0; id /= base, ++index)
    {
      path[index] = (char)('0' + (id % base));
    }
    size_t end_index = index - 1;

    while (start_index < end_index)
    {
      char temp = path[start_index];
      path[start_index] = path[end_index];
      path[end_index] = temp;
      start_index += 1;
      end_index -= 1;
    }

    index = string_slice_concat(
        path, index, length, string_slice_from_ptr(".relation"), true);
  }
  break;
  }

  return (StringSlice){.data = path, .length = index};
}

internal DiskResourceCreate disk_buffer_pool_resource_create(
    DiskBufferPool *pool, DiskResource resource, bool32 expect_new)
{
  assert(pool != NULL);

  // TODO: Use arena
  char path[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, path, ARRAY_LENGTH(path));

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

internal void
disk_buffer_pool_resource_delete(DiskBufferPool *pool, DiskResource resource)
{
  // TODO: Use arena
  char path[LINUX_PATH_MAX] = {};
  resource_to_path(pool->save_path, resource, path, ARRAY_LENGTH(path));

  LinuxUnlinkError error = linux_unlink(path);
  // FIXME: Log any failures
}

void disk_buffer_pool_new(
    DiskBufferPool *pool, StringSlice path, void *data, size_t length)
{
  assert(pool != NULL);
  assert(data != NULL);

  size_t buffers_length = length / (PAGE_SIZE + sizeof(MappedBuffer));
  assert(length == buffers_length * (PAGE_SIZE + sizeof(MappedBuffer)));

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

internal DiskBufferPoolSaveError
disk_buffer_pool_save_buffer(DiskBufferPool *pool, size_t buffer_index)
{
  MappedBuffer *buffer = pool->buffers + buffer_index;
  switch (buffer->status)
  {
  case MAPPED_BUFFER_STATUS_FREE:
  case MAPPED_BUFFER_STATUS_ALLOCATED:
    assert(false);
    return DISK_BUFFER_POOL_SAVE_OK;

  case MAPPED_BUFFER_STATUS_MODIFIED:
    break;
  }

  if (linux_seek(buffer->fd, PAGE_SIZE * buffer->block, SEEK_SET).error
      != LINUX_SEEK_OK)
  {
    return DISK_BUFFER_POOL_SAVE_SEEKING_FILE;
  }

  LinuxWriteResult write_result =
      linux_write(buffer->fd, buffer->page, PAGE_SIZE);

  if (write_result.error == LINUX_WRITE_OK)
  {
    assert(write_result.count == PAGE_SIZE);
  }
  else
  {
    return DISK_BUFFER_POOL_SAVE_WRITING_FILE;
  }

  if (linux_fdatasync(buffer->fd) != LINUX_FDATASYNC_OK)
  {
    return DISK_BUFFER_POOL_SAVE_SYNC;
  }

  buffer->status = MAPPED_BUFFER_STATUS_ALLOCATED;

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

  for (size_t i = 0; i < pool->buffers_length; ++i)
  {
    MappedBuffer *buffer = pool->buffers + i;
    switch (buffer->status)
    {
    case MAPPED_BUFFER_STATUS_FREE:
    case MAPPED_BUFFER_STATUS_ALLOCATED:
      continue;

    case MAPPED_BUFFER_STATUS_MODIFIED:
    {
      DiskBufferPoolSaveError buffer_error =
          disk_buffer_pool_save_buffer(pool, i);

      if (buffer_error != DISK_BUFFER_POOL_SAVE_OK)
      {
        continue;
      }

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
    // TODO: We don't need this much space
    char path[LINUX_PATH_MAX] = {};
    resource_to_path(pool->save_path, resource, path, ARRAY_LENGTH(path));

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
    // TODO: We don't need this much space
    char path[LINUX_PATH_MAX] = {};
    resource_to_path(pool->save_path, resource, path, ARRAY_LENGTH(path));

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
  return pool->buffers + buffer_index;
}

// ----- Disk buffer pool -----
