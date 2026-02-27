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

internal DiskBufferPoolNewBlockOpenResult disk_buffer_pool_new_block_open(
    DiskBufferPool *pool, DiskResource resource, void *data, size_t length);

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

internal size_t column_type_variable_size(ColumnType type, ColumnValue value)
{
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
  case COLUMN_TYPE_BOOLEAN:
    return 0;

  case COLUMN_TYPE_STRING:
    return value.string.length;
  }
}

typedef struct
{
  size_t fixed_size;
  size_t variable_size;
} TupleSizes;

internal TupleSizes tuple_column_fixed_and_variable_size(
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length)
{
  assert(types != NULL);
  assert(values != NULL);

  size_t fixed_size = 0;
  size_t variable_size = 0;
  for (int64_t i = 0; i < tuple_length; ++i)
  {
    fixed_size += column_type_fixed_size(types[i]);
    variable_size += column_type_variable_size(types[i], values[i]);
  }
  return (TupleSizes){
      .fixed_size = fixed_size,
      .variable_size = variable_size,
  };
}

internal size_t
tuple_column_fixed_size(const ColumnType *types, ColumnsLength tuple_length)
{
  assert(types != NULL);

  size_t fixed_size = 0;
  for (size_t i = 0; i < tuple_length; ++i)
  {
    fixed_size += column_type_fixed_size(types[i]);
  }
  return fixed_size;
}

internal size_t tuple_column_fixed_byte_offset(
    const ColumnType *types, int64_t length, ColumnsLength column_index)
{
  assert(column_index < length);

  size_t offset = 0;
  for (size_t i = 0; i < column_index; ++i)
  {
    offset += column_type_fixed_size(types[i]);
  }
  return offset;
}

typedef struct
{
  int16_t allocated_records;
  int16_t variable_data_start;
} RelationHeader;

internal const RelationHeader *relation_header_read(MappedBuffer *buffer)
{
  return mapped_buffer_read(buffer);
}

internal const void *
relation_tuple_read(MappedBuffer *buffer, size_t fixed_size, size_t tuple_index)
{
  return mapped_buffer_read(buffer) + sizeof(RelationHeader)
         + (fixed_size * tuple_index);
}

internal RelationHeader *relation_header_write(MappedBuffer *buffer)
{
  return mapped_buffer_write(buffer);
}

internal void *relation_tuple_write(
    MappedBuffer *buffer, size_t fixed_size, size_t tuple_index)
{
  return mapped_buffer_write(buffer) + sizeof(RelationHeader)
         + (fixed_size * tuple_index);
}

internal size_t
relation_free_space(MappedBuffer *buffer, TupleSizes tuple_sizes)
{
  const RelationHeader *header = relation_header_read(buffer);
  return header->variable_data_start
         - (tuple_sizes.fixed_size * header->allocated_records);
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

// TODO: This assumes that the column types do not change between inserts
PhysicalRelationInsertTupleError physical_relation_insert_tuple(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    const ColumnValue *values,
    const bool32 *primary_keys,
    ColumnsLength tuple_length)
{
  assert(pool != NULL);
  assert(types != NULL);
  assert(values != NULL);
  assert(primary_keys != NULL);
  assert(tuple_length > 0);

  TupleSizes tuple_sizes =
      tuple_column_fixed_and_variable_size(types, values, tuple_length);

  const size_t required_space =
      tuple_sizes.fixed_size + tuple_sizes.variable_size;

  size_t buffer_index = 0;
  enum
  {
    INSERT_TUPLE_STATUS_NEXT_BLOCK,
    INSERT_TUPLE_STATUS_NO_MORE_BLOCKS,
    INSERT_TUPLE_STATUS_POOL_FULL,
    INSERT_TUPLE_STATUS_FOUND,
  } status = INSERT_TUPLE_STATUS_NEXT_BLOCK;

  BlockIndex block = 0;
  for (block = 0; status == INSERT_TUPLE_STATUS_NEXT_BLOCK; ++block)
  {
    DiskBufferPoolOpenResult result = disk_buffer_pool_open(
        pool,
        (DiskResource){.type = RESOURCE_TYPE_RELATION,
                       .relation_id = relation_id},
        block);

    buffer_index = result.buffer_index;

    switch (result.error)
    {
    case DISK_BUFFER_POOL_OPEN_OK:
    {
      if (required_space <= relation_free_space(
              disk_buffer_pool_mapped_buffer(pool, buffer_index), tuple_sizes))
      {
        buffer_index = result.buffer_index;
        status = INSERT_TUPLE_STATUS_FOUND;
      }
      else
      {
        disk_buffer_pool_close(pool, result.buffer_index);
      }
    }
    break;

    case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
    case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
    case DISK_BUFFER_POOL_OPEN_READING_FILE:
      status = INSERT_TUPLE_STATUS_NEXT_BLOCK;
      continue;

    case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
      status = INSERT_TUPLE_STATUS_NO_MORE_BLOCKS;
      break;

    case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
      status = INSERT_TUPLE_STATUS_POOL_FULL;
      break;
    }
  }

  switch (status)
  {
  case INSERT_TUPLE_STATUS_FOUND:
    break;

  case INSERT_TUPLE_STATUS_POOL_FULL:
    return PHYSICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;

  case INSERT_TUPLE_STATUS_NEXT_BLOCK:
    assert(false);
    break;

  case INSERT_TUPLE_STATUS_NO_MORE_BLOCKS:
  {
    RelationHeader init_header = (RelationHeader){
        .allocated_records = 0,
        .variable_data_start = PAGE_SIZE,
    };

    DiskBufferPoolNewBlockOpenResult result = disk_buffer_pool_new_block_open(
        pool,
        (DiskResource){.type = RESOURCE_TYPE_RELATION,
                       .relation_id = relation_id},
        &init_header,
        sizeof(init_header));

    switch (result.error)
    {
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK:
      break;

    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_BUFFER_POOL_FULL:
      return PHYSICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;
      break;

    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OPENING_FILE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READ_FILE_SIZE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_SEEKING_FILE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READING_FILE:
      return PHYSICAL_RELATION_INSERT_TUPLE_OPENING_BUFFER;
    }

    buffer_index = result.buffer_index;

    if (required_space > relation_free_space(
            disk_buffer_pool_mapped_buffer(pool, result.buffer_index),
            tuple_sizes))
    {
      disk_buffer_pool_close(pool, result.buffer_index);
      return PHYSICAL_RELATION_INSERT_TUPLE_TOO_BIG;
    }
  }
  break;
  }

  MappedBuffer *buffer = disk_buffer_pool_mapped_buffer(pool, buffer_index);
  RelationHeader *header = relation_header_write(buffer);
  void *tuple_memory = relation_tuple_write(
      buffer, tuple_sizes.fixed_size, header->allocated_records);

  for (ColumnsLength column = 0; column < tuple_length; ++column)
  {
    const size_t column_byte_offset =
        tuple_column_fixed_byte_offset(types, tuple_length, column);

    StoredValue *field = tuple_memory + column_byte_offset;

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
      const u_int16_t MAX_UINT_16 = 0xFFFF;
      size_t field_variable_size =
          column_type_variable_size(types[column], values[column]);
      assert(field_variable_size <= MAX_UINT_16);

      header->variable_data_start -= (int16_t)field_variable_size;
      memory_copy_forward(
          mapped_buffer_write(buffer) + header->variable_data_start,
          values[column].string.data,
          field_variable_size);

      field->string = (StoreString){
          .length = (int16_t)field_variable_size,
          .offset = header->variable_data_start,
      };
    }
    break;
    }
  }

  header->allocated_records += 1;

  assert(
      tuple_sizes.fixed_size * header->allocated_records
      <= header->variable_data_start);

  DiskBufferPoolSaveError save_error =
      disk_buffer_pool_save_buffer(pool, buffer_index);

  disk_buffer_pool_close(pool, buffer_index);

  switch (save_error)
  {
  case DISK_BUFFER_POOL_SAVE_OK:
    return PHYSICAL_RELATION_INSERT_TUPLE_OK;

  case DISK_BUFFER_POOL_SAVE_SEEKING_FILE:
  case DISK_BUFFER_POOL_SAVE_WRITING_FILE:
  case DISK_BUFFER_POOL_SAVE_SYNC:
    return PHYSICAL_RELATION_INSERT_TUPLE_SAVING;
  }
}

PhysicalRelationDeleteTuplesError physical_relation_delete_tuples(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index,
    ColumnValue value)
{
  assert(pool != NULL);
  assert(types != NULL);
  assert(tuple_length > 0);
  assert(column_index < tuple_length);

  size_t fixed_size = tuple_column_fixed_size(types, tuple_length);

  const size_t column_byte_offset =
      tuple_column_fixed_byte_offset(types, tuple_length, column_index);

  for (BlockIndex block = 0;; ++block)
  {
    DiskBufferPoolOpenResult result = disk_buffer_pool_open(
        pool,
        (DiskResource){.type = RESOURCE_TYPE_RELATION,
                       .relation_id = relation_id},
        block);

    // Can't use break to break out of the loop from inside the switch, so we
    // handle this separately
    if (result.error == DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL)
    {
      break;
    }

    switch (result.error)
    {
    case DISK_BUFFER_POOL_OPEN_OK:
      break;

    case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
    case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
    case DISK_BUFFER_POOL_OPEN_READING_FILE:
      return PHYSICAL_RELATION_DELETE_TUPLES_READING;

    case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
      return PHYSICAL_RELATION_DELETE_TUPLES_BUFFER_POOL_FULL;

    case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
      assert(false);
      break;
    }

    MappedBuffer *buffer =
        disk_buffer_pool_mapped_buffer(pool, result.buffer_index);
    const RelationHeader *header = relation_header_read(buffer);

    int16_t block_variable_data_removed = 0;

    for (size_t tuple_index = 0; tuple_index < header->allocated_records;)
    {
      const void *tuple_memory =
          relation_tuple_read(buffer, fixed_size, tuple_index);

      bool32 delete = false;

      const StoredValue *field = tuple_memory + column_byte_offset;
      switch (types[column_index])
      {
      case COLUMN_TYPE_INTEGER:
        delete = field->integer == value.integer;
        break;

      case COLUMN_TYPE_BOOLEAN:
        delete = field->boolean == value.boolean;
        break;

      case COLUMN_TYPE_STRING:
      {
        StringSlice slice = (StringSlice){
            .length = field->string.length,
            .data = mapped_buffer_read(buffer) + field->string.offset,
        };
        delete = string_slice_eq(slice, value.string);
      }
      break;
      }

      int16_t block_variable_data_end = 0;
      int16_t block_variable_data_start = 0;
      for (ColumnsLength column = 0; column < tuple_length; ++column)
      {
        StoredValue *field =
            relation_tuple_write(buffer, fixed_size, tuple_index)
            + tuple_column_fixed_byte_offset(types, tuple_length, column);

        switch (types[column])
        {
        case COLUMN_TYPE_INTEGER:
        case COLUMN_TYPE_BOOLEAN:
          break;

        case COLUMN_TYPE_STRING:
        {
          field->string.offset += block_variable_data_removed;

          if (block_variable_data_end == 0)
          {
            block_variable_data_end =
                field->string.offset + field->string.length;
            block_variable_data_start = block_variable_data_end;
          }

          // Variable data should be contiguous in memory
          assert(
              field->string.offset + field->string.length
              == block_variable_data_start);

          block_variable_data_start = field->string.offset;
        }
        break;
        }
      }

      if (delete)
      {
        void *tuple_memory =
            relation_tuple_write(buffer, fixed_size, tuple_index);

        memory_copy_forward(
            tuple_memory,
            tuple_memory + fixed_size,
            fixed_size * (header->allocated_records - (tuple_index + 1)));

        if (block_variable_data_end != 0)
        {
          size_t variable_data_remaining =
              block_variable_data_start - header->variable_data_start;

          memory_copy_backward(
              mapped_buffer_write(buffer) + block_variable_data_end - 1,
              mapped_buffer_write(buffer) + block_variable_data_start - 1,
              variable_data_remaining);

          int16_t variable_data_removed =
              block_variable_data_end - block_variable_data_start;
          block_variable_data_removed += variable_data_removed;
          relation_header_write(buffer)->variable_data_start +=
              variable_data_removed;
        }

        relation_header_write(buffer)->allocated_records -= 1;
      }
      else
      {
        tuple_index++;
      }
    }

    DiskBufferPoolSaveError save_error =
        disk_buffer_pool_save_buffer(pool, result.buffer_index);

    disk_buffer_pool_close(pool, result.buffer_index);

    switch (save_error)
    {
    case DISK_BUFFER_POOL_SAVE_OK:
      break;

    case DISK_BUFFER_POOL_SAVE_SEEKING_FILE:
    case DISK_BUFFER_POOL_SAVE_WRITING_FILE:
    case DISK_BUFFER_POOL_SAVE_SYNC:
      return PHYSICAL_RELATION_DELETE_TUPLES_WRITING;
    }
  }

  return PHYSICAL_RELATION_DELETE_TUPLES_OK;
}

typedef enum
{
  LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_OK,
  LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_NO_MORE_TUPLES,
  LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_LOADING_PAGE,
} LoadNextNonEmptyRelationBlockError;

typedef struct
{
  BlockIndex block;
  size_t buffer_index;
  LoadNextNonEmptyRelationBlockError error;
} LoadNextNonEmptyRelationBlockResult;

internal LoadNextNonEmptyRelationBlockResult load_next_non_empty_relation_block(
    DiskBufferPool *pool, RelationId id, BlockIndex block)
{
  for (;; ++block)
  {
    DiskBufferPoolOpenResult result = disk_buffer_pool_open(
        pool,
        (DiskResource){.type = RESOURCE_TYPE_RELATION, .relation_id = id},
        block);

    switch (result.error)
    {
    case DISK_BUFFER_POOL_OPEN_OK:
      if (relation_header_read(
              disk_buffer_pool_mapped_buffer(pool, result.buffer_index))
              ->allocated_records
          > 0)
      {
        return (LoadNextNonEmptyRelationBlockResult){
            .error = LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_OK,
            .block = block,
            .buffer_index = result.buffer_index,
        };
      }
      disk_buffer_pool_close(pool, result.buffer_index);
      break;

    case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
      return (LoadNextNonEmptyRelationBlockResult){
          .error = LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_NO_MORE_TUPLES};

    case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
    case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
    case DISK_BUFFER_POOL_OPEN_READING_FILE:
    case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
      return (LoadNextNonEmptyRelationBlockResult){
          .error = LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_LOADING_PAGE};
    }
  }
}

PhysicalRelationIterator
physical_relation_iterate(DiskBufferPool *pool, RelationId id)
{
  assert(pool != NULL);

  LoadNextNonEmptyRelationBlockResult result =
      load_next_non_empty_relation_block(pool, id, 0);
  switch (result.error)
  {
  case LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_OK:
    return (PhysicalRelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = result.buffer_index,
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_OK,
    };

  case LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_NO_MORE_TUPLES:
    return (PhysicalRelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = {},
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES,
    };

  case LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_LOADING_PAGE:
    return (PhysicalRelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = {},
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_ERROR,
    };
  }
}

void physical_relation_iterator_next(PhysicalRelationIterator *it)
{
  assert(it != NULL);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);

  it->tuple_index += 1;
  if (it->tuple_index < relation_header_read(buffer)->allocated_records)
  {
    return;
  }

  disk_buffer_pool_close(it->pool, it->buffer_index);

  LoadNextNonEmptyRelationBlockResult result =
      load_next_non_empty_relation_block(
          it->pool, it->relation_id, mapped_buffer_block(buffer) + 1);

  switch (result.error)
  {
  case LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_OK:
    *it = (PhysicalRelationIterator){
        .pool = it->pool,
        .relation_id = it->relation_id,
        .buffer_index = result.buffer_index,
        .tuple_index = 0,
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_OK,
    };
    break;

  case LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_NO_MORE_TUPLES:
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES;
    break;

  case LOAD_NEXT_NON_EMPTY_RELATION_BLOCK_LOADING_PAGE:
    it->status = PHYSICAL_RELATION_ITERATOR_STATUS_ERROR;
    break;
  }
}

ColumnValue physical_relation_iterator_get(
    PhysicalRelationIterator *it,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index)
{
  assert(it != NULL);
  assert(types != NULL);
  assert(tuple_length > 0);
  assert(column_index < tuple_length);

  MappedBuffer *buffer =
      disk_buffer_pool_mapped_buffer(it->pool, it->buffer_index);
  size_t fixed_size = tuple_column_fixed_size(types, tuple_length);
  const void *tuple_memory =
      relation_tuple_read(buffer, fixed_size, it->tuple_index);

  const size_t column_byte_offset =
      tuple_column_fixed_byte_offset(types, tuple_length, column_index);
  const StoredValue *field = tuple_memory + column_byte_offset;

  switch (types[column_index])
  {
  case COLUMN_TYPE_INTEGER:
    return (ColumnValue){.integer = field->integer};
    break;

  case COLUMN_TYPE_BOOLEAN:
    return (ColumnValue){.boolean = field->boolean};
    break;

  case COLUMN_TYPE_STRING:
    return (ColumnValue){
        .string =
            (StringSlice){
                .length = field->string.length,
                .data = mapped_buffer_read(buffer) + field->string.offset,
            },
    };
    break;
  }
}

void physical_relation_iterator_close(PhysicalRelationIterator *it)
{
  if (it->status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
  {
    disk_buffer_pool_close(it->pool, it->buffer_index);
  }
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

internal DiskBufferPoolNewBlockOpenResult disk_buffer_pool_new_block_open(
    DiskBufferPool *pool, DiskResource resource, void *data, size_t length)
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

  memory_copy_forward(page, data, length);

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
