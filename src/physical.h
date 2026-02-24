#ifndef PHYSICAL_H

#include "linux.h"
#include "std.h"

#define PAGE_SIZE (size_t)KIBIBYTES(8)

typedef size_t BlockIndex;

// ----- Disk buffer pool -----

typedef enum
{
  MAPPED_BUFFER_STATUS_USED,
  MAPPED_BUFFER_STATUS_FREE,
  MAPPED_BUFFER_STATUS_NEEDS_CLOSING,
} MappedBufferStatus;

// TODO: Support having a single buffer opened multiple times, this reduces
// memory usage when multiple buffers read the same block from the same relation
typedef struct
{
  MappedBufferStatus status;
  // TODO: Share fds between buffers, don't create an fd per buffer due to
  // limits
  int fd;
  BlockIndex block;
  // NOTE: The total length of buffers is ALWAYS PAGE_SIZE
  void *page;
} MappedBuffer;

typedef struct
{
  char save_path_buffer[LINUX_PATH_MAX];
  StringSlice save_path;
  size_t buffers_length;
  MappedBuffer *buffers;
  void *buffer_pages;
} DiskBufferPool;

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

static void disk_buffer_pool_close(DiskBufferPool *pool, size_t buffer_index)
{
  assert(pool->buffers[buffer_index].status == MAPPED_BUFFER_STATUS_USED);

  switch (linux_close(pool->buffers[buffer_index].fd))
  {
  case LINUX_CLOSE_BAD_FD:
    assert(false);
    // Fallthrough
  case LINUX_CLOSE_OK:
    pool->buffers[buffer_index] =
        (MappedBuffer){.status = MAPPED_BUFFER_STATUS_FREE};
    break;

  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
  case LINUX_CLOSE_UNKNOWN:
    pool->buffers[buffer_index].status = MAPPED_BUFFER_STATUS_NEEDS_CLOSING;
    break;
  }
}

typedef struct
{
  size_t index;
  bool32 found;
} DiskBufferPoolFindFreeBufferResult;

static DiskBufferPoolFindFreeBufferResult
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

    case MAPPED_BUFFER_STATUS_NEEDS_CLOSING:
      disk_buffer_pool_close(pool, i);
      if (pool->buffers[i].status != MAPPED_BUFFER_STATUS_NEEDS_CLOSING)
      {
        i -= 1;
      }
      break;

    case MAPPED_BUFFER_STATUS_USED:
      break;
    }
  }

  return (DiskBufferPoolFindFreeBufferResult){
      .index = 0,
      .found = false,
  };
}

static DiskBufferPoolOpenResult
disk_buffer_pool_open(DiskBufferPool *pool, StringSlice path, BlockIndex block)
{
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
    LinuxOpenResult open_result =
        linux_open(path.data, LINUX_OPEN_DIRECT | LINUX_OPEN_READ_WRITE, 0);
    if (open_result.error != LINUX_OPEN_OK)
    {
      return (DiskBufferPoolOpenResult){.error =
                                            DISK_BUFFER_POOL_OPEN_OPENING_FILE};
    }
    fd = open_result.fd;
  }

  if (linux_seek(fd, PAGE_SIZE * block, LINUX_SEEK_SET) != LINUX_SEEK_OK)
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
      .status = MAPPED_BUFFER_STATUS_USED,
      .fd = fd,
      .block = block,
      .page = page,
  };

  return (DiskBufferPoolOpenResult){
      .buffer_index = free_buffer_result.index,
      .error = DISK_BUFFER_POOL_OPEN_OK,
  };
}

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

static DiskBufferPoolNewBlockOpenResult
disk_buffer_pool_new_block_open(DiskBufferPool *pool, StringSlice path)
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
    LinuxOpenResult open_result =
        linux_open(path.data, LINUX_OPEN_DIRECT | LINUX_OPEN_READ_WRITE, 0);
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

  size_t block = (result.stat.st_size / PAGE_SIZE) + 1;
  if (linux_ftruncate(fd, PAGE_SIZE * block) != LINUX_FTRUNCATE_OK)
  {
    return (DiskBufferPoolNewBlockOpenResult){
        .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE};
  }

  if (linux_seek(fd, PAGE_SIZE * block, LINUX_SEEK_SET) != LINUX_SEEK_OK)
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
      .status = MAPPED_BUFFER_STATUS_USED,
      .fd = fd,
      .block = block,
      .page = page,
  };

  return (DiskBufferPoolNewBlockOpenResult){
      .buffer_index = free_buffer_result.index,
      .error = DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK,
  };
}

typedef enum
{
  DISK_BUFFER_POOL_SAVE_OK,
  DISK_BUFFER_POOL_SAVE_SEEKING_FILE,
  DISK_BUFFER_POOL_SAVE_WRITING_FILE,
  DISK_BUFFER_POOL_SAVE_SYNC,
} DiskBufferPoolSaveError;

static DiskBufferPoolSaveError
disk_buffer_pool_save(DiskBufferPool *pool, size_t buffer_index)
{
  MappedBuffer buffer = pool->buffers[buffer_index];
  if (linux_seek(buffer.fd, PAGE_SIZE * buffer.block, SEEK_SET)
      != LINUX_SEEK_OK)
  {
    return DISK_BUFFER_POOL_SAVE_SEEKING_FILE;
  }

  LinuxWriteResult write_result =
      linux_write(buffer.fd, buffer.page, PAGE_SIZE);
  if (write_result.error != LINUX_WRITE_OK)
  {
    return DISK_BUFFER_POOL_SAVE_WRITING_FILE;
  }
  assert(write_result.count == PAGE_SIZE);

  if (linux_fdatasync(buffer.fd) != LINUX_FDATASYNC_OK)
  {
    return DISK_BUFFER_POOL_SAVE_SYNC;
  }

  return DISK_BUFFER_POOL_SAVE_OK;
}

// ----- Disk buffer pool -----

// ----- Relation -----

typedef int64_t RelationId;

// Limit number of columns to 2^16
typedef int16_t ColumnsLength;

// ----- Store types -----
typedef int64_t StoreInteger;

typedef struct
{
  int16_t offset;
  int16_t length;
} StoreString;

typedef union
{
  StoreInteger integer;
  StoreString string;
} StoredValue;
// ----- Store types -----

typedef enum
{
  COLUMN_TYPE_INTEGER,
  COLUMN_TYPE_STRING,
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
  StringSlice string;
} ColumnValue;

// TODO: Merge this and ColumnValue
typedef union
{
  MemoryInteger integer;
  MemorySlice string;
} ColumnValue2;

static StringSlice relation_id_to_path(
    StringSlice save_path, RelationId relation_id, char *path, size_t length)
{
  size_t index = string_slice_concat(path, 0, length, save_path, false);
  index = string_slice_concat(
      path, index, length, string_slice_from_ptr("/"), false);

  size_t start_index = index;
  if (relation_id == 0)
  {
    path[index++] = '0';
  }

  // TODO: Simplify
  const RelationId base = 10;
  for (RelationId id = relation_id; id > 0; id /= base, ++index)
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

  return (StringSlice){.data = path, .length = index};
}

static size_t column_type_fixed_size(ColumnType type)
{
  StoredValue value;
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
    return sizeof(value.integer);
  case COLUMN_TYPE_STRING:
    return sizeof(value.string);
  }
}

static size_t column_type_variable_size(ColumnType type, ColumnValue value)
{
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
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

static TupleSizes tuple_column_fixed_and_variable_size(
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

static size_t
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

static size_t tuple_column_fixed_byte_offset(
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

RelationHeader *relation_header(MappedBuffer buffer)
{
  return buffer.page;
}

void *relation_tuple(MappedBuffer buffer, size_t fixed_size, size_t tuple_index)
{
  return buffer.page + sizeof(RelationHeader) + (fixed_size * tuple_index);
}

void *relation_free_tuple(MappedBuffer buffer, TupleSizes tuple_sizes)
{
  return buffer.page + sizeof(RelationHeader)
         + (tuple_sizes.fixed_size
            * relation_header(buffer)->allocated_records);
}

size_t relation_free_space(MappedBuffer buffer, TupleSizes tuple_sizes)
{
  RelationHeader *header = relation_header(buffer);
  return header->variable_data_start
         - (tuple_sizes.fixed_size * header->allocated_records);
}

typedef enum
{
  RELATION_INSERT_TUPLE_OK,
  RELATION_INSERT_TUPLE_SAVING,
  RELATION_INSERT_TUPLE_OPENING_BUFFER,
  RELATION_INSERT_TUPLE_BUFFER_POOL_FULL,
  RELATION_INSERT_TUPLE_TUPLE_TOO_BIG,
} RelationInsertTupleError;

typedef enum
{
  RELATION_CREATE_OK,
  RELATION_CREATE_FAILED_TO_CREATE,
  RELATION_CREATE_FAILED_TO_STAT,
  RELATION_CREATE_ALREADY_EXISTS,
  RELATION_CREATE_PROGRAM_ERROR,
  RELATION_CREATE_FAILED_TO_WRITE,
} DiskBufferPoolCreateRelationError;

static DiskBufferPoolCreateRelationError
relation_create(DiskBufferPool *pool, RelationId id, bool32 expect_new)
{
  char path[LINUX_PATH_MAX] = {};
  relation_id_to_path(pool->save_path, id, path, ARRAY_LENGTH(path));
  LinuxOpenResult open_result = linux_open(
      path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
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
    return RELATION_CREATE_FAILED_TO_CREATE;

  case LINUX_OPEN_ALREADY_EXISTS:
    return RELATION_CREATE_ALREADY_EXISTS;

  case LINUX_OPEN_PATH_SEG_FAULT:
  case LINUX_OPEN_PATH_FILE_TOO_BIG:
  case LINUX_OPEN_FLAGS_MISUSE:
  case LINUX_OPEN_IS_DIRECTORY:
  case LINUX_OPEN_PATH_TOO_LONG:
  case LINUX_OPEN_NOT_FOUND:
  case LINUX_OPEN_UNKNOWN:
    return RELATION_CREATE_PROGRAM_ERROR;
  }

  if (!expect_new)
  {
    LinuxFStatResult result = linux_fstat(open_result.fd);
    if (result.error != LINUX_FSTAT_OK)
    {
      linux_close(open_result.fd);
      return RELATION_CREATE_FAILED_TO_STAT;
    }

    if (result.stat.st_size > 0)
    {
      linux_close(open_result.fd);
      return RELATION_CREATE_OK;
    }
  }

  RelationHeader header = (RelationHeader){
      .allocated_records = 0,
      .variable_data_start = PAGE_SIZE,
  };

  LinuxWriteResult write_result =
      linux_write(open_result.fd, (void *)&header, PAGE_SIZE);
  switch (write_result.error)
  {
  case LINUX_WRITE_OK:
    assert(write_result.count == PAGE_SIZE);
    break;

  case LINUX_WRITE_WOULD_BLOCK:
  case LINUX_WRITE_QUOTA:
  case LINUX_WRITE_INTERRUPT:
  case LINUX_WRITE_INVALID:
  case LINUX_WRITE_IO:
  case LINUX_WRITE_NO_SPACE:
  case LINUX_WRITE_PERMISSIONS:
  case LINUX_WRITE_PIPE_CLOSED:
    linux_close(open_result.fd);
    return RELATION_CREATE_FAILED_TO_WRITE;

  case LINUX_WRITE_BAD_FD:
  case LINUX_WRITE_BUFFER_SEG_FAULT:
  case LINUX_WRITE_INVALID_PEER_ADDRESS:
  case LINUX_WRITE_LENGTH_TOO_BIG:
  case LINUX_WRITE_UNKNOWN:
    linux_close(open_result.fd);
    return RELATION_CREATE_PROGRAM_ERROR;
  }

  switch (linux_fdatasync(open_result.fd))
  {
  case LINUX_FDATASYNC_OK:
    break;

  case LINUX_FDATASYNC_BAD_FD:
  case LINUX_FDATASYNC_FD_NO_SYNC_SUPPORT:
  case LINUX_FDATASYNC_UNKNOWN:
    linux_close(open_result.fd);
    return RELATION_CREATE_PROGRAM_ERROR;

  case LINUX_FDATASYNC_INTERRUPT:
  case LINUX_FDATASYNC_IO:
  case LINUX_FDATASYNC_NO_SPACE:
  case LINUX_FDATASYNC_READ_ONLY_FILESYSTEM:
  case LINUX_FDATASYNC_QUOTA:
    linux_close(open_result.fd);
    return RELATION_CREATE_FAILED_TO_WRITE;
  }

  switch (linux_close(open_result.fd))
  {
  case LINUX_CLOSE_OK:
    return RELATION_CREATE_OK;

  case LINUX_CLOSE_BAD_FD:
    return RELATION_CREATE_PROGRAM_ERROR;

  case LINUX_CLOSE_INTERRUPT:
  case LINUX_CLOSE_IO:
  case LINUX_CLOSE_QUOTA:
  case LINUX_CLOSE_UNKNOWN:
    return RELATION_CREATE_FAILED_TO_WRITE;
  }
}

static void relation_delete(DiskBufferPool *pool, RelationId id)
{
  char path[LINUX_PATH_MAX] = {};
  relation_id_to_path(pool->save_path, id, path, ARRAY_LENGTH(path));
  LinuxUnlinkError error = linux_unlink(path);
  // FIXME: Mark undeleted files to delete them later
  assert(error == LINUX_UNLINK_OK);
}

// TODO: This assumes that the column types do not change between inserts
static RelationInsertTupleError relation_insert_tuple(
    DiskBufferPool *pool,
    RelationId relation_id,
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length)
{
  assert(pool != NULL);
  assert(types != NULL);
  assert(values != NULL);
  assert(tuple_length > 0);

  TupleSizes tuple_sizes =
      tuple_column_fixed_and_variable_size(types, values, tuple_length);

  const size_t required_space =
      tuple_sizes.fixed_size + tuple_sizes.variable_size;

  size_t buffer_index = 0;
  enum
  {
    RELATION_INSERT_TUPLE_STATUS_OK,
    RELATION_INSERT_TUPLE_STATUS_NEXT_BLOCK,
    RELATION_INSERT_TUPLE_STATUS_EXPAND_FILE,
  } status = RELATION_INSERT_TUPLE_STATUS_NEXT_BLOCK;

  BlockIndex block = 0;
  for (block = 0; status == RELATION_INSERT_TUPLE_STATUS_NEXT_BLOCK; ++block)
  {
    char path[LINUX_PATH_MAX];
    DiskBufferPoolOpenResult result = disk_buffer_pool_open(
        pool,
        relation_id_to_path(pool->save_path, relation_id, path, LINUX_PATH_MAX),
        block);

    buffer_index = result.buffer_index;

    switch (result.error)
    {
    case DISK_BUFFER_POOL_OPEN_OK:
    {
      if (required_space
          <= relation_free_space(pool->buffers[buffer_index], tuple_sizes))
      {
        status = RELATION_INSERT_TUPLE_STATUS_OK;
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
      status = RELATION_INSERT_TUPLE_STATUS_NEXT_BLOCK;
      continue;

    case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
      status = RELATION_INSERT_TUPLE_STATUS_EXPAND_FILE;
      break;

    case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
      return RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;
    }
  }

  switch (status)
  {
  case RELATION_INSERT_TUPLE_STATUS_OK:
    break;

  case RELATION_INSERT_TUPLE_STATUS_NEXT_BLOCK:
    assert(false);
    break;

  case RELATION_INSERT_TUPLE_STATUS_EXPAND_FILE:
  {
    char path[LINUX_PATH_MAX];
    DiskBufferPoolNewBlockOpenResult result = disk_buffer_pool_new_block_open(
        pool,
        relation_id_to_path(
            pool->save_path, relation_id, path, LINUX_PATH_MAX));

    switch (result.error)
    {
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OK:
      break;

    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_BUFFER_POOL_FULL:
      return RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;
      break;

    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_OPENING_FILE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_RESIZE_FILE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READ_FILE_SIZE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_SEEKING_FILE:
    case DISK_BUFFER_POOL_NEW_BLOCK_OPEN_READING_FILE:
      return RELATION_INSERT_TUPLE_OPENING_BUFFER;
    }

    buffer_index = result.buffer_index;

    if (required_space
        > relation_free_space(pool->buffers[buffer_index], tuple_sizes))
    {
      disk_buffer_pool_close(pool, result.buffer_index);
      return RELATION_INSERT_TUPLE_TUPLE_TOO_BIG;
    }
  }
  break;
  }

  RelationHeader *header = relation_header(pool->buffers[buffer_index]);
  void *tuple_memory =
      relation_free_tuple(pool->buffers[buffer_index], tuple_sizes);

  for (ColumnsLength column = 0; column < tuple_length; ++column)
  {
    const size_t column_byte_offset =
        tuple_column_fixed_byte_offset(types, tuple_length, column);

    StoredValue *field = tuple_memory + column_byte_offset;

    switch (types[column])
    {

    case COLUMN_TYPE_INTEGER:
    {
      field->integer = values[column].integer;
    }
    break;

    case COLUMN_TYPE_STRING:
    {
      const u_int16_t MAX_UINT_16 = 0xFFFF;
      size_t field_variable_size =
          column_type_variable_size(types[column], values[column]);
      assert(field_variable_size <= MAX_UINT_16);

      header->variable_data_start -= (int16_t)field_variable_size;

      memory_copy_forward(
          &pool->buffers[buffer_index].page[header->variable_data_start],
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
      disk_buffer_pool_save(pool, buffer_index);

  disk_buffer_pool_close(pool, buffer_index);

  switch (save_error)
  {
  case DISK_BUFFER_POOL_SAVE_OK:
    return RELATION_INSERT_TUPLE_OK;

  case DISK_BUFFER_POOL_SAVE_SEEKING_FILE:
  case DISK_BUFFER_POOL_SAVE_WRITING_FILE:
  case DISK_BUFFER_POOL_SAVE_SYNC:
    return RELATION_INSERT_TUPLE_SAVING;
  }
}

static void relation_delete_tuples(
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
    char path[LINUX_PATH_MAX];
    DiskBufferPoolOpenResult result = disk_buffer_pool_open(
        pool,
        relation_id_to_path(pool->save_path, relation_id, path, LINUX_PATH_MAX),
        block);

    // Can't use break to break out of the loop from inside the swtich, so we
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
    case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
      // FIXME: Use transactions to handle failure
      continue;

    case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
      assert(false);
      break;
    }

    MappedBuffer *buffer = &pool->buffers[result.buffer_index];
    RelationHeader *header = relation_header(*buffer);

    int16_t block_variable_data_removed = 0;

    for (size_t tuple_index = 0; tuple_index < header->allocated_records;)
    {
      void *tuple_memory = relation_tuple(*buffer, fixed_size, tuple_index);

      bool32 delete = false;

      StoredValue *field = tuple_memory + column_byte_offset;
      switch (types[column_index])
      {
      case COLUMN_TYPE_INTEGER:
      {
        delete = field->integer == value.integer;
      }
      break;

      case COLUMN_TYPE_STRING:
      {
        StringSlice slice = (StringSlice){
            .length = field->string.length,
            .data = &buffer->page[field->string.offset],
        };
        delete = string_slice_eq(slice, value.string);
      }
      break;
      }

      int16_t block_variable_data_end = 0;
      int16_t block_variable_data_start = 0;
      for (ColumnsLength column = 0; column < tuple_length; ++column)
      {
        const size_t column_byte_offset =
            tuple_column_fixed_byte_offset(types, tuple_length, column);
        StoredValue *field = tuple_memory + column_byte_offset;

        switch (types[column])
        {
        case COLUMN_TYPE_INTEGER:
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
        memory_copy_forward(
            tuple_memory,
            tuple_memory + fixed_size,
            fixed_size * (header->allocated_records - (tuple_index + 1)));

        if (block_variable_data_end != 0)
        {
          size_t variable_data_remaining =
              block_variable_data_start - header->variable_data_start;

          memory_copy_backward(
              &buffer->page[block_variable_data_end - 1],
              &buffer->page[block_variable_data_start - 1],
              variable_data_remaining);

          int16_t variable_data_removed =
              block_variable_data_end - block_variable_data_start;
          block_variable_data_removed += variable_data_removed;
          header->variable_data_start += variable_data_removed;
        }

        header->allocated_records -= 1;
      }
      else
      {
        tuple_index++;
      }
    }
  }
}

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

typedef enum
{
  DISK_BUFFER_LOAD_NEXT_PAGE_OK,
  DISK_BUFFER_LOAD_NEXT_PAGE_NO_MORE_TUPLES,
  DISK_BUFFER_LOAD_NEXT_PAGE_LOADING_PAGE,
} LoadNextNonEmptyRelationBlockError;

typedef struct
{
  BlockIndex block;
  size_t buffer_index;
  LoadNextNonEmptyRelationBlockError error;
} LoadNextNonEmptyRelationBlockResult;

static LoadNextNonEmptyRelationBlockResult load_next_non_empty_relation_block(
    DiskBufferPool *pool, RelationId id, BlockIndex block)
{
  for (;; ++block)
  {
    char path[LINUX_PATH_MAX];
    DiskBufferPoolOpenResult result = disk_buffer_pool_open(
        pool,
        relation_id_to_path(pool->save_path, id, path, LINUX_PATH_MAX),
        block);

    switch (result.error)
    {
    case DISK_BUFFER_POOL_OPEN_OK:
      if (relation_header(pool->buffers[result.buffer_index])->allocated_records
          > 0)
      {
        return (LoadNextNonEmptyRelationBlockResult){
            .error = DISK_BUFFER_LOAD_NEXT_PAGE_OK,
            .block = block,
            .buffer_index = result.buffer_index,
        };
      }
      disk_buffer_pool_close(pool, result.buffer_index);
      break;

    case DISK_BUFFER_POOL_OPEN_FILE_TOO_SMALL:
      return (LoadNextNonEmptyRelationBlockResult){
          .error = DISK_BUFFER_LOAD_NEXT_PAGE_NO_MORE_TUPLES};

    case DISK_BUFFER_POOL_OPEN_OPENING_FILE:
    case DISK_BUFFER_POOL_OPEN_SEEKING_FILE:
    case DISK_BUFFER_POOL_OPEN_READING_FILE:
    case DISK_BUFFER_POOL_OPEN_BUFFER_POOL_FULL:
      return (LoadNextNonEmptyRelationBlockResult){
          .error = DISK_BUFFER_LOAD_NEXT_PAGE_LOADING_PAGE};
    }
  }
}

static RelationIterator relation_iterate(DiskBufferPool *pool, RelationId id)
{
  assert(pool != NULL);

  LoadNextNonEmptyRelationBlockResult result =
      load_next_non_empty_relation_block(pool, id, 0);
  switch (result.error)
  {
  case DISK_BUFFER_LOAD_NEXT_PAGE_OK:
    return (RelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = result.buffer_index,
        .tuple_index = 0,
        .status = RELATION_ITERATOR_STATUS_OK,
    };

  case DISK_BUFFER_LOAD_NEXT_PAGE_NO_MORE_TUPLES:
    return (RelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = {},
        .tuple_index = 0,
        .status = RELATION_ITERATOR_STATUS_NO_MORE_TUPLES,
    };

  case DISK_BUFFER_LOAD_NEXT_PAGE_LOADING_PAGE:
    return (RelationIterator){
        .pool = pool,
        .relation_id = id,
        .buffer_index = {},
        .tuple_index = 0,
        .status = RELATION_ITERATOR_STATUS_ERROR,
    };
  }
}

static void relation_iterator_next(RelationIterator *it)
{
  assert(it != NULL);

  MappedBuffer buffer = it->pool->buffers[it->buffer_index];

  it->tuple_index += 1;
  if (it->tuple_index < relation_header(buffer)->allocated_records)
  {
    return;
  }

  disk_buffer_pool_close(it->pool, it->buffer_index);

  LoadNextNonEmptyRelationBlockResult result =
      load_next_non_empty_relation_block(
          it->pool, it->relation_id, buffer.block + 1);

  switch (result.error)
  {
  case DISK_BUFFER_LOAD_NEXT_PAGE_OK:
    *it = (RelationIterator){
        .pool = it->pool,
        .relation_id = it->relation_id,
        .buffer_index = result.buffer_index,
        .tuple_index = 0,
        .status = RELATION_ITERATOR_STATUS_OK,
    };
    break;

  case DISK_BUFFER_LOAD_NEXT_PAGE_NO_MORE_TUPLES:
    it->status = RELATION_ITERATOR_STATUS_NO_MORE_TUPLES;
    break;

  case DISK_BUFFER_LOAD_NEXT_PAGE_LOADING_PAGE:
    it->status = RELATION_ITERATOR_STATUS_ERROR;
    break;
  }
}

static ColumnValue relation_iterator_get(
    RelationIterator *it,
    const ColumnType *types,
    ColumnsLength tuple_length,
    ColumnsLength column_index)
{
  assert(it != NULL);
  assert(types != NULL);
  assert(tuple_length > 0);
  assert(column_index < tuple_length);

  MappedBuffer *buffer = &it->pool->buffers[it->buffer_index];
  size_t fixed_size = tuple_column_fixed_size(types, tuple_length);
  void *tuple_memory = relation_tuple(*buffer, fixed_size, it->tuple_index);

  const size_t column_byte_offset =
      tuple_column_fixed_byte_offset(types, tuple_length, column_index);
  StoredValue *field = tuple_memory + column_byte_offset;

  switch (types[column_index])
  {
  case COLUMN_TYPE_INTEGER:
  {
    return (ColumnValue){.integer = field->integer};
  }
  break;

  case COLUMN_TYPE_STRING:
  {
    return (ColumnValue){
        .string =
            (StringSlice){
                .length = field->string.length,
                .data = &buffer->page[field->string.offset],
            },
    };
  }
  break;
  }
}

void relation_iterator_close(RelationIterator *it)
{
  if (it->status == RELATION_ITERATOR_STATUS_OK)
  {
    disk_buffer_pool_close(it->pool, it->buffer_index);
  }
}

// ----- Relation -----

#define PHYSICAL_H
#endif
