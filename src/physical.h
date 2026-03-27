#ifndef PHYSICAL_H

#include "linux.h"
#include "std.h"

// ----- Write Ahead Log -----

typedef uint32_t SegmentId;
typedef uint32_t SegmentOffset;

typedef struct
{
  SegmentId segment_id;
  SegmentOffset segment_offset;
} LogSequenceNumber;

CompareRelation lsn_cmp(LogSequenceNumber lhs, LogSequenceNumber rhs);

// TODO: Instead of keeping a WalSegmentHeader inside of each segment keep a
// separate file with the last written LSN, then when writing data first update
// the segments followed by this pointer file
typedef struct
{
  SegmentOffset last_entry_offset;
} WalSegmentHeader;

typedef struct
{
  StringSlice save_path;

  LogSequenceNumber last_persisted_lsn;
  LogSequenceNumber next_entry_lsn;
  LogSequenceNumber last_entry_lsn;

  /// The segment id at which the start of current_memory resides
  /// current_memory also contains memory_segment_id + 1
  SegmentId memory_segment_id;
  size_t memory_length;
  void *memory;
} WriteAheadLog;

// ----- Write Ahead Log -----

// ----- Disk buffer pool -----

typedef size_t BlockIndex;

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
  DISK_BUFFER_POOL_SAVE_OK,
  DISK_BUFFER_POOL_SAVE_TEMPORARAY_FAILURE,
  DISK_BUFFER_POOL_SAVE_DENIED,
  DISK_BUFFER_POOL_SAVE_NO_SPACE,
  DISK_BUFFER_POOL_SAVE_NO_MEMORY,
  DISK_BUFFER_POOL_SAVE_PROGRAM_ERROR,
  DISK_BUFFER_POOL_SAVE_IO,
} DiskBufferPoolSaveError;

DiskBufferPoolSaveError
disk_buffer_pool_save(DiskBufferPool *pool, WriteAheadLog *log);

typedef enum
{
  RESOURCE_TYPE_RELATION,
} DiskResourceType;

typedef struct
{
  DiskResourceType type;
  int64_t id;
} DiskResource;

typedef enum
{
  DISK_RESOURCE_CREATE_OK,
  DISK_RESOURCE_CREATE_OPENING,
  DISK_RESOURCE_CREATE_STAT,
  DISK_RESOURCE_CREATE_ALREADY_EXISTS,
  DISK_RESOURCE_CREATE_PROGRAM_ERROR,
  DISK_RESOURCE_CREATE_TRUNCATING,
  DISK_RESOURCE_CREATE_CLOSING,
} DiskResourceCreateError;

DiskResourceCreateError disk_buffer_pool_resource_create(
    DiskBufferPool *pool, DiskResource resource, bool32 expect_new);

typedef enum
{
  DISK_RESOURCE_SOFT_DELETE_OK,
  DISK_RESOURCE_SOFT_DELETE_DENIED,
  DISK_RESOURCE_SOFT_DELETE_NO_MEMORY,
  DISK_RESOURCE_SOFT_DELETE_TEMPORARAY_FAILURE,
  DISK_RESOURCE_SOFT_DELETE_DISK_FULL,
  DISK_RESOURCE_SOFT_DELETE_PROGRAM_ERROR,
} DiskResourceSoftDeleteError;

DiskResourceSoftDeleteError disk_buffer_pool_resource_soft_delete(
    DiskBufferPool *pool, DiskResource resource);

typedef enum
{
  DISK_RESOURCE_RESTORE_OK,
  DISK_RESOURCE_RESTORE_DENIED,
  DISK_RESOURCE_RESTORE_NO_MEMORY,
  DISK_RESOURCE_RESTORE_TEMPORARAY_FAILURE,
  DISK_RESOURCE_RESTORE_DISK_FULL,
  DISK_RESOURCE_RESTORE_PROGRAM_ERROR,
} DiskResourceRestoreError;

DiskResourceRestoreError
disk_buffer_pool_resource_restore(DiskBufferPool *pool, DiskResource resource);

typedef enum
{
  DISK_RESOURCE_DELETE_OK,
  DISK_RESOURCE_DELETE_DENIED,
  DISK_RESOURCE_DELETE_NO_MEMORY,
  DISK_RESOURCE_DELETE_TEMPORARAY_FAILURE,
  DISK_RESOURCE_DELETE_PROGRAM_ERROR,
} DiskResourceDeleteError;

DiskResourceDeleteError disk_buffer_pool_resource_delete(
    DiskBufferPool *pool, DiskResource resource, bool32 deleted);

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

  ColumnsLength tuple_length;
  const ColumnType *types;
  int16_t tuple_fixed_size;

  int16_t deleted_records;
  int16_t deleted_variable_data;
} PhysicalRelationIterator;

PhysicalRelationIterator physical_relation_iterator(
    DiskBufferPool *pool,
    RelationId id,
    ColumnsLength tuple_length,
    const ColumnType *types);

PhysicalRelationIteratorStatus
physical_relation_iterator_open(PhysicalRelationIterator *it, BlockIndex index);

PhysicalRelationIteratorStatus
physical_relation_iterate_tuples(PhysicalRelationIterator *it);

PhysicalRelationIteratorStatus
physical_relation_iterator_next_tuple(PhysicalRelationIterator *it);

PhysicalRelationIteratorStatus
physical_relation_iterator_next_block(PhysicalRelationIterator *it);

PhysicalRelationIteratorStatus
physical_relation_iterator_new_block(PhysicalRelationIterator *it);

BlockIndex physical_relation_iterator_block_index(PhysicalRelationIterator *it);

LogSequenceNumber
physical_relation_iterator_block_lsn(PhysicalRelationIterator *it);

Tuple physical_relation_iterator_get(PhysicalRelationIterator *it);

bool32 physical_relation_iterator_is_block_empty(PhysicalRelationIterator *it);

bool32 physical_relation_iterator_insert_tuple_fits(
    PhysicalRelationIterator *it, Tuple tuple);

void physical_relation_iterator_insert(
    PhysicalRelationIterator *it, LogSequenceNumber lsn, Tuple tuple);

PhysicalRelationIteratorStatus physical_relation_iterator_delete(
    PhysicalRelationIterator *it, LogSequenceNumber lsn);

void physical_relation_iterator_close(PhysicalRelationIterator *it);

// ----- Physical Relation -----

// ----- Write Ahead Log -----

typedef enum
{
  WAL_ENTRY_FIRST_ENTRY_SENTINEL,
  WAL_ENTRY_START,
  WAL_ENTRY_CREATE_RELATION_FILE,
  WAL_ENTRY_DELETE_RELATION_FILE,
  WAL_ENTRY_INSERT_TUPLE,
  WAL_ENTRY_DELETE_TUPLE,
  WAL_ENTRY_COMMIT,
  WAL_ENTRY_ABORT,
  WAL_ENTRY_UNDO,
} WalEntryTag;

typedef struct
{
  SegmentOffset previous_entry_offset;
  SegmentOffset entry_length;
  WalEntryTag tag;
} WalEntryHeader;

typedef union
{
  RelationId relation_id;

  struct
  {
    RelationId relation_id;
    BlockIndex block;
    ColumnsLength length;
  } tuple;

  struct
  {
    WalEntryTag tag;
    LogSequenceNumber lsn;
  } undo;
} WalEntryPayload;

typedef struct PACKED
{
  WalEntryHeader header;
  WalEntryPayload payload;
} WalEntry;

typedef enum
{
  WAL_ITERATOR_STATUS_OK,
  WAL_ITERATOR_STATUS_NO_MORE_ENTRIES,
  WAL_ITERATOR_STATUS_ERROR,
} WalIteratorStatus;

typedef struct
{
  WriteAheadLog *log;
  LogSequenceNumber current;

  LogSequenceNumber memory_lsn;
  SegmentOffset bytes_read_into_memory;
  size_t memory_length;
  void *memory;
} WalIterator;

WalIterator wal_iterate(WriteAheadLog *log, void *memory, size_t memory_length);

WalIteratorStatus wal_iterator_open(WalIterator *it, LogSequenceNumber lsn);
WalIteratorStatus wal_iterator_next(WalIterator *it);
WalIteratorStatus wal_iterator_previous(WalIterator *it);

WalEntry *wal_iterator_get(WalIterator *it);

typedef struct
{
  WalEntryPayload *payload;
  void *data;
} WalUndoEntry;

WalUndoEntry wal_iterator_get_undo_entry(WalIterator *it);

Tuple wal_iterator_get_tuple(WalIterator *it, ColumnType *types);

typedef enum
{
  WAL_NEW_OK,
  WAL_NEW_ERROR,
} WalNewError;

WalNewError wal_new(
    WriteAheadLog *log, StringSlice save_path, void *memory, size_t *length);

typedef enum
{
  WAL_WRITE_ENTRY_OK,
  WAL_WRITE_ENTRY_PROGRAM_ERROR,
  // Currently we don't support writing entries that are too large in the log to
  // simplify the implementation. This should not currently pose a problem as we
  // don't support currently large strings/ blobs and insert a single tuple at a
  // time, however when one of these changes this will need to be revised.
  WAL_WRITE_ENTRY_TOO_BIG,
  WAL_WRITE_ENTRY_WRITING_SEGMENT,
  WAL_WRITE_ENTRY_READING_SEGMENT,
} WalWriteEntryError;

typedef struct
{
  WalWriteEntryError error;
  LogSequenceNumber lsn;
} WalWriteResult;

WalWriteResult wal_write_entry(
    WriteAheadLog *log, WalEntry entry, size_t bytes_length, ByteSlice *bytes);

WalWriteResult wal_write_tuple_entry(
    WriteAheadLog *log,
    WalEntryTag entry,
    RelationId relation_id,
    BlockIndex block,
    Tuple tuple);

WalWriteResult wal_commit_transaction(WriteAheadLog *log);

WalWriteResult
wal_abort_transaction(WriteAheadLog *log, void *memory, size_t memory_length);

WalWriteResult wal_recover(WriteAheadLog *log, void *memory, size_t length);

// ----- Write Ahead Log -----

#define PHYSICAL_H
#endif
