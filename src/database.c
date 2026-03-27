#include "database.h"

struct Database
{
  DiskBufferPool pool;
  WriteAheadLog log;
  TransactionStatus transaction_status;
};

bool32 database_new(Database *db, StringSlice path, void *data, size_t length)
{
  assert(db != NULL);
  assert(data != NULL);

  size_t remaining = length;
  switch (wal_new(&db->log, path, data, &remaining))
  {
  case WAL_NEW_OK:
    break;

  case WAL_NEW_ERROR:
    return false;
  }
  assert(remaining > 0);

  disk_buffer_pool_new(&db->pool, path, data + (length - remaining), remaining);

  db->transaction_status = TRANSACTION_STATUS_INACTIVE;

  return relation_create_schema_relations(&db->pool);
}

LogicalRelationRecoverError
database_recover(Database *db, void *memory, size_t memory_length)
{
  switch (db->transaction_status)
  {
  case TRANSACTION_STATUS_INACTIVE:
    return logical_recover(&db->pool, &db->log, memory, memory_length);
    break;

  case TRANSACTION_STATUS_ACTIVE:
  case TRANSACTION_STATUS_REQUIRES_ABORTING:
    assert(false);
    return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;
  }
}

DatabaseCreateError database_create_table(
    Database *db,
    StringSlice name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length)
{
  switch (db->transaction_status)
  {
  case TRANSACTION_STATUS_ACTIVE:
    break;

  case TRANSACTION_STATUS_INACTIVE:
  case TRANSACTION_STATUS_REQUIRES_ABORTING:
    return (DatabaseCreateError){.status = db->transaction_status};
  }

  LogicalRelationCreateError error = logical_relation_create(
      &db->pool, &db->log, name, names, types, primary_keys, tuple_length);

  if (error != LOGICAL_RELATION_CREATE_OK)
  {
    db->transaction_status = TRANSACTION_STATUS_REQUIRES_ABORTING;
  }

  return (DatabaseCreateError){
      .status = db->transaction_status,
      .error = error,
  };
}

DatabaseDropError database_drop_table(Database *db, StringSlice relation_name)
{
  switch (db->transaction_status)
  {
  case TRANSACTION_STATUS_ACTIVE:
    break;

  case TRANSACTION_STATUS_INACTIVE:
  case TRANSACTION_STATUS_REQUIRES_ABORTING:
    return (DatabaseDropError){.status = db->transaction_status};
  }

  LogicalRelationDropError error =
      logical_relation_drop(&db->pool, &db->log, relation_name);

  if (error != LOGICAL_RELATION_DROP_OK)
  {
    db->transaction_status = TRANSACTION_STATUS_REQUIRES_ABORTING;
  }

  return (DatabaseDropError){
      .status = db->transaction_status,
      .error = error,
  };
}

DatabaseInsertTupleError
database_insert_tuple(Database *db, StringSlice relation_name, Tuple tuple)
{
  switch (db->transaction_status)
  {
  case TRANSACTION_STATUS_ACTIVE:
    break;

  case TRANSACTION_STATUS_INACTIVE:
  case TRANSACTION_STATUS_REQUIRES_ABORTING:
    return (DatabaseInsertTupleError){.status = db->transaction_status};
  }

  LogicalRelationInsertTupleError error =
      logical_relation_insert_tuple(&db->pool, &db->log, relation_name, tuple);

  if (error != LOGICAL_RELATION_INSERT_TUPLE_OK)
  {
    db->transaction_status = TRANSACTION_STATUS_REQUIRES_ABORTING;
  }

  return (DatabaseInsertTupleError){
      .status = db->transaction_status,
      .error = error,
  };
}

DatabaseDeleteTuplesError database_delete_tuples(
    Database *db,
    StringSlice relation_name,
    const bool32 *compare_column,
    Tuple tuple)
{
  switch (db->transaction_status)
  {
  case TRANSACTION_STATUS_ACTIVE:
    break;

  case TRANSACTION_STATUS_INACTIVE:
  case TRANSACTION_STATUS_REQUIRES_ABORTING:
    return (DatabaseDeleteTuplesError){.status = db->transaction_status};
  }

  LogicalRelationDeleteTuplesError error = logical_relation_delete_tuples(
      &db->pool, &db->log, relation_name, compare_column, tuple);

  if (error != LOGICAL_RELATION_DELETE_TUPLES_OK)
  {
    db->transaction_status = TRANSACTION_STATUS_REQUIRES_ABORTING;
  }

  return (DatabaseDeleteTuplesError){
      .status = db->transaction_status,
      .error = error,
  };
}

void database_start_transaction(Database *db)
{
  assert(db->transaction_status == TRANSACTION_STATUS_INACTIVE);
  wal_write_entry(&db->log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
  db->transaction_status = TRANSACTION_STATUS_ACTIVE;
}

void database_commit_transaction(Database *db)
{
  assert(db->transaction_status == TRANSACTION_STATUS_ACTIVE);
  wal_commit_transaction(&db->log);
  db->transaction_status = TRANSACTION_STATUS_INACTIVE;
}

DatabaseAbortError
database_abort_transaction(Database *db, void *memory, size_t memory_length)
{
  assert(
      db->transaction_status == TRANSACTION_STATUS_ACTIVE
      || db->transaction_status == TRANSACTION_STATUS_REQUIRES_ABORTING);

  WalWriteResult write_result =
      wal_abort_transaction(&db->log, memory, memory_length);

  switch (write_result.error)
  {
  case WAL_WRITE_ENTRY_OK:
    break;

  case WAL_WRITE_ENTRY_PROGRAM_ERROR:
  case WAL_WRITE_ENTRY_TOO_BIG:
    assert(false);
    return DATABASE_ABORT_PROGRAM_ERROR;

  case WAL_WRITE_ENTRY_WRITING_SEGMENT:
  case WAL_WRITE_ENTRY_READING_SEGMENT:
    return DATABASE_ABORT_IO;
  }

  WalIterator it = wal_iterate(&db->log, memory, memory_length);

  WalIteratorStatus it_status = wal_iterator_open(&it, write_result.lsn);

  for (; it_status == WAL_ITERATOR_STATUS_OK;
       it_status = wal_iterator_next(&it))
  {
    switch (logical_relation_undo(&db->pool, &it))
    {
    case LOGICAL_RELATION_UNDO_OK:
      break;

    case LOGICAL_RELATION_UNDO_PROGRAM_ERROR:
      return DATABASE_ABORT_PROGRAM_ERROR;

    case LOGICAL_RELATION_UNDO_IO:
      return DATABASE_ABORT_IO;

    case LOGICAL_RELATION_UNDO_OUT_OF_MEMORY:
      return DATABASE_ABORT_NO_MEMORY;

    case LOGICAL_RELATION_UNDO_BUFFER_POOL_FULL:
      return DATABASE_ABORT_BUFFER_POOL_FULL;
    }
  }

  switch (it_status)
  {
  case WAL_ITERATOR_STATUS_NO_MORE_ENTRIES:
    db->transaction_status = TRANSACTION_STATUS_INACTIVE;
    return DATABASE_ABORT_OK;

  case WAL_ITERATOR_STATUS_OK:
    assert(false);
    return DATABASE_ABORT_PROGRAM_ERROR;

  case WAL_ITERATOR_STATUS_ERROR:
    return DATABASE_ABORT_IO;
  }
}
