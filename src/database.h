#ifndef DATABASE_H
#define DATABASE_H

#include "logical.h"

// ----- Database -----

typedef enum
{
  TRANSACTION_STATUS_INACTIVE,
  TRANSACTION_STATUS_ACTIVE,
  TRANSACTION_STATUS_REQUIRES_ABORTING,
} TransactionStatus;

struct Database;
typedef struct Database Database;

bool32 database_new(Database *db, StringSlice path, void *data, size_t length);

LogicalRelationRecoverError
database_recover(Database *db, void *memory, size_t memory_length);

typedef struct
{
  TransactionStatus status;
  LogicalRelationCreateError error;
} DatabaseCreateError;

DatabaseCreateError database_create_table(
    Database *db,
    StringSlice relation_name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length);

typedef struct
{
  TransactionStatus status;
  LogicalRelationDropError error;
} DatabaseDropError;

DatabaseDropError database_drop_table(Database *db, StringSlice relation_name);

typedef struct
{
  TransactionStatus status;
  LogicalRelationInsertTupleError error;
} DatabaseInsertTupleError;

DatabaseInsertTupleError
database_insert_tuple(Database *db, StringSlice relation_name, Tuple tuple);

typedef struct
{
  TransactionStatus status;
  LogicalRelationDeleteTuplesError error;
} DatabaseDeleteTuplesError;

DatabaseDeleteTuplesError database_delete_tuples(
    Database *db,
    StringSlice relation_name,
    const bool32 *compare_column,
    Tuple tuple);

void database_start_transaction(Database *db);

void database_commit_transaction(Database *db);

typedef enum
{
  DATABASE_ABORT_OK,
  DATABASE_ABORT_IO,
  DATABASE_ABORT_PROGRAM_ERROR,
  DATABASE_ABORT_NO_MEMORY,
  DATABASE_ABORT_BUFFER_POOL_FULL,
} DatabaseAbortError;

DatabaseAbortError
database_abort_transaction(Database *db, void *memory, size_t memory_length);

// ----- Database -----

#endif
