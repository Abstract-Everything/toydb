#ifndef DATABASE_H
#define DATABASE_H

#include "logical.h"

// ----- Database -----

struct Database;
typedef struct Database Database;

bool32 database_new(Database *db, StringSlice path, void *data, size_t length);

LogicalRelationCreateError database_create_table(
    Database *db,
    StringSlice relation_name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length);

LogicalRelationDropError
database_drop_table(Database *db, StringSlice relation_name);

LogicalRelationInsertTupleError database_insert_tuple(
    Database *db,
    StringSlice relation_name,
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length);

LogicalRelationDeleteTuplesError database_delete_tuples(
    Database *db,
    StringSlice relation_name,
    ColumnsLength column_index,
    ColumnType type,
    ColumnValue value);

// ----- Database -----

#endif
