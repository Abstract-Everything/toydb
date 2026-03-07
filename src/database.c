#include "database.h"

struct Database
{
  DiskBufferPool pool;
};

bool32 database_new(Database *db, StringSlice path, void *data, size_t length)
{
  assert(db != NULL);
  assert(data != NULL);
  assert(length > 0);

  disk_buffer_pool_new(&db->pool, path, data, length);

  return relation_create_schema_relations(&db->pool);
}

LogicalRelationCreateError database_create_table(
    Database *db,
    StringSlice name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length)
{
  return logical_relation_create(
      &db->pool, name, names, types, primary_keys, tuple_length);
}

LogicalRelationDropError
database_drop_table(Database *db, StringSlice relation_name)
{
  return logical_relation_drop(&db->pool, relation_name);
}

LogicalRelationInsertTupleError
database_insert_tuple(Database *db, StringSlice relation_name, Tuple tuple)
{
  return logical_relation_insert_tuple(&db->pool, relation_name, tuple);
}

LogicalRelationDeleteTuplesError database_delete_tuples(
    Database *db,
    StringSlice relation_name,
    ColumnsLength *column_indices,
    Tuple tuple)
{
  return logical_relation_delete_tuples(
      &db->pool, relation_name, column_indices, tuple);
}
