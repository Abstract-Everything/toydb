#ifndef LOGICAL_H

#include "physical.h"
#include "std.h"

// ---------- Schema types ----------

const char *const relations_column_names[] = {
    "relation_id",
    "relation_name",
};

const ColumnType relations_column_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
};

STATIC_ASSERT(
    ARRAY_LENGTH(relations_column_names)
    == ARRAY_LENGTH(relations_column_types));

const char *const relation_columns_column_names[] = {
    "relation_id",
    "column_id",
    "column_name",
    "column_type",
};

const ColumnType relation_columns_column_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
    COLUMN_TYPE_INTEGER, // TODO: Implement enums
};

STATIC_ASSERT(
    ARRAY_LENGTH(relation_columns_column_names)
    == ARRAY_LENGTH(relation_columns_column_types));

// ---------- Schema types ----------

typedef struct
{
  int64_t relation_id;
  MemoryStore store;
} UserRelationData;

typedef struct
{
  // TODO: Switch to permanent storage
  MemoryStore relations;
  MemoryStore relation_columns;
  size_t blocks_per_user_relations;
  size_t user_relations_length;
  UserRelationData *user_relations;
} Database;

static void database_destroy(Database *db)
{
  assert(db != NULL);

  for (size_t i = 0; i < db->user_relations_length; ++i)
  {
    memory_store_destroy(&db->user_relations[i].store);
  }
  deallocate(db->user_relations, db->user_relations_length);

  memory_store_destroy(&db->relations);
  memory_store_destroy(&db->relation_columns);
}

static AllocateError database_new(
    Database *db,
    size_t relations_blocks,
    size_t relation_columns_blocks,
    size_t data_blocks)
{
  assert(db != NULL);
  assert(relations_blocks > 0);
  assert(relation_columns_blocks > 0);
  assert(data_blocks > 0);

  *db = (Database){
      .relations = {},
      .relation_columns = {},
      .blocks_per_user_relations = data_blocks,
      .user_relations_length = 0,
      .user_relations = NULL,
  };

  if (memory_store_new(&db->relations, relations_blocks)
          == ALLOCATE_OUT_OF_MEMORY
      || memory_store_new(&db->relation_columns, relations_blocks)
             == ALLOCATE_OUT_OF_MEMORY)
  {
    database_destroy(db);
    return ALLOCATE_OUT_OF_MEMORY;
  }

  return ALLOCATE_OK;
}

bool32 query_relation_id_by_name(Database db, StringSlice name, int64_t *id)
{
  for (TupleIterator i = memory_store_iterate(&db.relations); i.valid;
       tuple_iterator_next(&i))
  {
    ColumnValue tuple_name = tuple_iterator_get(
        &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 1);
    if (string_slice_eq(name, tuple_name.string))
    {
      ColumnValue tuple_id = tuple_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

      *id = tuple_id.integer;
      return true;
    }
  }
  return false;
}

size_t find_user_relation_index(Database db, int64_t id)
{
  for (size_t i = 0; i < db.user_relations_length; ++i)
  {
    if (db.user_relations[i].relation_id == id)
    {
      return i;
    }
  }

  // Given the query found the relation in the relations table (that's how we
  // get the id parameter), there should always exist a store for it
  assert(false);
  return 0;
}

typedef enum
{
  DATABASE_CREATE_TABLE_OK,
  DATABASE_CREATE_TABLE_OUT_OF_MEMORY,
  DATABASE_CREATE_TABLE_NO_SPACE,
} DatabaseCreateTableError;

static DatabaseCreateTableError database_create_table(
    Database *db,
    StringSlice name,
    StringSlice const *names,
    ColumnType const *types,
    ColumnsLength length)
{
  assert(db != NULL);
  assert(name.length > 0);
  assert(names != NULL);
  assert(types != NULL);
  assert(length > 0);

  int64_t relation_id = 0;
  for (TupleIterator i = memory_store_iterate(&db->relations); i.valid;
       tuple_iterator_next(&i))
  {
    ColumnValue value = tuple_iterator_get(
        &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

    if (relation_id <= value.integer)
    {
      relation_id = value.integer + 1;
    }
  }

  UserRelationData relation_data = {
      .relation_id = relation_id,
      .store = {},
  };
  if (memory_store_new(&relation_data.store, db->blocks_per_user_relations)
      == ALLOCATE_OUT_OF_MEMORY)
  {
    return DATABASE_CREATE_TABLE_OUT_OF_MEMORY;
  }

  // TODO: Implement dynamic array
  if (reallocate(
          sizeof(db->user_relations[0]),
          &db->user_relations, // TODO: warning
          &db->user_relations_length,
          db->user_relations_length + 1)
      == ALLOCATE_OUT_OF_MEMORY)
  {
    return DATABASE_CREATE_TABLE_OUT_OF_MEMORY;
  }
  db->user_relations[db->user_relations_length - 1] = relation_data;

  MemoryStoreInsertTupleError insert_error = MEMORY_STORE_INSERT_TUPLE_OK;
  for (int16_t column = 0;
       column < length && insert_error == MEMORY_STORE_INSERT_TUPLE_OK;
       ++column)
  {
    ColumnValue relation_column_values[] = {
        {.integer = relation_id},
        {.integer = column},
        {.string = names[column]},
        {.integer = types[column]},
    };
    STATIC_ASSERT(
        ARRAY_LENGTH(relation_column_values)
        == ARRAY_LENGTH(relation_columns_column_types));

    insert_error = memory_store_insert_tuple(
        &db->relation_columns,
        relation_columns_column_types,
        relation_column_values,
        ARRAY_LENGTH(relation_column_values));
  }

  if (insert_error == MEMORY_STORE_INSERT_TUPLE_OK)
  {
    ColumnValue relations_values[] = {
        {.integer = relation_id},
        {.string = name},
    };
    STATIC_ASSERT(
        ARRAY_LENGTH(relations_values) == ARRAY_LENGTH(relations_column_types));

    insert_error = memory_store_insert_tuple(
        &db->relations,
        relations_column_types,
        relations_values,
        ARRAY_LENGTH(relations_values));
  }

  if (insert_error != MEMORY_STORE_INSERT_TUPLE_OK)
  {
    memory_store_delete_tuples(
        &db->relation_columns,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        0,
        (ColumnValue){.integer = relation_id});

    memory_store_delete_tuples(
        &db->relation_columns,
        relations_column_types,
        ARRAY_LENGTH(relations_column_types),
        0,
        (ColumnValue){.integer = relation_id});

    memory_store_destroy(
        &db->user_relations[db->user_relations_length - 1].store);
    db->user_relations[db->user_relations_length - 1].relation_id = -1;

    // TODO: Implement dynamic array
    if (reallocate(
            sizeof(db->user_relations[0]),
            &db->user_relations, // TODO: warning
            &db->user_relations_length,
            db->user_relations_length - 1)
        == ALLOCATE_OUT_OF_MEMORY)
    {
      return DATABASE_CREATE_TABLE_OUT_OF_MEMORY;
    }

    return DATABASE_CREATE_TABLE_NO_SPACE;
  }

  return DATABASE_CREATE_TABLE_OK;
}

typedef enum
{
  DATABASE_DROP_TABLE_OK,
  DATABASE_DROP_TABLE_OUT_OF_MEMORY,
  DATABASE_DROP_TABLE_NOT_FOUND,
} DatabaseDropTableError;

static DatabaseDropTableError
database_drop_table(Database *db, StringSlice name)
{
  assert(db != NULL);
  assert(name.length > 0);

  int64_t relation_id = 0;
  if (!query_relation_id_by_name(*db, name, &relation_id))
  {
    return DATABASE_DROP_TABLE_NOT_FOUND;
  }

  size_t relation_index = find_user_relation_index(*db, relation_id);
  if (relation_index != db->user_relations_length - 1)
  {
    size_t last_index = db->user_relations_length - 1;
    UserRelationData temp = db->user_relations[relation_index];
    db->user_relations[relation_index] = db->user_relations[last_index];
    db->user_relations[last_index] = temp;
  }

  // Remove the last element, the previous for loop should make sure that the
  // relation to be removed is always last
  if (reallocate(
          sizeof(db->user_relations[0]),
          &db->user_relations, // TODO: warning
          &db->user_relations_length,
          db->user_relations_length - 1)
      == ALLOCATE_OUT_OF_MEMORY)
  {
    return DATABASE_DROP_TABLE_OUT_OF_MEMORY;
  }

  memory_store_delete_tuples(
      &db->relation_columns,
      relation_columns_column_types,
      ARRAY_LENGTH(relation_columns_column_types),
      0,
      (ColumnValue){.integer = relation_id});

  memory_store_delete_tuples(
      &db->relation_columns,
      relations_column_types,
      ARRAY_LENGTH(relations_column_types),
      0,
      (ColumnValue){.integer = relation_id});

  return DATABASE_DROP_TABLE_OK;
}

static AllocateError database_get_relation_column_types(
    Database *db, int64_t id, ColumnType **types, ColumnsLength *length_)
{
  assert(*types == NULL);
  assert(*length_ == 0);

  size_t length = 0;

  for (TupleIterator i = memory_store_iterate(&db->relation_columns); i.valid;
       tuple_iterator_next(&i))
  {
    ColumnValue tuple_relation_id = tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        0);

    if (tuple_relation_id.integer != id)
    {
      continue;
    }

    ColumnValue tuple_column_type = tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        3);

    if (reallocate(sizeof(ColumnType), (void **)types, &length, length + 1)
        == ALLOCATE_OUT_OF_MEMORY)
    {
      deallocate(*types, length);
      return ALLOCATE_OUT_OF_MEMORY;
    }

    (*types)[length - 1] = tuple_column_type.integer;
  }

  // We don't allow relations without columns to exist
  assert(length > 0);

  *length_ = length;

  return ALLOCATE_OK;
}

typedef enum
{
  DATABASE_INSERT_TUPLE_OK,
  DATABASE_INSERT_TUPLE_OUT_OF_MEMORY,
  DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH,
  DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH,
  DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND,
} DatabaseInsertTupleError;

static DatabaseInsertTupleError database_insert_tuple(
    Database *db,
    StringSlice name,
    const ColumnType *types,
    const ColumnValue *values,
    int16_t length)
{
  int64_t relation_id = 0;
  if (!query_relation_id_by_name(*db, name, &relation_id))
  {
    return DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND;
  }

  ColumnType *columns_types = NULL;
  ColumnsLength columns_length = 0;
  if (database_get_relation_column_types(
          db, relation_id, &columns_types, &columns_length)
      != ALLOCATE_OK)
  {
    return DATABASE_INSERT_TUPLE_OUT_OF_MEMORY;
  }

  if (length != columns_length)
  {
    return DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH;
  }

  for (size_t i = 0; i < length; ++i)
  {
    if (types[i] != columns_types[i])
    {
      deallocate(columns_types, columns_length);
      return DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH;
    }
  }

  deallocate(columns_types, columns_length);

  size_t relation_index = find_user_relation_index(*db, relation_id);

  for (int64_t i = 0; i < length; ++i)
  {
    MemoryStoreInsertTupleError insert_error = memory_store_insert_tuple(
        &db->user_relations[relation_index].store, types, values, length);
    // TODO: Use transactions to handle failure
    assert(insert_error == MEMORY_STORE_INSERT_TUPLE_OK);
  }

  return DATABASE_INSERT_TUPLE_OK;
}

typedef enum
{
  DATABASE_DELETE_TUPLES_OK,
  DATABASE_DELETE_TUPLES_OUT_OF_MEMORY,
  DATABASE_DELETE_TUPLES_COLUMN_INDEX_OUT_OF_RANGE,
  DATABASE_DELETE_TUPLES_COLUMN_TYPE_MISMATCH,
  DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND
} DatabaseDeleteTuplesError;

static DatabaseDeleteTuplesError database_delete_tuples(
    Database *db,
    StringSlice name,
    ColumnsLength column_index,
    ColumnType type,
    ColumnValue value)
{
  int64_t relation_id = 0;
  if (!query_relation_id_by_name(*db, name, &relation_id))
  {
    return DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND;
  }

  ColumnType *columns_types = NULL;
  ColumnsLength columns_length = 0;
  if (database_get_relation_column_types(
          db, relation_id, &columns_types, &columns_length)
      != ALLOCATE_OK)
  {
    return DATABASE_DELETE_TUPLES_OUT_OF_MEMORY;
  }

  if (column_index >= columns_length)
  {
    return DATABASE_DELETE_TUPLES_COLUMN_INDEX_OUT_OF_RANGE;
  }

  if (type != columns_types[column_index])
  {
    return DATABASE_DELETE_TUPLES_COLUMN_TYPE_MISMATCH;
  }

  size_t relation_index = find_user_relation_index(*db, relation_id);

  memory_store_delete_tuples(
      &db->user_relations[relation_index].store,
      columns_types,
      columns_length,
      column_index,
      value);

  deallocate(columns_types, columns_length);

  return DATABASE_DELETE_TUPLES_OK;
}

#define LOGICAL_H
#endif
