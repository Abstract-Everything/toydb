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
  // TODO: Switch to permanent storage
  MemoryStore relations;
  MemoryStore relation_columns;
} Database;

static void database_destroy(Database *db)
{
  assert(db != NULL);

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

typedef enum
{
  DATABASE_CREATE_TABLE_OK,
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

    return DATABASE_CREATE_TABLE_NO_SPACE;
  }

  return DATABASE_CREATE_TABLE_OK;
}

typedef enum
{
  DATABASE_DROP_TABLE_OK,
  DATABASE_DROP_TABLE_NOT_FOUND,
} DatabaseDropTableError;

static DatabaseDropTableError
database_drop_table(Database *db, StringSlice name)
{
  assert(db != NULL);
  assert(name.length > 0);

  bool32 found = false;
  int64_t relation_id = 0;
  for (TupleIterator i = memory_store_iterate(&db->relations); i.valid;
       tuple_iterator_next(&i))
  {
    ColumnValue tuple_name = tuple_iterator_get(
        &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 1);
    if (string_slice_eq(name, tuple_name.string))
    {
      ColumnValue tuple_id = tuple_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

      relation_id = tuple_id.integer;
      found = true;
      break;
    }
  }

  if (!found)
  {
    return DATABASE_DROP_TABLE_NOT_FOUND;
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

#define LOGICAL_H
#endif
