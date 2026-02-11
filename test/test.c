#include "linux.h"
#include "logical.h"

#define USERS_TABLE_NAME "users"

const ColumnType types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
};

void create_table(Database *db)
{
  const StringSlice names[] = {
      string_slice_from_ptr("id"),
      string_slice_from_ptr("email"),
  };

  STATIC_ASSERT(ARRAY_LENGTH(names) == ARRAY_LENGTH(types));

  assert(
      database_create_table(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          names,
          types,
          ARRAY_LENGTH(names))
      == DATABASE_CREATE_TABLE_OK);
}

void drop_table(Database *db)
{
  database_drop_table(db, string_slice_from_ptr(USERS_TABLE_NAME));
}

void insert_tuple(Database *db)
{
  const ColumnValue values[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("user@company")},
  };
  STATIC_ASSERT(ARRAY_LENGTH(types) == ARRAY_LENGTH(values));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          types,
          values,
          ARRAY_LENGTH(types))
      == DATABASE_INSERT_TUPLE_OK);
}

void delete_tuples(Database *db)
{
  const ColumnValue values[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("user@company")},
  };
  STATIC_ASSERT(ARRAY_LENGTH(types) == ARRAY_LENGTH(values));
  assert(
      database_delete_tuples(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          0,
          COLUMN_TYPE_INTEGER,
          (ColumnValue){.integer = 0})
      == DATABASE_DELETE_TUPLES_OK);
}

int main(int argc, char *argv[])
{
  UNUSED(argc);
  UNUSED(argv);

  Database db = {};
  if (database_new(&db, 1, 1, 1) == ALLOCATE_OUT_OF_MEMORY)
  {
    exit(PROGRAM_EXIT_ERROR);
  }

  create_table(&db);

  insert_tuple(&db);

  delete_tuples(&db);

  drop_table(&db);
}
