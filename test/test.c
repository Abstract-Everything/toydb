#include "linux.h"
#include "logical.h"

#define USERS_TABLE_NAME "users"

void create_table(Database *db)
{
  const StringSlice names[] = {
      string_slice_from_ptr("id"),
      string_slice_from_ptr("email"),
  };

  const ColumnType types[] = {
      COLUMN_TYPE_INTEGER,
      COLUMN_TYPE_STRING,
  };

  STATIC_ASSERT(ARRAY_LENGTH(names) == ARRAY_LENGTH(types));

  database_create_table(
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      names,
      types,
      ARRAY_LENGTH(names));
}

void drop_table(Database *db)
{
  database_drop_table(db, string_slice_from_ptr(USERS_TABLE_NAME));
}

int main(int argc, char *argv[])
{
  Database db = {};
  if (database_new(&db, 1, 1, 1) == ALLOCATE_OUT_OF_MEMORY)
  {
    exit(PROGRAM_EXIT_ERROR);
  }

  create_table(&db);

  drop_table(&db);
}
