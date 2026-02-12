#include "linux.h"
#include "logical.h"

#include <stdio.h>

#define USERS_TABLE_NAME "users"

void relation_print(Relation relation)
{

  for (ColumnsLength i = 0; i < relation.tuple_length; ++i)
  {
    printf(
        "%.*s",
        (int)relation.names[i].length,
        (char *)&relation.data[relation.names[i].offset]);
    if (i < relation.tuple_length - 1)
    {
      printf(", ");
    }
  }

  printf("\n---------\n");

  for (size_t tuple_index = 0; tuple_index < relation.length; ++tuple_index)
  {
    ColumnValue2 *tuple =
        (tuple_index * relation.tuple_length) + relation.values;

    for (ColumnsLength column = 0; column < relation.tuple_length; ++column)
    {
      switch (relation.types[column])
      {
      case COLUMN_TYPE_INTEGER:
        printf("%ld", tuple[column].integer);
        break;

      case COLUMN_TYPE_STRING:
        printf(
            "%.*s",
            (int)tuple[column].string.length,
            (char *)&relation.data[tuple[column].string.offset]);
        break;
      }

      if (column < relation.tuple_length - 1)
      {
        printf(", ");
      }
    }

    printf("\n");
  }

  printf("\n\n");
}

const ColumnType types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
};

const char *const names[] = {
    "id",
    "email",
};

void create_table(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(names[0]),
      string_slice_from_ptr(names[1]),
  };

  STATIC_ASSERT(ARRAY_LENGTH(names_slice) == ARRAY_LENGTH(types));

  assert(
      database_create_table(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          names_slice,
          types,
          ARRAY_LENGTH(names_slice))
      == DATABASE_CREATE_TABLE_OK);
}

void drop_table(Database *db)
{
  database_drop_table(db, string_slice_from_ptr(USERS_TABLE_NAME));
}

void insert_tuples(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("user@company")},
  };
  STATIC_ASSERT(ARRAY_LENGTH(types) == ARRAY_LENGTH(values1));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          types,
          values1,
          ARRAY_LENGTH(types))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values2[] = {
      {.integer = 1},
      {.string = string_slice_from_ptr("admin@company")},
  };
  STATIC_ASSERT(ARRAY_LENGTH(types) == ARRAY_LENGTH(values2));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          types,
          values2,
          ARRAY_LENGTH(types))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values3[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("guest@company")},
  };
  STATIC_ASSERT(ARRAY_LENGTH(types) == ARRAY_LENGTH(values3));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          types,
          values3,
          ARRAY_LENGTH(types))
      == DATABASE_INSERT_TUPLE_OK);
}

void dump_relations_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(relations_relation_name))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

void dump_relation_columns_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(relation_columns_relation_name))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

void dump_users_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

void project_email(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(names[1]),
  };

  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  assert(
      relation_project(&relation, names_slice, ARRAY_LENGTH(names_slice))
      == RELATION_PROJECT_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

void project_id(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(names[0]),
  };

  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  assert(
      relation_project(&relation, names_slice, ARRAY_LENGTH(names_slice))
      == RELATION_PROJECT_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

void select_id(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  assert(
      relation_select(
          &relation,
          string_slice_from_ptr(names[0]),
          (Predicate){
              .operator = PREDICATE_OPERATOR_EQUAL,
              .value = {.integer = 0},
          })
      == RELATION_SELECT_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

void select_email(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  assert(
      relation_select(
          &relation,
          string_slice_from_ptr(names[1]),
          (Predicate){
              .operator = PREDICATE_OPERATOR_STRING_PREFIX_EQUAL,
              .value = {.string = string_slice_from_ptr("user")},
          })
      == RELATION_SELECT_OK);
  relation_print(relation);
  relation_destroy(&relation);
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
  assert(database_new(&db, 1, 1, 1) == ALLOCATE_OK);

  printf("Creating users table\n");
  create_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);

  printf("Inserting tuples\n");
  insert_tuples(&db);

  dump_users_table(&db);

  printf("Project by email\n");

  project_email(&db);
  printf("Project by id\n");
  project_id(&db);

  printf("Select id is 0\n");
  select_id(&db);

  printf("Select email with prefix 'user'\n");
  select_email(&db);

  printf("Deleting user with id 0\n");
  delete_tuples(&db);
  dump_users_table(&db);

  printf("Dropping users table\n");
  drop_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);
}
