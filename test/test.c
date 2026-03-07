#include "database.h"
#include "parser.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>

// include the c files to make the build simpler
#include "parser.c"

#include "database.c"

#include "logical.c"

#include "physical.c"

#define FILE_RETRIES 1000
#define MAX_OPEN_BUFFERS 10

#define USERS_TABLE_NAME "users"
#define SHOPPING_CART_TABLE_NAME "shopping_cart"

void expect_database_insert_tuple(
    LogicalRelationInsertTupleError error,
    Database *db,
    StringSlice relation_name,
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length)
{
  // TODO: Use arena allocator
  size_t data_length = tuple_data_length(tuple_length, types, values);
  char data[data_length] = {};

  assert(
      database_insert_tuple(
          db,
          relation_name,
          tuple_from_data(tuple_length, types, data_length, data, values))
      == error);
}

void assert_database_insert_tuple(
    Database *db,
    StringSlice relation_name,
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length)
{
  expect_database_insert_tuple(
      LOGICAL_RELATION_INSERT_TUPLE_OK,
      db,
      relation_name,
      types,
      values,
      tuple_length);
}

internal void query_iterator_print(QueryIterator query_it)
{
  TupleIterator *it = query_iterator_get_output_iterator(&query_it);
  ColumnsLength tuple_length = tuple_iterator_tuple_length(it);
  for (ColumnsLength i = 0; i < tuple_length; ++i)
  {
    StringSlice name = tuple_iterator_column_name(it, i);
    printf("%.*s", (int)name.length, name.data);

    if (i < tuple_length - 1)
    {
      printf(", ");
    }
  }

  printf("\n---------\n");

  for (; tuple_iterator_valid(it) == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       tuple_iterator_next(it))
  {
    for (ColumnsLength column = 0; column < tuple_length; ++column)
    {
      ColumnValue value = tuple_iterator_column_value(it, column);

      switch (tuple_iterator_column_type(it, column))
      {
      case COLUMN_TYPE_INTEGER:
        printf("%ld", value.integer);
        break;

      case COLUMN_TYPE_STRING:
        printf("%.*s", (int)value.string.length, value.string.data);
        break;

      case COLUMN_TYPE_BOOLEAN:
        printf("%s", value.boolean == 0 ? "false" : "true");
        break;
      }

      if (column < tuple_length - 1)
      {
        printf(", ");
      }
    }
    printf("\n");
  }

  assert(
      tuple_iterator_valid(it)
      == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES);

  printf("\n\n");
}

internal void run_query(Database *db, size_t length, QueryParameter *parameters)
{
  QueryIterator it = {
      .length = 0,
      .iterators = NULL,
  };
  assert(
      query_iterator_new(&db->pool, length, parameters, &it)
      == QUERY_ITERATOR_OK);
  query_iterator_print(it);
  query_iterator_destroy(&it);
}

internal void run_sql_query(Database *db, StringSlice query)
{
  printf("Running SQL:\n%.*s\n\n", (int)query.length, query.data);

  SqlParseResult result = sql_parse_query(query);
  assert(result.error == SQL_PARSE_ERROR_OK);

  run_query(db, result.parameters_length, result.parameters);

  deallocate_sql_parse_result(&result);
}

internal void dump_relations_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name =
               string_slice_from_ptr(relations_relation_name)}});
}

internal void dump_relation_columns_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name =
               string_slice_from_ptr(relation_columns_relation_name)}});
}

internal void dump_users_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)}});
}

internal void dump_shopping_cart_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name =
               string_slice_from_ptr(SHOPPING_CART_TABLE_NAME)}});
}

const ColumnType users_relation_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
    COLUMN_TYPE_BOOLEAN,
};

const bool32 users_relation_primary_keys[] = {
    true,
    false,
    false,
};

const char *const users_relation_names[] = {
    "id",
    "email",
    "is_admin",
};

const ColumnType shopping_cart_relation_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
};

const bool32 shopping_cart_relation_primary_keys[] = {
    true,
    true,
};

const char *const shopping_cart_relation_names[] = {
    "user_id",
    "item",
};

internal void create_users_table(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(users_relation_names[0]),
      string_slice_from_ptr(users_relation_names[1]),
      string_slice_from_ptr(users_relation_names[2]),
  };

  STATIC_ASSERT(
      ARRAY_LENGTH(names_slice) == ARRAY_LENGTH(users_relation_types));

  assert(
      database_create_table(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          names_slice,
          users_relation_types,
          users_relation_primary_keys,
          ARRAY_LENGTH(names_slice))
      == LOGICAL_RELATION_CREATE_OK);
}

internal void create_shopping_cart_table(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(shopping_cart_relation_names[0]),
      string_slice_from_ptr(shopping_cart_relation_names[1]),
  };

  STATIC_ASSERT(
      ARRAY_LENGTH(names_slice) == ARRAY_LENGTH(shopping_cart_relation_types));

  assert(
      database_create_table(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          names_slice,
          shopping_cart_relation_types,
          shopping_cart_relation_primary_keys,
          ARRAY_LENGTH(names_slice))
      == LOGICAL_RELATION_CREATE_OK);
}

internal void drop_table(Database *db)
{
  database_drop_table(db, string_slice_from_ptr(USERS_TABLE_NAME));
}

internal void insert_users(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("user@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values1));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      users_relation_types,
      values1,
      ARRAY_LENGTH(users_relation_types));

  const ColumnValue values2[] = {
      {.integer = 1},
      {.string = string_slice_from_ptr("admin@company")},
      {.boolean = (StoreBoolean) true},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values2));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      users_relation_types,
      values2,
      ARRAY_LENGTH(users_relation_types));

  const ColumnValue values3[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("guest@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values3));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      users_relation_types,
      values3,
      ARRAY_LENGTH(users_relation_types));
}

internal void insert_users_primary_key_violation(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 3},
      {.string = string_slice_from_ptr("someother@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values1));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      users_relation_types,
      values1,
      ARRAY_LENGTH(users_relation_types));

  const ColumnValue values2[] = {
      {.integer = values1[0].integer},
      {.string = string_slice_from_ptr("someotherone@company")},
      {.boolean = (StoreBoolean) true},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values2));
  expect_database_insert_tuple(
      LOGICAL_RELATION_INSERT_TUPLE_PRIMARY_KEY_VIOLATION,
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      users_relation_types,
      values2,
      ARRAY_LENGTH(users_relation_types));
}

internal void insert_shopping_cart_items(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("soda")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values1));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
      shopping_cart_relation_types,
      values1,
      ARRAY_LENGTH(values1));

  const ColumnValue values2[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("bread")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values2));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
      shopping_cart_relation_types,
      values2,
      ARRAY_LENGTH(values2));

  const ColumnValue values3[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("sugar")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values3));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
      shopping_cart_relation_types,
      values3,
      ARRAY_LENGTH(values3));

  const ColumnValue values4[] = {
      {.integer = 1},
      {.string = string_slice_from_ptr("coca powder")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values4));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
      shopping_cart_relation_types,
      values4,
      ARRAY_LENGTH(values4));

  const ColumnValue values5[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("bread")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values5));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
      shopping_cart_relation_types,
      values5,
      ARRAY_LENGTH(values5));

  const ColumnValue values6[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("brown sugar")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values6));
  assert_database_insert_tuple(
      db,
      string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
      shopping_cart_relation_types,
      values6,
      ARRAY_LENGTH(values6));
}

internal void query_read(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      });
}

internal void query_project_by_id(Database *db)
{
  StringSlice project_columns[] = {
      string_slice_from_ptr(users_relation_names[0]),
  };

  QueryParameter parameters[] = {
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_PROJECT,
       .project =
           {
               .query_index = 0,
               .column_names = project_columns,
               .tuple_length = ARRAY_LENGTH(project_columns),
           }},
  };

  run_query(db, ARRAY_LENGTH(parameters), parameters);
}

internal void query_project_by_email(Database *db)
{
  StringSlice project_columns[] = {
      string_slice_from_ptr(users_relation_names[1]),
  };

  QueryParameter parameters[] = {
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_PROJECT,
       .project =
           {
               .query_index = 0,
               .column_names = project_columns,
               .tuple_length = ARRAY_LENGTH(project_columns),
           }},
  };

  run_query(db, ARRAY_LENGTH(parameters), parameters);
}

internal void query_select_email(Database *db)
{
  StringSlice project_columns[] = {
      string_slice_from_ptr(users_relation_names[1]),
  };

  QueryParameter parameters[] = {
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_SELECT,
       .select =
           {
               .query_index = 0,
               .length = 1,
               .conditions =
                   (SelectQueryCondition[]){
                       {
                           .operator = PREDICATE_OPERATOR_STRING_LIKE,
                           .lhs =
                               {
                                   .type = PREDICATE_VARIABLE_TYPE_COLUMN,
                                   .column_name = string_slice_from_ptr(
                                       users_relation_names[1]),
                               },
                           .rhs =
                               {
                                   .type = PREDICATE_VARIABLE_TYPE_CONSTANT,
                                   .constant =
                                       {
                                           .type = COLUMN_TYPE_STRING,
                                           .value.string =
                                               string_slice_from_ptr("user%"),
                                       },
                               },
                       },
                   },
           }},
  };

  run_query(db, ARRAY_LENGTH(parameters), parameters);
}

internal void query_cartesian_product(Database *db)
{
  QueryParameter parameters[] = {
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(SHOPPING_CART_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_CARTESIAN_PRODUCT,
       .cartesian_product = {.lhs_index = 0, .rhs_index = 1}},
  };

  run_query(db, ARRAY_LENGTH(parameters), parameters);
}

internal void multi_stage_query(Database *db)
{
  StringSlice project_columns[] = {
      string_slice_from_ptr(users_relation_names[0]),
      string_slice_from_ptr(users_relation_names[1]),
      string_slice_from_ptr(shopping_cart_relation_names[1]),
  };

  QueryParameter parameters[] = {
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_READ,
       .read_relation_name = string_slice_from_ptr(SHOPPING_CART_TABLE_NAME)},
      {.operator = QUERY_OPERATOR_CARTESIAN_PRODUCT,
       .cartesian_product = {.lhs_index = 0, .rhs_index = 1}},
      {
          .operator = QUERY_OPERATOR_SELECT,
          .select =
              {
                  .query_index = 2,
                  .length = 1,
                  .conditions =
                      (SelectQueryCondition[]){
                          {
                              .operator = PREDICATE_OPERATOR_EQUAL,
                              .lhs =
                                  {
                                      .type = PREDICATE_VARIABLE_TYPE_COLUMN,
                                      .column_name =
                                          string_slice_from_ptr("users.id"),
                                  },
                              .rhs =
                                  {
                                      .type = PREDICATE_VARIABLE_TYPE_COLUMN,
                                      .column_name = string_slice_from_ptr(
                                          "shopping_cart.user_id"),
                                  },
                          },
                      },
              },
      },
      {.operator = QUERY_OPERATOR_PROJECT,
       .project =
           {
               .query_index = 3,
               .column_names = project_columns,
               .tuple_length = ARRAY_LENGTH(project_columns),
           }},
  };

  run_query(db, ARRAY_LENGTH(parameters), parameters);
}

internal void delete_tuples(Database *db)
{
  ColumnsLength tuple_length = 1;
  int16_t column_index = 0;
  ColumnType type = COLUMN_TYPE_INTEGER;
  ColumnValue value = {.integer = 0};

  // TODO: Use arena allocator
  size_t data_length = tuple_data_length(tuple_length, &type, &value);
  char data[data_length] = {};

  assert(
      database_delete_tuples(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          &column_index,
          tuple_from_data(tuple_length, &type, data_length, data, &value))
      == LOGICAL_RELATION_DELETE_TUPLES_OK);
}

void sql_select_star(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users;"));
}

void sql_select_users_id(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT id\n"
          "FROM users;"));
}

void sql_select_users_id_email(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT id, email\n"
          "FROM users;"));
}

void sql_select_from_users_and_shopping_cart(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users, shopping_cart;"));
}

void sql_where_email_prefix_user(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users, shopping_cart\n"
          "WHERE email LIKE 'user%';"));
}

void sql_where_email_or_id_equal(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users, shopping_cart\n"
          "WHERE id = 2 OR email LIKE 'user%';"));
}

void sql_where_email_and_id_equal(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users, shopping_cart\n"
          "WHERE id = 0 AND item LIKE 'b%';"));
}

void sql_manual_join(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users, shopping_cart\n"
          "WHERE users.id = shopping_cart.user_id;"));
}

int main(int argc, char *argv[])
{
  UNUSED(argc);
  UNUSED(argv);

  size_t memory_length = MAX_OPEN_BUFFERS * (PAGE_SIZE + sizeof(MappedBuffer));
  void *data = malloc(memory_length);

  Database db = {};
  {
    char path[LINUX_PATH_MAX];
    StringSlice tmp_path = string_slice_from_ptr("/tmp/db/");
    size_t tmp_path_end_index =
        string_slice_concat(path, 0, LINUX_PATH_MAX, tmp_path, false);

    bool32 found = false;
    for (size_t i = 0; i < FILE_RETRIES && !found; ++i)
    {
      assert(sprintf(path + tmp_path_end_index, "%zu", i) > 0);

      struct stat st = {};
      if (stat(path, &st) == -1)
      {
        break;
      }
    }

    assert(
        mkdir(
            path,
            S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)
        == 0);

    assert(database_new(&db, string_slice_from_ptr(path), data, memory_length));
  }

  printf("Creating users table\n");
  create_users_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);

  printf("Creating shopping cart table\n");
  create_shopping_cart_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);

  printf("Inserting tuples\n");
  insert_users(&db);
  dump_users_table(&db);

  printf("Violating primary key\n");
  insert_users_primary_key_violation(&db);
  dump_users_table(&db);

  insert_shopping_cart_items(&db);
  dump_shopping_cart_table(&db);

  printf("Running basic queries: read users table\n");
  query_read(&db);

  printf("Running basic queries: project by id\n");
  query_project_by_id(&db);

  printf("Running basic queries: project by email\n");
  query_project_by_email(&db);

  printf("Running basic queries: select email with prefix 'user'\n");
  query_select_email(&db);

  printf("Running basic queries: cartesian product\n");
  query_cartesian_product(&db);

  printf("Running basic queries: multi stage\n");
  multi_stage_query(&db);

  sql_select_star(&db);
  sql_select_users_id(&db);
  sql_select_users_id_email(&db);
  sql_select_from_users_and_shopping_cart(&db);
  sql_where_email_prefix_user(&db);
  sql_where_email_or_id_equal(&db);
  sql_where_email_and_id_equal(&db);
  sql_manual_join(&db);

  printf("Deleting user with id 0\n");
  delete_tuples(&db);
  dump_users_table(&db);

  printf("Dropping users table\n");
  drop_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);

  free(data);
}
