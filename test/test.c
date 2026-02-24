#include "logical.h"
#include "parser.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>

#define FILE_RETRIES 1000
#define MAX_OPEN_BUFFERS 10

#define USERS_TABLE_NAME "users"
#define SHOPPING_CART_TABLE_NAME "shopping_cart"

static void query_iterator_print(QueryIterator query_it)
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

  for (; tuple_iterator_valid(it) == RELATION_ITERATOR_STATUS_OK;
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

  assert(tuple_iterator_valid(it) == RELATION_ITERATOR_STATUS_NO_MORE_TUPLES);

  printf("\n\n");
}

static void run_query(Database *db, size_t length, QueryParameter *parameters)
{
  QueryIterator it = {
      .length = 0,
      .iterators = NULL,
  };
  assert(database_query(db, length, parameters, &it) == DATABASE_QUERY_OK);
  query_iterator_print(it);
  query_iterator_destroy(&it);
}

static void run_sql_query(Database *db, StringSlice query)
{
  printf("Running SQL:\n%.*s\n\n", (int)query.length, query.data);

  StringSlice *select_names = NULL;
  size_t select_length = 0;
  StringSlice *from_names = NULL;
  size_t from_length = 0;
  size_t conditions_length = 0;
  ParsedWhereCondition *conditions = NULL;
  size_t parameters_length = 0;
  QueryParameter *parameters = NULL;
  assert(
      sql_parse_query(
          query,
          &select_names,
          &select_length,
          &from_names,
          &from_length,
          &conditions,
          &conditions_length,
          &parameters,
          &parameters_length)
      == SQL_PARSE_ERROR_OK);

  run_query(db, parameters_length, parameters);

  deallocate(select_names, sizeof(*select_names) * select_length);
  deallocate(from_names, sizeof(*from_names) * from_length);
  for (size_t i = 0; i < conditions_length; ++i)
  {
    deallocate(
        conditions[i].conditions, sizeof(*conditions) * conditions[i].length);
  }
  deallocate(conditions, sizeof(*conditions) * conditions_length);
  deallocate(parameters, sizeof(*parameters) * parameters_length);
}

static void dump_relations_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name =
               string_slice_from_ptr(relations_relation_name)}});
}

static void dump_relation_columns_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name =
               string_slice_from_ptr(relation_columns_relation_name)}});
}

static void dump_users_table(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)}});
}

static void dump_shopping_cart_table(Database *db)
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

static void create_users_table(Database *db)
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
      == DATABASE_CREATE_TABLE_OK);
}

static void create_shopping_cart_table(Database *db)
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
      == DATABASE_CREATE_TABLE_OK);
}

static void drop_table(Database *db)
{
  database_drop_table(db, string_slice_from_ptr(USERS_TABLE_NAME));
}

static void insert_users(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("user@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values1));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          users_relation_types,
          values1,
          ARRAY_LENGTH(users_relation_types))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values2[] = {
      {.integer = 1},
      {.string = string_slice_from_ptr("admin@company")},
      {.boolean = (StoreBoolean) true},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values2));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          users_relation_types,
          values2,
          ARRAY_LENGTH(users_relation_types))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values3[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("guest@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values3));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          users_relation_types,
          values3,
          ARRAY_LENGTH(users_relation_types))
      == DATABASE_INSERT_TUPLE_OK);
}

static void insert_users_primary_key_violation(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 3},
      {.string = string_slice_from_ptr("someother@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values1));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          users_relation_types,
          values1,
          ARRAY_LENGTH(users_relation_types))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values2[] = {
      {.integer = values1[0].integer},
      {.string = string_slice_from_ptr("someotherone@company")},
      {.boolean = (StoreBoolean) true},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values2));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          users_relation_types,
          values2,
          ARRAY_LENGTH(users_relation_types))
      == DATABASE_INSERT_TUPLE_PRIMARY_KEY_VIOLATION);
}

static void insert_shopping_cart_items(Database *db)
{
  const ColumnValue values1[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("soda")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values1));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          shopping_cart_relation_types,
          values1,
          ARRAY_LENGTH(values1))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values2[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("bread")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values2));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          shopping_cart_relation_types,
          values2,
          ARRAY_LENGTH(values2))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values3[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("sugar")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values3));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          shopping_cart_relation_types,
          values3,
          ARRAY_LENGTH(values3))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values4[] = {
      {.integer = 1},
      {.string = string_slice_from_ptr("coca powder")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values4));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          shopping_cart_relation_types,
          values4,
          ARRAY_LENGTH(values4))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values5[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("bread")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values5));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          shopping_cart_relation_types,
          values5,
          ARRAY_LENGTH(values5))
      == DATABASE_INSERT_TUPLE_OK);

  const ColumnValue values6[] = {
      {.integer = 2},
      {.string = string_slice_from_ptr("brown sugar")},
  };
  STATIC_ASSERT(
      ARRAY_LENGTH(shopping_cart_relation_types) == ARRAY_LENGTH(values6));
  assert(
      database_insert_tuple(
          db,
          string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
          shopping_cart_relation_types,
          values6,
          ARRAY_LENGTH(values6))
      == DATABASE_INSERT_TUPLE_OK);
}

static void query_read(Database *db)
{
  run_query(
      db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      });
}

static void query_project_by_id(Database *db)
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

static void query_project_by_email(Database *db)
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

static void query_select_email(Database *db)
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

static void query_cartesian_product(Database *db)
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

static void multi_stage_query(Database *db)
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

static void delete_tuples(Database *db)
{
  const ColumnValue values[] = {
      {.integer = 0},
      {.string = string_slice_from_ptr("user@company")},
      {.boolean = (StoreBoolean) false},
  };
  STATIC_ASSERT(ARRAY_LENGTH(users_relation_types) == ARRAY_LENGTH(values));
  assert(
      database_delete_tuples(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          0,
          COLUMN_TYPE_INTEGER,
          (ColumnValue){.integer = 0})
      == DATABASE_DELETE_TUPLES_OK);
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

    assert(
        database_new(&db, string_slice_from_ptr(path), data, memory_length)
        == RELATION_CREATE_OK);
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
