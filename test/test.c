#include "linux.h"
#include "logical.h"
#include "parser.h"

#include <stdio.h>

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

  for (; tuple_iterator_valid(it); tuple_iterator_next(it))
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
      }

      if (column < tuple_length - 1)
      {
        printf(", ");
      }
    }
    printf("\n");
  }

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
  StringSlice *select_names = NULL;
  size_t select_length = 0;
  StringSlice *from_names = NULL;
  size_t from_length = 0;
  size_t parameters_length = 0;
  QueryParameter *parameters = NULL;
  assert(
      sql_parse_query(
          query,
          &select_names,
          &select_length,
          &from_names,
          &from_length,
          &parameters,
          &parameters_length)
      == SQL_PARSE_ERROR_OK);

  run_query(db, parameters_length, parameters);

  deallocate(select_names, sizeof(*select_names) * select_length);
  deallocate(from_names, sizeof(*from_names) * from_length);
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
};

const char *const users_relation_names[] = {
    "id",
    "email",
};

const ColumnType shopping_cart_relation_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
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
  };

  STATIC_ASSERT(
      ARRAY_LENGTH(names_slice) == ARRAY_LENGTH(users_relation_types));

  assert(
      database_create_table(
          db,
          string_slice_from_ptr(USERS_TABLE_NAME),
          names_slice,
          users_relation_types,
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
               .operator = PREDICATE_OPERATOR_STRING_LIKE,
               .constant =
                   {
                       .column_name =
                           string_slice_from_ptr(users_relation_names[1]),
                       .value.string = string_slice_from_ptr("user%"),
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
      {.operator = QUERY_OPERATOR_SELECT,
       .select =
           {
               .query_index = 2,
               .operator = PREDICATE_OPERATOR_EQUAL_COLUMNS,
               .two_columns =
                   {
                       .lhs_column_name = string_slice_from_ptr("users.id"),
                       .rhs_column_name =
                           string_slice_from_ptr("shopping_cart.user_id"),
                   },
           }},
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

void select_star_sql(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users;"));
}

void select_id_sql(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT id\n"
          "FROM users;"));
}

void select_id_email_sql(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT id, email\n"
          "FROM users;"));
}

void select_cartesian_product(Database *db)
{
  run_sql_query(
      db,
      string_slice_from_ptr(
          "SELECT *\n"
          "FROM users, shopping_cart;"));
}

int main(int argc, char *argv[])
{
  UNUSED(argc);
  UNUSED(argv);

  Database db = {};
  assert(database_new(&db, 1, 1, 1) == ALLOCATE_OK);

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

  printf("Deleting user with id 0\n");
  delete_tuples(&db);
  dump_users_table(&db);

  printf("SQL parser: select star\n");
  select_star_sql(&db);

  printf("SQL parser: select id\n");
  select_id_sql(&db);

  printf("SQL parser: select id and email\n");
  select_id_email_sql(&db);

  printf("SQL parser: cartesian product\n");
  select_cartesian_product(&db);

  printf("Dropping users table\n");
  drop_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);
}
