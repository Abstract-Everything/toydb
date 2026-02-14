#include "linux.h"
#include "logical.h"

#include <stdio.h>

#define USERS_TABLE_NAME "users"
#define SHOPPING_CART_TABLE_NAME "shopping_cart"

static void relation_print(Relation relation)
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

static void run_query(
    Database *db,
    size_t length,
    QueryOperator *operators,
    QueryParameter *parameters)
{
  QueryIterator it = {
      .length = 0,
      .iterators = NULL,
  };
  assert(
      database_query(db, length, operators, parameters, &it)
      == DATABASE_QUERY_OK);
  query_iterator_print(it);
  query_iterator_destroy(&it);
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

static void dump_relations_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(relations_relation_name))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

static void dump_relation_columns_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(relation_columns_relation_name))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

static void dump_users_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

static void dump_shopping_cart_table(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(SHOPPING_CART_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

static void project_email(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(users_relation_names[1]),
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

static void project_id(Database *db)
{
  const StringSlice names_slice[] = {
      string_slice_from_ptr(users_relation_names[0]),
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

static void select_id(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  assert(
      relation_select(
          &relation,
          (Predicate){
              .operator = PREDICATE_OPERATOR_EQUAL_CONSTANT,
              .constant =
                  {
                      .column_name =
                          string_slice_from_ptr(users_relation_names[0]),
                      .value.integer = 0,
                  },
          })
      == RELATION_SELECT_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

static void select_email(Database *db)
{
  Relation relation = {};
  assert(
      database_read_relation(
          db, &relation, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);
  assert(
      relation_select(
          &relation,
          (Predicate){
              .operator = PREDICATE_OPERATOR_STRING_PREFIX_EQUAL,
              .constant =
                  {
                      .column_name =
                          string_slice_from_ptr(users_relation_names[1]),
                      .value.string = string_slice_from_ptr("user"),
                  },
          })
      == RELATION_SELECT_OK);
  relation_print(relation);
  relation_destroy(&relation);
}

static void cartesian_product(Database *db)
{
  Relation users = {};
  assert(
      database_read_relation(
          db, &users, string_slice_from_ptr(USERS_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);

  Relation cart = {};
  assert(
      database_read_relation(
          db, &cart, string_slice_from_ptr(SHOPPING_CART_TABLE_NAME))
      == DATABASE_READ_RELATION_OK);

  Relation product = {};
  assert(
      relation_cartesian_product(users, cart, &product)
      == RELATION_CARTESIAN_PRODUCT_OK);

  relation_print(product);

  relation_destroy(&users);
  relation_destroy(&cart);
  relation_destroy(&product);
}

static void query_read(Database *db)
{
  QueryOperator operators[] = {
      QUERY_OPERATOR_READ,
  };

  QueryParameter parameters[] = {
      {.read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
  };

  STATIC_ASSERT(ARRAY_LENGTH(operators) == ARRAY_LENGTH(parameters));

  run_query(db, ARRAY_LENGTH(operators), operators, parameters);
}

static void query_project_by_id(Database *db)
{
  StringSlice project_columns[] = {
      string_slice_from_ptr(users_relation_names[0]),
  };

  QueryOperator operators[] = {
      QUERY_OPERATOR_READ,
      QUERY_OPERATOR_PROJECT,
  };

  QueryParameter parameters[] = {
      {.read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.project =
           {
               .query_index = 0,
               .column_names = project_columns,
               .tuple_length = ARRAY_LENGTH(project_columns),
           }},
  };

  STATIC_ASSERT(ARRAY_LENGTH(operators) == ARRAY_LENGTH(parameters));

  run_query(db, ARRAY_LENGTH(operators), operators, parameters);
}

static void query_project_by_email(Database *db)
{
  StringSlice project_columns[] = {
      string_slice_from_ptr(users_relation_names[1]),
  };

  QueryOperator operators[] = {
      QUERY_OPERATOR_READ,
      QUERY_OPERATOR_PROJECT,
  };

  QueryParameter parameters[] = {
      {.read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      {.project =
           {
               .query_index = 0,
               .column_names = project_columns,
               .tuple_length = ARRAY_LENGTH(project_columns),
           }},
  };

  STATIC_ASSERT(ARRAY_LENGTH(operators) == ARRAY_LENGTH(parameters));
  run_query(db, ARRAY_LENGTH(operators), operators, parameters);
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

  printf("Project by email\n");

  project_email(&db);
  printf("Project by id\n");
  project_id(&db);

  printf("Select id is 0\n");
  select_id(&db);

  printf("Select email with prefix 'user'\n");
  select_email(&db);

  printf("Performing cartesian product on users and shopping cart\n");
  cartesian_product(&db);

  printf("Running basic queries: read users table\n");
  query_read(&db);

  printf("Running basic queries: project by id\n");
  query_project_by_id(&db);

  printf("Running basic queries: project by email\n");
  query_project_by_email(&db);

  printf("Deleting user with id 0\n");
  delete_tuples(&db);
  dump_users_table(&db);

  printf("Dropping users table\n");
  drop_table(&db);
  dump_relations_table(&db);
  dump_relation_columns_table(&db);
}
