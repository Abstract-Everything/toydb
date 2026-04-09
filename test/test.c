#include "database.h"
#include "parser.h"
#include <fcntl.h>
#include <stddef.h>
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

void assert_database_create_table(
    Database *db,
    StringSlice relation_name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length)
{
  database_start_transaction(db);
  DatabaseCreateError result = database_create_table(
      db, relation_name, names, types, primary_keys, tuple_length);
  assert(result.status == TRANSACTION_STATUS_ACTIVE);
  assert(result.error == LOGICAL_RELATION_CREATE_OK);
  database_commit_transaction(db);
}

void assert_database_drop_table(Database *db, StringSlice relation_name)
{
  database_start_transaction(db);
  DatabaseDropError result = database_drop_table(db, relation_name);
  assert(result.status == TRANSACTION_STATUS_ACTIVE);
  assert(result.error == LOGICAL_RELATION_DROP_OK);
  database_commit_transaction(db);
}

void assert_database_insert_tuple(
    Database *db,
    StringSlice relation_name,
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length)
{
  size_t data_length = tuple_data_length(tuple_length, types, values);
  char data[data_length] = {};

  database_start_transaction(db);
  DatabaseInsertTupleError result = database_insert_tuple(
      db,
      relation_name,
      tuple_from_data(tuple_length, types, data_length, data, values));
  assert(result.status == TRANSACTION_STATUS_ACTIVE);
  assert(result.error == LOGICAL_RELATION_INSERT_TUPLE_OK);
  database_commit_transaction(db);
}

void assert_database_delete_tuples(
    Database *db,
    StringSlice relation_name,
    const bool32 *compare_column,
    ColumnType type,
    ColumnValue value)
{
  // TODO: Use arena allocator
  size_t data_length = tuple_data_length(1, &type, &value);
  char data[data_length] = {};

  database_start_transaction(db);
  DatabaseDeleteTuplesError result = database_delete_tuples(
      db,
      relation_name,
      compare_column,
      tuple_from_data(1, &type, data_length, data, &value));
  assert(result.status == TRANSACTION_STATUS_ACTIVE);
  assert(result.error == LOGICAL_RELATION_DELETE_TUPLES_OK);
  database_commit_transaction(db);
}

void assert_relation_values(
    DiskBufferPool *pool,
    ColumnsLength relation_tuple_length,
    RelationId relation_id,
    const ColumnType *relation_types,
    size_t values_length,
    const ColumnValue *values)
{
  PhysicalRelationIterator it = physical_relation_iterator(
      pool, relation_id, relation_tuple_length, relation_types);

  PhysicalRelationIteratorStatus status = physical_relation_iterate_tuples(&it);

  assert(
      status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
      || (status == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS
          && values_length == 0));

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = physical_relation_iterator_next_tuple(&it))
  {
    assert((size_t)(it.tuple_index * relation_tuple_length) < values_length);

    Tuple tuple = physical_relation_iterator_get(&it);
    for (ColumnsLength column = 0; column < relation_tuple_length; ++column)
    {
      assert(column_value_eq(
          relation_types[column],
          (values
           + ((ptrdiff_t)(it.tuple_index * relation_tuple_length)))[column],
          tuple_get(tuple, column)));
    }
  }
  assert(status == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS);
  physical_relation_iterator_close(&it);
}

internal void query_iterator_print(QueryIterator query_it)
{
  QueryIteratorStartResult result = query_iterator_start(&query_it);
  assert(
      result.status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
      || result.status == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS);

  TupleIterator *it = result.it;

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

  PhysicalRelationIteratorStatus status = result.status;
  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = tuple_iterator_next(it))
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

  assert(status == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS);

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

internal void drop_table(Database *db)
{
  assert_database_drop_table(db, string_slice_from_ptr(USERS_TABLE_NAME));
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

internal void delete_tuple_user_with_id_0(Database *db)
{
  assert_database_delete_tuples(
      db,
      string_slice_from_ptr(USERS_TABLE_NAME),
      users_relation_primary_keys,
      COLUMN_TYPE_INTEGER,
      (ColumnValue){.integer = 0});
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

void find_empty_path(void *path, size_t length)
{
  StringSlice tmp_path = string_slice_from_ptr("/tmp/db/");
  size_t tmp_path_end_index =
      string_slice_concat(path, 0, length, tmp_path, true);

  assert(
      mkdir(
          tmp_path.data,
          S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)
          == 0
      || errno == EEXIST);

  tmp_path_end_index -= 1; // Remove null terminator

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
}

void test_queries()
{
  char path[LINUX_PATH_MAX];

  size_t memory_length =
      (3 * LOG_SEGMENT_DATA_SIZE)
      + (MAX_OPEN_BUFFERS * (PAGE_SIZE + sizeof(MappedBuffer)));
  void *data = malloc(memory_length);

  find_empty_path(path, ARRAY_LENGTH(path));

  Database db = {};
  assert(database_new(&db, string_slice_from_ptr(path), data, memory_length));

  {
    printf("Creating users table\n");
    const StringSlice names_slice[] = {
        string_slice_from_ptr(users_relation_names[0]),
        string_slice_from_ptr(users_relation_names[1]),
        string_slice_from_ptr(users_relation_names[2]),
    };

    STATIC_ASSERT(
        ARRAY_LENGTH(names_slice) == ARRAY_LENGTH(users_relation_types));

    assert_database_create_table(
        &db,
        string_slice_from_ptr(USERS_TABLE_NAME),
        names_slice,
        users_relation_types,
        users_relation_primary_keys,
        ARRAY_LENGTH(names_slice));
  }

  {
    printf("Creating shopping cart table\n");
    const StringSlice names_slice[] = {
        string_slice_from_ptr(shopping_cart_relation_names[0]),
        string_slice_from_ptr(shopping_cart_relation_names[1]),
    };

    STATIC_ASSERT(
        ARRAY_LENGTH(names_slice)
        == ARRAY_LENGTH(shopping_cart_relation_types));
    assert_database_create_table(
        &db,
        string_slice_from_ptr(SHOPPING_CART_TABLE_NAME),
        names_slice,
        shopping_cart_relation_types,
        shopping_cart_relation_primary_keys,
        ARRAY_LENGTH(names_slice));
  }

  printf("Inserting tuples\n");
  insert_users(&db);

  insert_shopping_cart_items(&db);
  dump_shopping_cart_table(&db);

  printf("Running basic queries: read users table\n");
  run_query(
      &db,
      1,
      (QueryParameter[]){
          {.operator = QUERY_OPERATOR_READ,
           .read_relation_name = string_slice_from_ptr(USERS_TABLE_NAME)},
      });

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

  deallocate(data, memory_length);
}

void test_lsn_operations()
{
  {
    LogSequenceNumber a = (LogSequenceNumber){
        .segment_id = 5,
        .segment_offset = 10,
    };

    LogSequenceNumber b = a;

    assert(lsn_cmp(a, b) == CMP_EQUAL);
  }

  {
    LogSequenceNumber a = (LogSequenceNumber){
        .segment_id = 5,
        .segment_offset = 10,
    };

    LogSequenceNumber b = a;
    b.segment_id += 1;

    assert(lsn_cmp(a, b) == CMP_SMALLER);
    assert(lsn_cmp(b, a) == CMP_GREATER);
  }

  {
    LogSequenceNumber a = (LogSequenceNumber){
        .segment_id = 5,
        .segment_offset = 10,
    };

    LogSequenceNumber b = a;
    b.segment_offset += 1;

    assert(lsn_cmp(a, b) == CMP_SMALLER);
    assert(lsn_cmp(b, a) == CMP_GREATER);
  }

  {
    LogSequenceNumber a = (LogSequenceNumber){
        .segment_id = 5,
        .segment_offset = 10,
    };

    size_t difference =
        (LOG_SEGMENT_DATA_SIZE * 5) + (LOG_SEGMENT_DATA_SIZE / 10);

    assert(lsn_distance(a, lsn_add(a, difference)) == difference);
  }
}

// TODO: Write some meaningful data then check that it was read as expected
WalWriteResult
write_garbage_tuple_entry_with_specific_size(WriteAheadLog *log, size_t size)
{
  assert(size < 3 * LOG_SEGMENT_DATA_SIZE);

  WalEntry entry = (WalEntry){.header.tag = WAL_ENTRY_INSERT_TUPLE};
  ByteSlice slice = (ByteSlice){
      .data = log->memory,
      .length = 3 * LOG_SEGMENT_DATA_SIZE,
  };
  size_t length = wal_entry_length(entry, 1, &slice);
  slice.length -= length - size;

  return wal_write_entry(log, entry, 1, &slice);
}

WriteAheadLog create_test_log(
    char *path, size_t path_length, void *memory, size_t memory_length)
{
  find_empty_path(path, path_length);
  WriteAheadLog log = {};
  wal_new(&log, string_slice_from_ptr(path), memory, &memory_length);
  return log;
}

void test_physical_wal()
{
  char path[LINUX_PATH_MAX];

  size_t memory_length = 3 * LOG_SEGMENT_DATA_SIZE;
  void *log_mem = NULL;
  assert(allocate(&log_mem, memory_length) == ALLOCATE_OK);
  void *scratch_mem = NULL;
  assert(allocate(&scratch_mem, memory_length) == ALLOCATE_OK);

  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    assert(log.memory_segment_id == LSN_MINIMUM.segment_id);
    assert(lsn_cmp(LSN_MINIMUM, log.next_entry_lsn) == CMP_SMALLER);
    assert(lsn_cmp(log.last_persisted_lsn, LSN_MINIMUM) == CMP_EQUAL);
    assert(lsn_cmp(log.last_entry_lsn, LSN_MINIMUM) == CMP_EQUAL);
  }

  // We currently don't support very large tuples see WAL_WRITE_ENTRY_TOO_BIG
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE);

    assert(write_result.error == WAL_WRITE_ENTRY_TOO_BIG);
  }

  // Success writing small entry
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    LogSequenceNumber next_lsn = log.next_entry_lsn;

    WalEntry start = (WalEntry){.header.tag = WAL_ENTRY_START};

    WalWriteResult write_result = wal_write_entry(&log, start, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    assert(lsn_cmp(write_result.lsn, next_lsn) == CMP_EQUAL);

    WalEntry *entry = wal_last_entry(&log);
    assert(entry->header.tag == WAL_ENTRY_START);
    assert(entry->header.entry_length == wal_entry_length(start, 0, NULL));
    assert(entry->header.previous_entry_offset != 0);
  }

  // Success writing two small entry
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    WalEntry *entry = wal_last_entry(&log);
    assert(entry->header.tag == WAL_ENTRY_START);
    // We are writing the same entry twice, therefore they should have the same
    // length
    assert(entry->header.previous_entry_offset == entry->header.entry_length);
  }

  // Success writing entry split between two segments
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    WalEntry *entry = wal_last_entry(&log);
    assert(entry->header.tag == WAL_ENTRY_START);
  }

  // Success three large entries
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = write_garbage_tuple_entry_with_specific_size(
        &log,
        lsn_distance(
            log.next_entry_lsn,
            lsn_add(LSN_MINIMUM, LOG_SEGMENT_DATA_SIZE - 1)));
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    assert(
        lsn_cmp(
            log.next_entry_lsn, lsn_add(LSN_MINIMUM, LOG_SEGMENT_DATA_SIZE - 1))
        == CMP_EQUAL);

    write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    assert(
        lsn_cmp(
            log.next_entry_lsn,
            lsn_add(LSN_MINIMUM, 2 * (LOG_SEGMENT_DATA_SIZE - 1)))
        == CMP_EQUAL);

    write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    assert(
        lsn_cmp(
            log.next_entry_lsn,
            lsn_add(LSN_MINIMUM, 3 * (LOG_SEGMENT_DATA_SIZE - 1)))
        == CMP_EQUAL);

    write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    assert(
        lsn_cmp(
            log.next_entry_lsn,
            lsn_add(LSN_MINIMUM, 4 * (LOG_SEGMENT_DATA_SIZE - 1)))
        == CMP_EQUAL);
  }

  { // Reading single entry after sync
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    assert(wal_sync(&log) == WAL_WRITE_SEGMENT_OK);
    assert(lsn_cmp(log.last_persisted_lsn, log.last_entry_lsn) == CMP_EQUAL);

    WalSegmentHeader header;
    WalReadSegmentResult read_result = wal_read_segment_from_disk(
        log.save_path, LSN_MINIMUM, &header, scratch_mem, memory_length);

    assert(read_result.error == WAL_READ_SEGMENT_OK);
    assert(memory_compare(log.memory, scratch_mem, read_result.data_read));
  }

  { // New instance reads written entries
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    assert(wal_sync(&log) == WAL_WRITE_SEGMENT_OK);

    size_t stl = memory_length;
    WriteAheadLog log_after_write = {};
    assert(
        wal_new(
            &log_after_write, string_slice_from_ptr(path), scratch_mem, &stl)
        == WAL_NEW_OK);

    assert(
        lsn_cmp(log.last_persisted_lsn, log_after_write.last_persisted_lsn)
        == CMP_EQUAL);

    assert(
        lsn_cmp(log.next_entry_lsn, log_after_write.next_entry_lsn)
        == CMP_EQUAL);

    assert(
        lsn_cmp(log.last_entry_lsn, log_after_write.last_entry_lsn)
        == CMP_EQUAL);

    assert(log.memory_segment_id == log_after_write.memory_segment_id);

    assert(memory_compare(
        log.memory,
        log_after_write.memory,
        lsn_distance(wal_memory_lsn(&log), log.next_entry_lsn)));
  }

  // Writing entries
  {
    const char byte_slice_data_raw[] = "onetwothreefourfivesixseveneight";
    ByteSlice byte_slice = (ByteSlice){
        .data = byte_slice_data_raw,
        .length = ARRAY_LENGTH(byte_slice_data_raw),
    };

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      WalWriteResult write_result = wal_write_entry(
          &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_START);
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(last_entry->header.entry_length == sizeof(WalEntryHeader));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      const RelationId relation_id = 0;

      WalWriteResult write_result = wal_write_entry(
          &log,
          (WalEntry){
              .header.tag = WAL_ENTRY_CREATE_RELATION_FILE,
              .payload.relation_id = relation_id,
          },
          0,
          NULL);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_CREATE_RELATION_FILE);
      assert(last_entry->payload.relation_id == relation_id);
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(
          last_entry->header.entry_length
          == sizeof(WalEntryHeader) + sizeof(last_entry->payload.relation_id));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      const RelationId relation_id = 1;

      WalWriteResult write_result = wal_write_entry(
          &log,
          (WalEntry){
              .header.tag = WAL_ENTRY_DELETE_RELATION_FILE,
              .payload.relation_id = relation_id,
          },
          0,
          NULL);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_DELETE_RELATION_FILE);
      assert(last_entry->payload.relation_id == relation_id);
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(
          last_entry->header.entry_length
          == sizeof(WalEntryHeader) + sizeof(last_entry->payload.relation_id));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      const WalEntryPayload payload = (WalEntryPayload){
          .tuple =
              {
                  .block = 1,
                  .relation_id = 2,
                  .length = 3,
              },
      };

      WalWriteResult write_result = wal_write_entry(
          &log,
          (WalEntry){.header.tag = WAL_ENTRY_INSERT_TUPLE, .payload = payload},
          1,
          &byte_slice);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_INSERT_TUPLE);
      assert(wal_entry_payload_eq(
          WAL_ENTRY_INSERT_TUPLE, last_entry->payload, payload));
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(
          last_entry->header.entry_length
          == sizeof(WalEntryHeader) + sizeof(last_entry->payload.tuple)
                 + byte_slice.length);
      assert(memory_compare(
          (void *)last_entry
              + (last_entry->header.entry_length - byte_slice.length),
          byte_slice.data,
          byte_slice.length));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      const WalEntryPayload payload = (WalEntryPayload){
          .tuple =
              {
                  .block = 1,
                  .relation_id = 2,
                  .length = 3,
              },
      };

      WalWriteResult write_result = wal_write_entry(
          &log,
          (WalEntry){.header.tag = WAL_ENTRY_DELETE_TUPLE, .payload = payload},
          1,
          &byte_slice);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_DELETE_TUPLE);
      assert(wal_entry_payload_eq(
          WAL_ENTRY_DELETE_TUPLE, last_entry->payload, payload));
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(
          last_entry->header.entry_length
          == sizeof(WalEntryHeader) + sizeof(last_entry->payload.tuple)
                 + byte_slice.length);
      assert(memory_compare(
          (void *)last_entry
              + (last_entry->header.entry_length - byte_slice.length),
          byte_slice.data,
          byte_slice.length));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      const WalEntryPayload payload = (WalEntryPayload){
          .undo =
              {
                  .lsn = LSN_MINIMUM,
                  .tag = WAL_ENTRY_INSERT_TUPLE,
              },
      };

      WalWriteResult write_result = wal_write_entry(
          &log,
          (WalEntry){.header.tag = WAL_ENTRY_UNDO, .payload = payload},
          1,
          &byte_slice);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_UNDO);
      assert(
          wal_entry_payload_eq(WAL_ENTRY_UNDO, last_entry->payload, payload));
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(
          last_entry->header.entry_length
          == sizeof(WalEntryHeader) + sizeof(last_entry->payload.undo)
                 + byte_slice.length);
      assert(memory_compare(
          (void *)last_entry
              + (last_entry->header.entry_length - byte_slice.length),
          byte_slice.data,
          byte_slice.length));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      WalWriteResult write_result = wal_write_entry(
          &log, (WalEntry){.header.tag = WAL_ENTRY_COMMIT}, 0, NULL);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_COMMIT);
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(last_entry->header.entry_length == sizeof(WalEntryHeader));
    }

    {
      WriteAheadLog log =
          create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

      WalWriteResult write_result = wal_write_entry(
          &log, (WalEntry){.header.tag = WAL_ENTRY_ABORT}, 0, NULL);
      assert(write_result.error == WAL_WRITE_ENTRY_OK);

      WalEntry *last_entry = wal_last_entry(&log);
      assert(last_entry->header.tag == WAL_ENTRY_ABORT);
      assert(
          lsn_distance(log.last_entry_lsn, log.next_entry_lsn)
          == last_entry->header.entry_length);
      assert(last_entry->header.entry_length == sizeof(WalEntryHeader));
    }
  }

  { // commit syncs memory
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    const RelationId relation_id = 0;

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_CREATE_RELATION_FILE,
            .payload.relation_id = relation_id,
        },
        0,
        NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    WalSegmentHeader header;
    WalReadSegmentResult read_result = wal_read_segment_from_disk(
        log.save_path, LSN_MINIMUM, &header, scratch_mem, memory_length);
    assert(read_result.error == WAL_READ_SEGMENT_NOT_FOUND);

    write_result = wal_commit_transaction(&log);

    read_result = wal_read_segment_from_disk(
        log.save_path, LSN_MINIMUM, &header, scratch_mem, memory_length);
    assert(read_result.error == WAL_READ_SEGMENT_OK);
    assert(LSN_MINIMUM.segment_id == write_result.lsn.segment_id);
    assert(header.last_entry_offset == write_result.lsn.segment_offset);
    assert(memory_compare(
        scratch_mem, log.memory, log.next_entry_lsn.segment_offset));
  }

  { // abort writes undo entries
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    LogSequenceNumber entry_lsns[5];

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[0] = write_result.lsn;

    const WalEntryPayload create_relation_paylaod =
        (WalEntryPayload){.relation_id = 1};

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_CREATE_RELATION_FILE,
            .payload = create_relation_paylaod,
        },
        0,
        NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[1] = write_result.lsn;

    const WalEntryPayload delete_relation_paylaod =
        (WalEntryPayload){.relation_id = 2};

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_DELETE_RELATION_FILE,
            .payload = delete_relation_paylaod,
        },
        0,
        NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[2] = write_result.lsn;

    const WalEntryPayload insert_tuple_paylaod = (WalEntryPayload){
        .tuple =
            {
                .block = 1,
                .length = 2,
                .relation_id = 3,
            },
    };
    const char insert_slice_raw[] = "onetwothreefourfivesixseveneight";
    ByteSlice insert_slice = (ByteSlice){
        .data = insert_slice_raw,
        .length = ARRAY_LENGTH(insert_slice_raw),
    };

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_INSERT_TUPLE,
            .payload = insert_tuple_paylaod,
        },
        1,
        &insert_slice);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[3] = write_result.lsn;

    const WalEntryPayload delete_tuple_payload = (WalEntryPayload){
        .tuple =
            {
                .block = 4,
                .length = 5,
                .relation_id = 6,
            },
    };
    const char delete_slice_raw[] = "thgienevesxisevifruofeerhtowteno";
    ByteSlice delete_slice = (ByteSlice){
        .data = delete_slice_raw,
        .length = ARRAY_LENGTH(delete_slice_raw),
    };

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_DELETE_TUPLE,
            .payload = delete_tuple_payload,
        },
        1,
        &delete_slice);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[4] = write_result.lsn;

    write_result = wal_abort_transaction(&log, scratch_mem, memory_length);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    WalIterator it = wal_iterate(&log, scratch_mem, memory_length);
    assert(wal_iterator_open(&it, write_result.lsn) == WAL_ITERATOR_STATUS_OK);

    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_UNDO);
    assert(
        lsn_cmp(wal_iterator_get(&it)->payload.undo.lsn, entry_lsns[4])
        == CMP_EQUAL);
    assert(wal_iterator_get(&it)->payload.undo.tag == WAL_ENTRY_DELETE_TUPLE);
    assert(wal_entry_payload_eq(
        WAL_ENTRY_DELETE_TUPLE,
        delete_tuple_payload,
        *wal_iterator_get_undo_entry(&it).payload));
    assert(memory_compare(
        wal_iterator_get_undo_entry(&it).data,
        delete_slice.data,
        delete_slice.length));

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_UNDO);
    assert(
        lsn_cmp(wal_iterator_get(&it)->payload.undo.lsn, entry_lsns[3])
        == CMP_EQUAL);
    assert(wal_iterator_get(&it)->payload.undo.tag == WAL_ENTRY_INSERT_TUPLE);
    assert(wal_entry_payload_eq(
        WAL_ENTRY_DELETE_TUPLE,
        insert_tuple_paylaod,
        *wal_iterator_get_undo_entry(&it).payload));
    assert(memory_compare(
        wal_iterator_get_undo_entry(&it).data,
        insert_slice.data,
        insert_slice.length));

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_UNDO);
    assert(
        lsn_cmp(wal_iterator_get(&it)->payload.undo.lsn, entry_lsns[2])
        == CMP_EQUAL);
    assert(
        wal_iterator_get(&it)->payload.undo.tag
        == WAL_ENTRY_DELETE_RELATION_FILE);
    assert(wal_entry_payload_eq(
        WAL_ENTRY_DELETE_RELATION_FILE,
        delete_relation_paylaod,
        *wal_iterator_get_undo_entry(&it).payload));

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_UNDO);
    assert(
        lsn_cmp(wal_iterator_get(&it)->payload.undo.lsn, entry_lsns[1])
        == CMP_EQUAL);
    assert(
        wal_iterator_get(&it)->payload.undo.tag
        == WAL_ENTRY_CREATE_RELATION_FILE);
    assert(wal_entry_payload_eq(
        WAL_ENTRY_CREATE_RELATION_FILE,
        create_relation_paylaod,
        *wal_iterator_get_undo_entry(&it).payload));

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_NO_MORE_ENTRIES);
  }

  // Iterating on in-memory entries
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    LogSequenceNumber entry_lsns[3];

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[0] = write_result.lsn;

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_CREATE_RELATION_FILE,
            .payload.relation_id = 0,
        },
        0,
        NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[1] = write_result.lsn;

    write_result = wal_write_entry(
        &log,
        (WalEntry){
            .header.tag = WAL_ENTRY_CREATE_RELATION_FILE,
            .payload.relation_id = 1,
        },
        0,
        NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[2] = write_result.lsn;

    WalIterator it = wal_iterate(&log, scratch_mem, memory_length);
    assert(wal_iterator_open(&it, entry_lsns[0]) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[0]) == CMP_EQUAL);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_START);

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[1]) == CMP_EQUAL);
    assert(
        wal_iterator_get(&it)->header.tag == WAL_ENTRY_CREATE_RELATION_FILE
        && wal_iterator_get(&it)->payload.relation_id == 0);

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[2]) == CMP_EQUAL);
    assert(
        wal_iterator_get(&it)->header.tag == WAL_ENTRY_CREATE_RELATION_FILE
        && wal_iterator_get(&it)->payload.relation_id == 1);

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_NO_MORE_ENTRIES);
    assert(lsn_cmp(it.current, entry_lsns[2]) == CMP_EQUAL);

    assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[1]) == CMP_EQUAL);

    assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[0]) == CMP_EQUAL);

    assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_NO_MORE_ENTRIES);
    assert(lsn_cmp(it.current, entry_lsns[0]) == CMP_EQUAL);
  }

  // Iterating in-memory entries
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    WalWriteResult write_result = wal_write_entry(
        &log, (WalEntry){.header.tag = WAL_ENTRY_START}, 0, NULL);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);

    WalIterator it = wal_iterate(&log, scratch_mem, memory_length);
    assert(wal_iterator_open(&it, write_result.lsn) == WAL_ITERATOR_STATUS_OK);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_START);
  }

  // Iterating on disk and in-memory entries
  {
    WriteAheadLog log =
        create_test_log(path, ARRAY_LENGTH(path), log_mem, memory_length);

    LogSequenceNumber entry_lsns[3];

    WalWriteResult write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[0] = write_result.lsn;

    write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[1] = write_result.lsn;

    write_result = write_garbage_tuple_entry_with_specific_size(
        &log, LOG_SEGMENT_DATA_SIZE - 1);
    assert(write_result.error == WAL_WRITE_ENTRY_OK);
    entry_lsns[2] = write_result.lsn;

    assert(log.memory_segment_id > LSN_MINIMUM.segment_id);

    WalIterator it = wal_iterate(&log, scratch_mem, memory_length);
    assert(wal_iterator_open(&it, entry_lsns[0]) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[0]) == CMP_EQUAL);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_INSERT_TUPLE);

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[1]) == CMP_EQUAL);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_INSERT_TUPLE);

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[2]) == CMP_EQUAL);
    assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_INSERT_TUPLE);

    assert(wal_iterator_next(&it) == WAL_ITERATOR_STATUS_NO_MORE_ENTRIES);
    assert(lsn_cmp(it.current, entry_lsns[2]) == CMP_EQUAL);

    assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[1]) == CMP_EQUAL);

    assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
    assert(lsn_cmp(it.current, entry_lsns[0]) == CMP_EQUAL);

    assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_NO_MORE_ENTRIES);
    assert(lsn_cmp(it.current, entry_lsns[0]) == CMP_EQUAL);
  }

  deallocate(scratch_mem, memory_length);
  deallocate(log_mem, memory_length);
}

size_t modified_buffers_count(Database *db, MappedBufferStatus buffer_status)
{
  size_t count = 0;
  for (size_t i = 0; i < db->pool.buffers_length; ++i)
  {
    MappedBuffer *buffer = mapped_buffer(&db->pool, i);
    if (buffer->status == buffer_status)
    {
      count += 1;
    }
  }
  return count;
}

Database create_test_db(
    char *path, size_t path_length, void *memory, size_t memory_length)
{
  find_empty_path(path, path_length);
  Database db = {};
  assert(database_new(&db, string_slice_from_ptr(path), memory, memory_length));
  return db;
}

Database
create_test_db_existing_path(char *path, void *memory, size_t memory_length)
{
  Database db = {};
  assert(database_new(&db, string_slice_from_ptr(path), memory, memory_length));
  return db;
}

void test_operations()
{
  char path[LINUX_PATH_MAX];

  size_t memory_length =
      (3 * LOG_SEGMENT_DATA_SIZE)
      + (MAX_OPEN_BUFFERS * (PAGE_SIZE + sizeof(MappedBuffer)));
  void *data = malloc(memory_length);
  void *second_data = malloc(memory_length);

  StringSlice table_a_name = string_slice_from_ptr("table_a");
  StringSlice table_b_name = string_slice_from_ptr("table_b");

  const ColumnType table_types[] = {
      COLUMN_TYPE_INTEGER,
      COLUMN_TYPE_STRING,
      COLUMN_TYPE_BOOLEAN,
  };

  const bool32 table_pks[] = {
      true,
      false,
      false,
  };

  const StringSlice table_names[] = {
      string_slice_from_ptr("integer"),
      string_slice_from_ptr("string"),
      string_slice_from_ptr("boolean"),
  };

  ColumnValue table_values[] = {
      (ColumnValue){.integer = 0},
      (ColumnValue){.string = string_slice_from_ptr("zero")},
      (ColumnValue){.boolean = true},

      (ColumnValue){.integer = 1},
      (ColumnValue){.string = string_slice_from_ptr("one")},
      (ColumnValue){.boolean = false},

      (ColumnValue){.integer = 2},
      (ColumnValue){.string = string_slice_from_ptr("two")},
      (ColumnValue){.boolean = true},
  };

  const ColumnsLength table_tuple_length = ARRAY_LENGTH(table_types);

  STATIC_ASSERT(table_tuple_length == ARRAY_LENGTH(table_pks));
  STATIC_ASSERT(table_tuple_length == ARRAY_LENGTH(table_names));
  STATIC_ASSERT(3 * table_tuple_length == ARRAY_LENGTH(table_values));

  { // Creating a new table
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    ColumnValue expected_relations_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.string = table_a_name},
    };

    STATIC_ASSERT(
        ARRAY_LENGTH(relations_types)
        == ARRAY_LENGTH(expected_relations_values));

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        ARRAY_LENGTH(expected_relations_values),
        expected_relations_values);

    ColumnValue expected_relation_columns_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 0},
        (ColumnValue){.string = table_names[0]},
        (ColumnValue){.integer = table_types[0]},
        (ColumnValue){.integer = table_pks[0]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 1},
        (ColumnValue){.string = table_names[1]},
        (ColumnValue){.integer = table_types[1]},
        (ColumnValue){.integer = table_pks[1]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 2},
        (ColumnValue){.string = table_names[2]},
        (ColumnValue){.integer = table_types[2]},
        (ColumnValue){.integer = table_pks[2]},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        ARRAY_LENGTH(expected_relation_columns_values),
        expected_relation_columns_values);
  }

  { // Creating two tables with the same name
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    database_start_transaction(&db);
    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_REQUIRES_ABORTING);
    assert(result.error == LOGICAL_RELATION_CREATE_ALREADY_EXISTS);
  }

  { // Creating two tables with the same name
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);
    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        (bool32[]){
            false,
            false,
            false,
        },
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_REQUIRES_ABORTING);
    assert(result.error == LOGICAL_RELATION_CREATE_NO_PRIMARY_KEY);
  }

  // TODO
  // { // Creating a table with duplicate column name
  //   find_empty_path(path, ARRAY_LENGTH(path));
  //   Database db = {};
  //   assert(database_new(&db, string_slice_from_ptr(path), data,
  //   memory_length));
  //
  //   database_start_transaction(&db);
  //   DatabaseCreateError result = database_create_table(
  //       &db,
  //       table_a_name,
  //       (StringSlice[]){
  //           string_slice_from_ptr("name"),
  //           string_slice_from_ptr("name"),
  //       },
  //       (ColumnType[]){
  //           COLUMN_TYPE_INTEGER,
  //           COLUMN_TYPE_STRING,
  //           COLUMN_TYPE_BOOLEAN,
  //       },
  //       (bool32[]){
  //           true,
  //           false,
  //       },
  //       2);
  //   assert(result.inactive_transaction == false);
  //   assert(result.error == LOGICAL_RELATION_CREATE_DUPLICATE_COLUMN_NAME);
  //   database_commit_transaction(&db);
  // }

  { // Creating two tables
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    assert_database_create_table(
        &db,
        table_b_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    ColumnValue expected_relations_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.string = table_a_name},

        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.string = table_b_name},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        ARRAY_LENGTH(expected_relations_values),
        expected_relations_values);

    ColumnValue expected_relation_columns_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 0},
        (ColumnValue){.string = table_names[0]},
        (ColumnValue){.integer = table_types[0]},
        (ColumnValue){.integer = table_pks[0]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 1},
        (ColumnValue){.string = table_names[1]},
        (ColumnValue){.integer = table_types[1]},
        (ColumnValue){.integer = table_pks[1]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 2},
        (ColumnValue){.string = table_names[2]},
        (ColumnValue){.integer = table_types[2]},
        (ColumnValue){.integer = table_pks[2]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.integer = 0},
        (ColumnValue){.string = table_names[0]},
        (ColumnValue){.integer = table_types[0]},
        (ColumnValue){.integer = table_pks[0]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.integer = 1},
        (ColumnValue){.string = table_names[1]},
        (ColumnValue){.integer = table_types[1]},
        (ColumnValue){.integer = table_pks[1]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.integer = 2},
        (ColumnValue){.string = table_names[2]},
        (ColumnValue){.integer = table_types[2]},
        (ColumnValue){.integer = table_pks[2]},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        ARRAY_LENGTH(expected_relation_columns_values),
        expected_relation_columns_values);
  }

  { // Aborting creation of table
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    database_start_transaction(&db);
    DatabaseCreateError result = database_create_table(
        &db,
        table_b_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);
    assert(
        database_abort_transaction(&db, second_data, memory_length)
        == DATABASE_ABORT_OK);

    ColumnValue expected_relations_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.string = table_a_name},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        ARRAY_LENGTH(expected_relations_values),
        expected_relations_values);

    ColumnValue expected_relation_columns_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 0},
        (ColumnValue){.string = table_names[0]},
        (ColumnValue){.integer = table_types[0]},
        (ColumnValue){.integer = table_pks[0]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 1},
        (ColumnValue){.string = table_names[1]},
        (ColumnValue){.integer = table_types[1]},
        (ColumnValue){.integer = table_pks[1]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS},
        (ColumnValue){.integer = 2},
        (ColumnValue){.string = table_names[2]},
        (ColumnValue){.integer = table_types[2]},
        (ColumnValue){.integer = table_pks[2]},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        ARRAY_LENGTH(expected_relation_columns_values),
        expected_relation_columns_values);
  }

  { // Inserting tuples primary key absent
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    assert_database_insert_tuple(
        &db, table_a_name, table_types, table_values, table_tuple_length);

    size_t data_length =
        tuple_data_length(table_tuple_length, table_types, table_values);
    char data[data_length] = {};

    database_start_transaction(&db);
    DatabaseInsertTupleError result = database_insert_tuple(
        &db,
        table_a_name,
        tuple_from_data(
            table_tuple_length, table_types, data_length, data, table_values));
    assert(result.status == TRANSACTION_STATUS_REQUIRES_ABORTING);
    assert(result.error == LOGICAL_RELATION_INSERT_TUPLE_PRIMARY_KEY_VIOLATION);
  }

  { // Inserting tuples
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    for (size_t i = 0; i < ARRAY_LENGTH(table_values); i += table_tuple_length)
    {
      assert_database_insert_tuple(
          &db, table_a_name, table_types, table_values + i, table_tuple_length);
    }

    assert_relation_values(
        &db.pool,
        table_tuple_length,
        RESERVED_RELATION_IDS,
        table_types,
        ARRAY_LENGTH(table_values),
        table_values);
  }

  { // Aborting Inserting tuples
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    assert_database_insert_tuple(
        &db, table_a_name, table_types, table_values, table_tuple_length);

    ColumnValue aborted_values[] = {
        (ColumnValue){.integer = 1},
        (ColumnValue){.string = string_slice_from_ptr("one")},
        (ColumnValue){.boolean = false},
    };

    size_t data_length =
        tuple_data_length(table_tuple_length, table_types, aborted_values);
    char data[data_length] = {};

    database_start_transaction(&db);
    DatabaseInsertTupleError result = database_insert_tuple(
        &db,
        table_a_name,
        tuple_from_data(
            table_tuple_length,
            table_types,
            data_length,
            data,
            aborted_values));
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_INSERT_TUPLE_OK);
    assert(
        database_abort_transaction(&db, second_data, memory_length)
        == DATABASE_ABORT_OK);

    assert_relation_values(
        &db.pool,
        table_tuple_length,
        RESERVED_RELATION_IDS,
        table_types,
        table_tuple_length,
        table_values);
  }

  { // Deleting tuples
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    for (size_t i = 0; i < ARRAY_LENGTH(table_values); i += table_tuple_length)
    {
      assert_database_insert_tuple(
          &db, table_a_name, table_types, table_values + i, table_tuple_length);
    }

    assert_database_delete_tuples(
        &db, table_a_name, table_pks, table_types[0], table_values[0]);

    assert_relation_values(
        &db.pool,
        table_tuple_length,
        RESERVED_RELATION_IDS,
        table_types,
        2 * table_tuple_length,
        table_values + table_tuple_length);
  }

  { // Aborting deleting tuples
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    for (size_t i = 0; i < ARRAY_LENGTH(table_values); i += table_tuple_length)
    {
      assert_database_insert_tuple(
          &db, table_a_name, table_types, table_values + i, table_tuple_length);
    }

    size_t data_length = tuple_data_length(
        table_tuple_length, table_types, table_values + table_tuple_length);
    char data[data_length] = {};

    database_start_transaction(&db);
    DatabaseDeleteTuplesError result = database_delete_tuples(
        &db,
        table_a_name,
        table_pks,
        tuple_from_data(
            table_tuple_length,
            table_types,
            data_length,
            data,
            table_values + table_tuple_length));
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_DELETE_TUPLES_OK);
    assert(
        database_abort_transaction(&db, second_data, memory_length)
        == DATABASE_ABORT_OK);

    ColumnValue expected_values[] = {
        table_values[0],
        table_values[1],
        table_values[2],
        table_values[0 + (2 * table_tuple_length)],
        table_values[1 + (2 * table_tuple_length)],
        table_values[2 + (2 * table_tuple_length)],
        table_values[0 + (table_tuple_length)],
        table_values[1 + (table_tuple_length)],
        table_values[2 + (table_tuple_length)],
    };
    assert_relation_values(
        &db.pool,
        table_tuple_length,
        RESERVED_RELATION_IDS,
        table_types,
        ARRAY_LENGTH(expected_values),
        expected_values);
  }

  { // Dropping table
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    assert_database_create_table(
        &db,
        table_b_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    assert_database_drop_table(&db, table_a_name);

    ColumnValue expected_relations_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.string = table_b_name},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        ARRAY_LENGTH(expected_relations_values),
        expected_relations_values);

    ColumnValue expected_relation_columns_values[] = {
        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.integer = 0},
        (ColumnValue){.string = table_names[0]},
        (ColumnValue){.integer = table_types[0]},
        (ColumnValue){.integer = table_pks[0]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.integer = 1},
        (ColumnValue){.string = table_names[1]},
        (ColumnValue){.integer = table_types[1]},
        (ColumnValue){.integer = table_pks[1]},

        (ColumnValue){.integer = RESERVED_RELATION_IDS + 1},
        (ColumnValue){.integer = 2},
        (ColumnValue){.string = table_names[2]},
        (ColumnValue){.integer = table_types[2]},
        (ColumnValue){.integer = table_pks[2]},
    };

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        ARRAY_LENGTH(expected_relation_columns_values),
        expected_relation_columns_values);

    // TODO: Assert that querying relation returns not found
  }

  { // Aborting Dropping table
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    assert_database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    assert_database_create_table(
        &db,
        table_b_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);

    for (size_t i = 0; i < ARRAY_LENGTH(table_values); i += table_tuple_length)
    {
      assert_database_insert_tuple(
          &db, table_a_name, table_types, table_values + i, table_tuple_length);
    }

    database_start_transaction(&db);
    DatabaseDropError result = database_drop_table(&db, table_a_name);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_DROP_OK);
    assert(
        database_abort_transaction(&db, second_data, memory_length)
        == DATABASE_ABORT_OK);

    assert_relation_values(
        &db.pool,
        ARRAY_LENGTH(table_types),
        RESERVED_RELATION_IDS,
        table_types,
        ARRAY_LENGTH(table_values),
        table_values);
  }

  { // Test without saving WAL buffers are not synced
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);
    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    size_t count = modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED);

    assert(
        disk_buffer_pool_save(&db.pool, &db.log) == DISK_BUFFER_POOL_SAVE_OK);

    assert(count == modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED));
  }

  { // Test Saving Wal allows buffers to be synced
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);
    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    size_t count = modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED);

    wal_sync(&db.log);

    assert(
        disk_buffer_pool_save(&db.pool, &db.log) == DISK_BUFFER_POOL_SAVE_OK);

    assert(
        count == modified_buffers_count(&db, MAPPED_BUFFER_STATUS_ALLOCATED));
  }

  { // Test buffers with lsns ahead of the persisted lsns are not saved
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);
    DatabaseCreateError create_result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(create_result.status == TRANSACTION_STATUS_ACTIVE);
    assert(create_result.error == LOGICAL_RELATION_CREATE_OK);

    size_t data_length =
        tuple_data_length(table_tuple_length, table_types, table_values);
    char data[data_length] = {};

    size_t create_modified_buffers =
        modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED);

    DatabaseInsertTupleError insert_result = database_insert_tuple(
        &db,
        table_a_name,
        tuple_from_data(
            table_tuple_length, table_types, data_length, data, table_values));
    assert(insert_result.status == TRANSACTION_STATUS_ACTIVE);
    assert(insert_result.error == LOGICAL_RELATION_INSERT_TUPLE_OK);

    size_t insert_modified_buffers =
        modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED);

    wal_sync(&db.log);

    create_result = database_create_table(
        &db,
        table_b_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(create_result.status == TRANSACTION_STATUS_ACTIVE);
    assert(create_result.error == LOGICAL_RELATION_CREATE_OK);

    size_t last_create_modified_buffers =
        modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED);

    assert(
        disk_buffer_pool_save(&db.pool, &db.log) == DISK_BUFFER_POOL_SAVE_OK);

    assert(
        insert_modified_buffers - create_modified_buffers
        == modified_buffers_count(&db, MAPPED_BUFFER_STATUS_ALLOCATED));

    assert(
        last_create_modified_buffers
            - (insert_modified_buffers - create_modified_buffers)
        == modified_buffers_count(&db, MAPPED_BUFFER_STATUS_MODIFIED));
  }

  size_t recover_memory_length = 2 * LOG_SEGMENT_DATA_SIZE;
  void *recover_memory = NULL;
  assert(allocate(&recover_memory, recover_memory_length) == ALLOCATE_OK);

  { // Test recovery of empty log
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    LogSequenceNumber last_entry_lsn = db.log.last_entry_lsn;

    wal_sync(&db.log);

    assert(database_new(&db, string_slice_from_ptr(path), data, memory_length));

    assert(
        database_recover(&db, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert(lsn_cmp(db.log.last_entry_lsn, last_entry_lsn) == CMP_EQUAL);
  }

  { // Test recovery of start entry
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);
    database_start_transaction(&db);

    LogSequenceNumber last_entry_lsn = db.log.last_entry_lsn;
    LogSequenceNumber next_entry_lsn = db.log.next_entry_lsn;

    wal_sync(&db.log);

    assert(database_new(&db, string_slice_from_ptr(path), data, memory_length));

    assert(
        database_recover(&db, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert(lsn_cmp(db.log.last_entry_lsn, next_entry_lsn) == CMP_EQUAL);
    assert(wal_last_entry(&db.log)->header.tag == WAL_ENTRY_ABORT);
  }

  { // Test recovery of commited changes
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);

    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    for (size_t i = 0; i < ARRAY_LENGTH(table_values); i += table_tuple_length)
    {
      ColumnValue *values = table_values + i;
      size_t data_length =
          tuple_data_length(table_tuple_length, table_types, values);
      char data[data_length] = {};

      DatabaseInsertTupleError result = database_insert_tuple(
          &db,
          table_a_name,
          tuple_from_data(
              table_tuple_length, table_types, data_length, data, values));

      assert(result.status == TRANSACTION_STATUS_ACTIVE);
      assert(result.error == LOGICAL_RELATION_INSERT_TUPLE_OK);
    }
    database_commit_transaction(&db);

    disk_buffer_pool_save(&db.pool, &db.log);

    Database db_recover =
        create_test_db_existing_path(path, second_data, memory_length);

    assert(
        lsn_cmp(db.log.next_entry_lsn, db_recover.log.next_entry_lsn)
        == CMP_EQUAL);
    assert(memory_compare(
        db.log.memory,
        db_recover.log.memory,
        lsn_distance(LSN_MINIMUM, db.log.next_entry_lsn)));

    assert(
        database_recover(&db, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert_relation_values(
        &db_recover.pool,
        table_tuple_length,
        RESERVED_RELATION_IDS,
        table_types,
        ARRAY_LENGTH(table_values),
        table_values);
  }

  { // Test recovery of committed changes with unsaved buffers
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);

    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    for (size_t i = 0; i < ARRAY_LENGTH(table_values); i += table_tuple_length)
    {
      ColumnValue *values = table_values + i;
      size_t data_length =
          tuple_data_length(table_tuple_length, table_types, values);
      char data[data_length] = {};

      DatabaseInsertTupleError result = database_insert_tuple(
          &db,
          table_a_name,
          tuple_from_data(
              table_tuple_length, table_types, data_length, data, values));

      assert(result.status == TRANSACTION_STATUS_ACTIVE);
      assert(result.error == LOGICAL_RELATION_INSERT_TUPLE_OK);
    }
    database_commit_transaction(&db);

    Database db_recover =
        create_test_db_existing_path(path, second_data, memory_length);

    assert(
        lsn_cmp(db.log.next_entry_lsn, db_recover.log.next_entry_lsn)
        == CMP_EQUAL);
    assert(memory_compare(
        db.log.memory,
        db_recover.log.memory,
        lsn_distance(LSN_MINIMUM, db.log.next_entry_lsn)));

    assert(
        database_recover(&db_recover, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert_relation_values(
        &db_recover.pool,
        table_tuple_length,
        RESERVED_RELATION_IDS,
        table_types,
        ARRAY_LENGTH(table_values),
        table_values);
  }

  { // Test recovery of uncommitted changes aborts transaction
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);

    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    wal_sync(&db.log);

    Database db_recover =
        create_test_db_existing_path(path, second_data, memory_length);

    assert(
        lsn_cmp(db.log.next_entry_lsn, db_recover.log.next_entry_lsn)
        == CMP_EQUAL);
    assert(memory_compare(
        db.log.memory,
        db_recover.log.memory,
        lsn_distance(LSN_MINIMUM, db.log.next_entry_lsn)));

    assert(
        database_recover(&db_recover, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert_relation_values(
        &db_recover.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        0,
        NULL);

    assert_relation_values(
        &db_recover.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        0,
        NULL);
  }

  { // Test recovery of uncommitted changes aborts transaction
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);

    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    wal_sync(&db.log);
    disk_buffer_pool_save(&db.pool, &db.log);

    Database db_recover =
        create_test_db_existing_path(path, second_data, memory_length);

    assert(
        lsn_cmp(db.log.next_entry_lsn, db_recover.log.next_entry_lsn)
        == CMP_EQUAL);
    assert(memory_compare(
        db.log.memory,
        db_recover.log.memory,
        lsn_distance(LSN_MINIMUM, db.log.next_entry_lsn)));

    assert(
        database_recover(&db_recover, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert_relation_values(
        &db_recover.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        0,
        NULL);

    assert_relation_values(
        &db_recover.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        0,
        NULL);
  }

  { // Test recovery of partially written undo
    Database db = create_test_db(path, ARRAY_LENGTH(path), data, memory_length);

    database_start_transaction(&db);

    DatabaseCreateError result = database_create_table(
        &db,
        table_a_name,
        table_names,
        table_types,
        table_pks,
        table_tuple_length);
    assert(result.status == TRANSACTION_STATUS_ACTIVE);
    assert(result.error == LOGICAL_RELATION_CREATE_OK);

    wal_abort_transaction(&db.log, recover_memory, recover_memory_length);

    WalIterator it =
        wal_iterate(&db.log, recover_memory, recover_memory_length);

    assert(
        wal_iterator_open(&it, db.log.last_entry_lsn)
        == WAL_ITERATOR_STATUS_OK);

    LogSequenceNumber complete_undo_next_entry = db.log.next_entry_lsn;
    LogSequenceNumber complete_undo_last_entry = db.log.last_entry_lsn;
    {
      assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
      LogSequenceNumber new_next_entry = it.current;
      assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
      LogSequenceNumber new_last_entry = it.current;
      assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_UNDO);

      db.log.next_entry_lsn = new_next_entry;
      db.log.last_entry_lsn = new_last_entry;

      assert(wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK);
      assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_UNDO);
    }

    wal_sync(&db.log);

    disk_buffer_pool_save(&db.pool, &db.log);

    Database db_recover =
        create_test_db_existing_path(path, second_data, memory_length);

    assert(
        lsn_cmp(db.log.next_entry_lsn, db_recover.log.next_entry_lsn)
        == CMP_EQUAL);

    assert(memory_compare(
        db.log.memory,
        db_recover.log.memory,
        lsn_distance(LSN_MINIMUM, db.log.next_entry_lsn)));

    assert(
        database_recover(&db_recover, recover_memory, recover_memory_length)
        == LOGICAL_RELATION_RECOVER_OK);

    assert(
        lsn_cmp(db_recover.log.next_entry_lsn, complete_undo_next_entry)
        == CMP_EQUAL);

    assert(
        lsn_cmp(db_recover.log.last_entry_lsn, complete_undo_last_entry)
        == CMP_EQUAL);

    assert_relation_values(
        &db_recover.pool,
        ARRAY_LENGTH(relations_types),
        RELATIONS_RELATION_ID,
        relations_types,
        0,
        NULL);

    assert_relation_values(
        &db_recover.pool,
        ARRAY_LENGTH(relation_columns_types),
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        0,
        NULL);
  }

  free(recover_memory);
  free(second_data);
  free(data);
}

int main(int argc, char *argv[])
{
  UNUSED(argc);
  UNUSED(argv);

  test_lsn_operations();
  test_physical_wal();
  test_operations();
  test_queries();
}
