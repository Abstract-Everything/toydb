#ifndef LOGICAL_H

#include "physical.h"
#include "std.h"

#define RELATION_NAME_QUALIFIER '.'

// ---------- Schema types ----------

const char *const relations_relation_name = "relations";

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

const char *const relation_columns_relation_name = "relation_columns";

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
  deallocate(
      db->user_relations,
      sizeof(db->user_relations[0]) * db->user_relations_length);

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

static bool32
query_relation_id_by_name(Database db, StringSlice name, int64_t *id)
{
  for (BlockTupleIterator i = memory_store_iterate(&db.relations); i.valid;
       block_tuple_iterator_next(&i))
  {
    ColumnValue tuple_name = block_tuple_iterator_get(
        &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 1);
    if (string_slice_eq(name, tuple_name.string))
    {
      ColumnValue tuple_id = block_tuple_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

      *id = tuple_id.integer;
      return true;
    }
  }
  return false;
}

static size_t find_user_relation_index(Database db, int64_t id)
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

// TODO: Check that column names are unique
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
  for (BlockTupleIterator i = memory_store_iterate(&db->relations); i.valid;
       block_tuple_iterator_next(&i))
  {
    ColumnValue value = block_tuple_iterator_get(
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
  if (reallocate_update_length(
          sizeof(db->user_relations[0]),
          (void **)&db->user_relations,
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
    if (reallocate_update_length(
            sizeof(db->user_relations[0]),
            (void **)&db->user_relations,
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
  if (reallocate_update_length(
          sizeof(db->user_relations[0]),
          (void **)&db->user_relations,
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
      &db->relations,
      relations_column_types,
      ARRAY_LENGTH(relations_column_types),
      0,
      (ColumnValue){.integer = relation_id});

  return DATABASE_DROP_TABLE_OK;
}

static AllocateError allocate_column_table_name(
    String *string, StringSlice relation_name, StringSlice column_name)
{
  if (string_from_string_slice(relation_name, string) != ALLOCATE_OK)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  if (string_append(string, string_slice_from_ptr(".")) != ALLOCATE_OK)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  return string_append(string, column_name);
}

static AllocateError database_get_static_relation_column_metadata(
    StringSlice relation_name,
    ColumnsLength tuple_length,
    const char *const *relation_names,
    const ColumnType *relation_types,
    ColumnType **types,
    String **names)
{
  AllocateError status =
      allocate((void **)types, sizeof(**types) * tuple_length);
  if (status == ALLOCATE_OK && names != NULL)
  {
    status = allocate((void **)names, sizeof(**names) * tuple_length);
  }

  size_t column_index = 0;
  for (; column_index < tuple_length && status == ALLOCATE_OK; ++column_index)
  {
    (*types)[column_index] = relation_types[column_index];

    if (names != NULL)
    {
      String *string = &(*names)[column_index];
      status = allocate_column_table_name(
          &(*names)[column_index],
          relation_name,
          string_slice_from_ptr(relation_names[column_index]));
    }
  }

  if (status != ALLOCATE_OK)
  {
    for (size_t i = 0; i < column_index; ++i) { string_destroy(&(*names)[i]); }
    deallocate(*types, sizeof(**types) * tuple_length);
    if (names != NULL)
    {
      deallocate(*names, sizeof(**names) * tuple_length);
    }
    return ALLOCATE_OUT_OF_MEMORY;
  }

  return ALLOCATE_OK;
}

typedef enum
{
  DATABASE_GET_RELATION_COLUMN_METADATA_OK,
  DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY,
  DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND,
} DatabaseGetRelationColumnMetadataError;

static DatabaseGetRelationColumnMetadataError
database_get_relation_column_metadata_(
    Database *db,
    StringSlice relation_name,
    MemoryStore **store,
    ColumnsLength *tuple_length_,
    ColumnType **types,
    String **names)
{
  assert(db != NULL);
  assert(store != NULL);
  assert(*store == NULL);
  assert(tuple_length_ != NULL);
  assert(*tuple_length_ == 0);
  assert(types != NULL);

  if (string_slice_eq(
          relation_name, string_slice_from_ptr(relations_relation_name)))
  {
    ColumnsLength tuple_length = ARRAY_LENGTH(relations_column_types);

    if (database_get_static_relation_column_metadata(
            relation_name,
            tuple_length,
            relations_column_names,
            relations_column_types,
            types,
            names)
        != ALLOCATE_OK)
    {
      return DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY;
    }

    *tuple_length_ = tuple_length;
    *store = &db->relations;
    return DATABASE_GET_RELATION_COLUMN_METADATA_OK;
  }

  if (string_slice_eq(
          relation_name, string_slice_from_ptr(relation_columns_relation_name)))
  {
    ColumnsLength tuple_length = ARRAY_LENGTH(relation_columns_column_types);

    if (database_get_static_relation_column_metadata(
            relation_name,
            tuple_length,
            relation_columns_column_names,
            relation_columns_column_types,
            types,
            names)
        != ALLOCATE_OK)
    {
      return DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY;
    }

    *tuple_length_ = tuple_length;
    *store = &db->relation_columns;
    return DATABASE_GET_RELATION_COLUMN_METADATA_OK;
  }

  int64_t relation_id = 0;
  if (!query_relation_id_by_name(*db, relation_name, &relation_id))
  {
    return DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND;
  }

  size_t tuple_length = 0;
  size_t largest_column_id = 0;
  for (BlockTupleIterator i = memory_store_iterate(&db->relation_columns);
       i.valid;
       block_tuple_iterator_next(&i))
  {
    ColumnValue tuple_relation_id = block_tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        0);

    if (tuple_relation_id.integer != relation_id)
    {
      continue;
    }

    ColumnValue column_id = block_tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        1);

    largest_column_id = MAX(column_id.integer, largest_column_id);
    tuple_length += 1;
  }

  // We don't allow relations without columns to exist
  assert(tuple_length > 0);
  assert(tuple_length == largest_column_id + 1);

  AllocateError status =
      allocate((void **)types, sizeof(**types) * tuple_length);

  if (status == ALLOCATE_OK && names != NULL)
  {
    status = allocate((void **)names, sizeof(**names) * tuple_length);

    for (size_t i = 0; i < tuple_length; ++i)
    {
      (*names)[i] = (String){
          .data = NULL,
          .length = 0,
      };
    }
  }

  for (BlockTupleIterator i = memory_store_iterate(&db->relation_columns);
       i.valid && status == ALLOCATE_OK;
       block_tuple_iterator_next(&i))
  {
    ColumnValue tuple_relation_id = block_tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        0);

    if (tuple_relation_id.integer != relation_id)
    {
      continue;
    }

    ColumnValue column_id = block_tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        1);

    ColumnValue type = block_tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        3);

    (*types)[column_id.integer] = type.integer;

    if (names != NULL)
    {
      ColumnValue name = block_tuple_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          2);

      status = allocate_column_table_name(
          &(*names)[column_id.integer], relation_name, name.string);
    }
  }

  if (status != ALLOCATE_OK)
  {
    deallocate(*types, sizeof(**types) * tuple_length);

    if (names != NULL)
    {
      for (size_t i = 0; i < tuple_length; ++i)
      {
        string_destroy(&(*names)[i]);
      }
      deallocate(*names, sizeof(**names) * tuple_length);
    }
    return DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY;
  }

  *tuple_length_ = (ColumnsLength)tuple_length;
  *store =
      &db->user_relations[find_user_relation_index(*db, relation_id)].store;

  return DATABASE_GET_RELATION_COLUMN_METADATA_OK;
}

static DatabaseGetRelationColumnMetadataError
database_get_relation_column_metadata(
    Database *db,
    StringSlice relation_name,
    MemoryStore **store,
    ColumnsLength *tuple_length,
    ColumnType **types,
    String **names)
{
  assert(types != NULL);
  assert(*types == NULL);
  assert(names != NULL);
  assert(*names == NULL);

  return database_get_relation_column_metadata_(
      db, relation_name, store, tuple_length, types, names);
}

static DatabaseGetRelationColumnMetadataError
database_get_relation_column_types(
    Database *db,
    StringSlice relation_name,
    MemoryStore **store,
    ColumnsLength *tuple_length,
    ColumnType **types)
{
  assert(types != NULL);
  assert(*types == NULL);

  return database_get_relation_column_metadata_(
      db, relation_name, store, tuple_length, types, NULL);
}

typedef enum
{
  DATABASE_INSERT_TUPLE_OK,
  DATABASE_INSERT_TUPLE_OUT_OF_MEMORY,
  DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH,
  DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH,
  DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND,
} DatabaseInsertTupleError;

// TODO: Take relation as argument
static DatabaseInsertTupleError database_insert_tuple(
    Database *db,
    StringSlice name,
    const ColumnType *types,
    const ColumnValue *values,
    int16_t length)
{
  assert(db != NULL);

  ColumnType *columns_types = NULL;
  ColumnsLength columns_length = 0;
  MemoryStore *store = NULL;
  switch (database_get_relation_column_types(
      db, name, &store, &columns_length, &columns_types))
  {
  case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
    break;

  case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
    return DATABASE_INSERT_TUPLE_OUT_OF_MEMORY;

  case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
    return DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND;
  }

  if (length != columns_length)
  {
    return DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH;
  }

  for (size_t i = 0; i < length; ++i)
  {
    if (types[i] != columns_types[i])
    {
      deallocate(columns_types, sizeof(*columns_types) * columns_length);
      return DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH;
    }
  }

  deallocate(columns_types, sizeof(*columns_types) * columns_length);

  MemoryStoreInsertTupleError insert_error =
      memory_store_insert_tuple(store, types, values, length);
  // TODO: Use transactions to handle failure
  assert(insert_error == MEMORY_STORE_INSERT_TUPLE_OK);

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
    // TDOO: take column name instead of index
    ColumnsLength column_index,
    ColumnType type,
    ColumnValue value)
{
  assert(db != NULL);

  ColumnType *columns_types = NULL;
  ColumnsLength columns_length = 0;
  MemoryStore *store = NULL;
  switch (database_get_relation_column_types(
      db, name, &store, &columns_length, &columns_types))
  {
  case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
    break;

  case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
    return DATABASE_DELETE_TUPLES_OUT_OF_MEMORY;

  case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
    return DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND;
  }

  if (column_index >= columns_length)
  {
    return DATABASE_DELETE_TUPLES_COLUMN_INDEX_OUT_OF_RANGE;
  }

  if (type != columns_types[column_index])
  {
    return DATABASE_DELETE_TUPLES_COLUMN_TYPE_MISMATCH;
  }

  memory_store_delete_tuples(
      store, columns_types, columns_length, column_index, value);

  deallocate(columns_types, sizeof(*columns_types) * columns_length);

  return DATABASE_DELETE_TUPLES_OK;
}

typedef enum
{
  DATABASE_READ_RELATION_OK,
  DATABASE_READ_RELATION_RELATION_NOT_FOUND,
  DATABASE_READ_RELATION_OUT_OF_MEMORY,
} DatabaseReadRelationError;

// ----- Query -----

typedef enum
{
  PREDICATE_OPERATOR_GRANULAR_INVALID,
  PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_PREFIX_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_TWO_COLUMNS_EQUAL,
} PredicateOperatorGranular;

typedef enum
{
  PREDICATE_OPERATOR_EQUAL_COLUMNS,
  PREDICATE_OPERATOR_EQUAL_CONSTANT,
  PREDICATE_OPERATOR_STRING_PREFIX_EQUAL,
} PredicateOperator;

typedef enum
{
  QUERY_OPERATOR_READ,
  QUERY_OPERATOR_PROJECT,
  QUERY_OPERATOR_SELECT,
  QUERY_OPERATOR_CARTESIAN_PRODUCT,
} QueryOperator;

typedef struct
{
  size_t query_index;
  StringSlice *column_names;
  ColumnsLength tuple_length;
} ProjectQueryParameter;

typedef struct
{
  size_t query_index;
  PredicateOperator operator;
  union
  {
    struct
    {
      StringSlice column_name;
      ColumnValue value;
    } constant;

    struct
    {
      StringSlice lhs_column_name;
      StringSlice rhs_column_name;
    } two_columns;
  };
} SelectQueryParameter;

typedef struct
{
  size_t lhs_index;
  size_t rhs_index;
} CartesianProductQueryParameter;

typedef union
{
  StringSlice read_relation_name;
  ProjectQueryParameter project;
  SelectQueryParameter select;
  CartesianProductQueryParameter cartesian_product;
} QueryParameter;

typedef enum
{
  TUPLE_ITERATOR_OK,
  TUPLE_ITERATOR_SELECT_COLUMN_NOT_FOUND,
  TUPLE_ITERATOR_SELECT_COLUMN_TYPE_MISMATCH,
} TupleIteratorError;

typedef union
{
  struct
  {
    ColumnsLength column_index;
    ColumnValue value;
  } constant;

  struct
  {
    ColumnsLength lhs_column_index;
    ColumnsLength rhs_column_index;
  } two_columns;
} TupleIteratorSelectPredicate;

typedef struct TupleIterator
{
  QueryOperator operator;
  union
  {
    struct
    {
      BlockTupleIterator it;
      ColumnsLength tuple_length;
      String *column_names;
      ColumnType *column_types;
    } read;

    struct
    {
      struct TupleIterator *it;
      ColumnsLength tuple_length;
      ColumnsLength *mapped_ids;
    } project;

    struct
    {
      struct TupleIterator *it;
      PredicateOperatorGranular operator;
      TupleIteratorSelectPredicate predicate;
    } select;

    struct
    {
      struct TupleIterator *lhs;
      struct TupleIterator *rhs;
    } cartesian_product;
  };
} TupleIterator;

void tuple_iterator_destroy(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
  {
    for (size_t i = 0; i < it->read.tuple_length; ++i)
    {
      string_destroy(&it->read.column_names[i]);
    }
    deallocate(
        it->read.column_names,
        sizeof(*it->read.column_names) * it->read.tuple_length);
    deallocate(
        it->read.column_types,
        sizeof(*it->read.column_types) * it->read.tuple_length);
  }
  break;

  case QUERY_OPERATOR_PROJECT:
  {
    deallocate(
        it->project.mapped_ids,
        sizeof(it->project.mapped_ids) * it->project.tuple_length);
  }
  break;

  case QUERY_OPERATOR_SELECT:
  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
    break;
  }
}

static TupleIterator read_tuple_iterator(
    MemoryStore *store,
    String *names,
    ColumnType *types,
    ColumnsLength tuple_length)
{
  return (TupleIterator){
      .operator= QUERY_OPERATOR_READ,
      .read =
          {
              .it = memory_store_iterate(store),
              .column_names = names,
              .column_types = types,
              .tuple_length = tuple_length,
          },
  };
}

static TupleIterator project_tuple_iterator(
    TupleIterator *it, ColumnsLength tuple_length, ColumnsLength *mapped_ids)
{
  return (TupleIterator){
      .operator= QUERY_OPERATOR_PROJECT,
      .project =
          {
              .it = it,
              .tuple_length = tuple_length,
              .mapped_ids = mapped_ids,
          },
  };
}

static TupleIterator select_tuple_iterator(
    TupleIterator *it,
    PredicateOperatorGranular operator,
    TupleIteratorSelectPredicate predicate)
{
  return (TupleIterator){
      .operator= QUERY_OPERATOR_SELECT,
      .select =
          {
              .it = it,
              .operator= operator,
              .predicate = predicate,
          },
  };
}

static TupleIterator
cartesian_product_tuple_iterator(TupleIterator *lhs, TupleIterator *rhs)
{
  return (TupleIterator){
      .operator= QUERY_OPERATOR_CARTESIAN_PRODUCT,
      .cartesian_product = {.lhs = lhs, .rhs = rhs},
  };
}

static ColumnsLength tuple_iterator_tuple_length(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return it->read.tuple_length;

  case QUERY_OPERATOR_PROJECT:
    return it->project.tuple_length;

  case QUERY_OPERATOR_SELECT:
    return tuple_iterator_tuple_length(it->select.it);

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    ColumnsLength lhs_length =
        tuple_iterator_tuple_length(it->cartesian_product.lhs);
    ColumnsLength rhs_length =
        tuple_iterator_tuple_length(it->cartesian_product.rhs);
    return (ColumnsLength)(lhs_length + rhs_length);
  }
  }
}

static bool32 tuple_iterator_valid(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return it->read.it.valid;

  case QUERY_OPERATOR_PROJECT:
    return tuple_iterator_valid(it->project.it);

  case QUERY_OPERATOR_SELECT:
    return tuple_iterator_valid(it->select.it);

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
    return tuple_iterator_valid(it->cartesian_product.lhs);
  }
}

void cartesian_product_tuple_iterator_get_target_iterator_and_column_id(
    TupleIterator *it, TupleIterator **target_it, ColumnsLength *column_id)
{
  ColumnsLength lhs_tuple_length =
      tuple_iterator_tuple_length(it->cartesian_product.lhs);

  if (*column_id < lhs_tuple_length)
  {
    *target_it = it->cartesian_product.lhs;
    return;
  }

  *column_id = (ColumnsLength)(*column_id - lhs_tuple_length);
  *target_it = it->cartesian_product.rhs;
}

static StringSlice
tuple_iterator_column_name(TupleIterator *it, ColumnsLength column_id)
{
  assert(column_id < tuple_iterator_tuple_length(it));

  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return string_slice_from_string(it->read.column_names[column_id]);

  case QUERY_OPERATOR_PROJECT:
    return tuple_iterator_column_name(
        it->project.it, it->project.mapped_ids[column_id]);

  case QUERY_OPERATOR_SELECT:
    return tuple_iterator_column_name(it->select.it, column_id);

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    TupleIterator *target_it = NULL;
    cartesian_product_tuple_iterator_get_target_iterator_and_column_id(
        it, &target_it, &column_id);

    return tuple_iterator_column_name(target_it, column_id);
  }
  }
}

static ColumnType
tuple_iterator_column_type(TupleIterator *it, ColumnsLength column_id)
{
  assert(column_id < tuple_iterator_tuple_length(it));

  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return it->read.column_types[column_id];

  case QUERY_OPERATOR_PROJECT:
    return tuple_iterator_column_type(
        it->project.it, it->project.mapped_ids[column_id]);

  case QUERY_OPERATOR_SELECT:
    return tuple_iterator_column_type(it->select.it, column_id);

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    TupleIterator *target_it = NULL;
    cartesian_product_tuple_iterator_get_target_iterator_and_column_id(
        it, &target_it, &column_id);

    return tuple_iterator_column_type(target_it, column_id);
  }
  }
}

static ColumnValue
tuple_iterator_column_value(TupleIterator *it, ColumnsLength column_id)
{
  assert(tuple_iterator_valid(it));
  assert(column_id < tuple_iterator_tuple_length(it));

  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return block_tuple_iterator_get(
        &it->read.it, it->read.column_types, it->read.tuple_length, column_id);

  case QUERY_OPERATOR_PROJECT:
    return tuple_iterator_column_value(
        it->project.it, it->project.mapped_ids[column_id]);

  case QUERY_OPERATOR_SELECT:
    return tuple_iterator_column_value(it->select.it, column_id);

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    TupleIterator *target_it = NULL;
    cartesian_product_tuple_iterator_get_target_iterator_and_column_id(
        it, &target_it, &column_id);

    return tuple_iterator_column_value(target_it, column_id);
  }
  }
}

bool32 tuple_iterator_select_tuple(TupleIterator *it)
{
  switch (it->select.operator)
  {
  case PREDICATE_OPERATOR_GRANULAR_INVALID:
    assert(false);
    return false;

  case PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL:
    return tuple_iterator_column_value(
               it->select.it, it->select.predicate.constant.column_index)
               .integer
           == it->select.predicate.constant.value.integer;

  case PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL:
    return string_slice_eq(
        tuple_iterator_column_value(
            it->select.it, it->select.predicate.constant.column_index)
            .string,
        it->select.predicate.constant.value.string);

  case PREDICATE_OPERATOR_GRANULAR_STRING_PREFIX_EQUAL:
    return string_slice_prefix_eq(
        tuple_iterator_column_value(
            it->select.it, it->select.predicate.constant.column_index)
            .string,
        it->select.predicate.constant.value.string);

  case PREDICATE_OPERATOR_GRANULAR_TWO_COLUMNS_EQUAL:
    switch (tuple_iterator_column_type(
        it->select.it, it->select.predicate.constant.column_index))
    {
    case COLUMN_TYPE_INTEGER:
      return tuple_iterator_column_value(
                 it->select.it,
                 it->select.predicate.two_columns.lhs_column_index)
                 .integer
             == tuple_iterator_column_value(
                    it->select.it,
                    it->select.predicate.two_columns.rhs_column_index)
                    .integer;

    case COLUMN_TYPE_STRING:
      return string_slice_eq(
          tuple_iterator_column_value(
              it->select.it, it->select.predicate.two_columns.lhs_column_index)
              .string,
          tuple_iterator_column_value(
              it->select.it, it->select.predicate.two_columns.rhs_column_index)
              .string);
    }
  }
}

static void tuple_iterator_reset(TupleIterator *it);

static void tuple_iterator_next(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    block_tuple_iterator_next(&it->read.it);
    break;

  case QUERY_OPERATOR_PROJECT:
    tuple_iterator_next(it->project.it);
    break;

  case QUERY_OPERATOR_SELECT:
  {
    for (tuple_iterator_next(it->select.it);
         tuple_iterator_valid(it->select.it)
         && !tuple_iterator_select_tuple(it);
         tuple_iterator_next(it->select.it))
    {
    }
  }
  break;

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    tuple_iterator_next(it->cartesian_product.rhs);
    if (!tuple_iterator_valid(it->cartesian_product.rhs))
    {
      tuple_iterator_reset(it->cartesian_product.rhs);
      tuple_iterator_next(it->cartesian_product.lhs);
    }
  }
  break;
  }
}

static void tuple_iterator_reset(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    it->read.it = memory_store_iterate(it->read.it.store);
    break;

  case QUERY_OPERATOR_PROJECT:
    tuple_iterator_reset(it->project.it);
    break;

  case QUERY_OPERATOR_SELECT:
  {
    tuple_iterator_reset(it->select.it);
    if (!tuple_iterator_select_tuple(it))
    {
      tuple_iterator_next(it);
    }
  }
  break;

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    tuple_iterator_reset(it->cartesian_product.rhs);
    tuple_iterator_reset(it->cartesian_product.lhs);
  }
  break;
  }
}

// TODO: Raise errors for ambiguous names. Example:
//   search_name = user_id
//   columns = users.user_id, roles.user_id
static bool32
match_column_names(StringSlice search_name, StringSlice column_name)
{
  StringSlice search_name_qualifier =
      string_slice_find_past(search_name, RELATION_NAME_QUALIFIER);

  StringSlice column_name_qualifier =
      string_slice_find_past(column_name, RELATION_NAME_QUALIFIER);

  assert(column_name_qualifier.data != NULL);

  return (
      (search_name_qualifier.data != NULL
       && string_slice_eq(column_name, search_name))
      || string_slice_eq(column_name_qualifier, search_name));
}

bool32 tuple_iterator_find_column_name(
    TupleIterator *it, StringSlice column_name, ColumnsLength *column_id)
{
  for (ColumnsLength i = 0; i < tuple_iterator_tuple_length(it); ++i)
  {
    if (match_column_names(column_name, tuple_iterator_column_name(it, i)))
    {
      *column_id = i;
      return true;
    }
  }

  return false;
}

typedef struct
{
  size_t length;
  TupleIterator *iterators;
} QueryIterator;

typedef enum
{
  DATABASE_QUERY_OK,
  DATABASE_QUERY_OUT_OF_MEMORY,
  DATABASE_QUERY_RELATION_NOT_FOUND,
  DATABASE_QUERY_COLUMN_NOT_FOUND,
  DATABASE_QUERY_OPERATOR_TYPE_MISMATCH,
} DatabaseQueryError;

static DatabaseQueryError database_query(
    Database *db,
    size_t length,
    const QueryOperator *operators,
    const QueryParameter *parameters,
    QueryIterator *it)
{
  // TODO: Make sure the dependendencies have no loops
  assert(db != NULL);
  assert(length > 0);
  assert(operators != NULL);
  assert(parameters != NULL);
  assert(it != NULL);

  TupleIterator *iterators = NULL;
  if (allocate((void **)&iterators, sizeof(TupleIterator) * length)
      != ALLOCATE_OK)
  {
    return DATABASE_QUERY_OUT_OF_MEMORY;
  }

  DatabaseQueryError status = DATABASE_QUERY_OK;
  size_t query = 0;
  for (; status == DATABASE_QUERY_OK && query < length; ++query)
  {
    switch (operators[query])
    {
    case QUERY_OPERATOR_READ:
    {
      MemoryStore *store = NULL;
      ColumnsLength tuple_length = 0;
      ColumnType *types = NULL;
      String *names = NULL;
      switch (database_get_relation_column_metadata(
          db,
          parameters[query].read_relation_name,
          &store,
          &tuple_length,
          &types,
          &names))
      {
      case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
        iterators[query] =
            read_tuple_iterator(store, names, types, tuple_length);
        break;

      case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
        status = DATABASE_QUERY_OUT_OF_MEMORY;
        break;

      case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
        status = DATABASE_QUERY_RELATION_NOT_FOUND;
        break;
      }
    }
    break;

    case QUERY_OPERATOR_PROJECT:
    {
      ProjectQueryParameter project = parameters[query].project;
      assert(project.query_index < query);

      ColumnsLength *mapped_ids = NULL;
      if (allocate(
              (void **)&mapped_ids, sizeof(*mapped_ids) * project.tuple_length)
          != ALLOCATE_OK)
      {
        status = DATABASE_QUERY_OUT_OF_MEMORY;
        break;
      }

      bool32 found = true;
      TupleIterator *iter = &iterators[project.query_index];
      for (ColumnsLength i = 0; i < project.tuple_length && found; ++i)
      {
        ColumnsLength column_id = 0;
        found = tuple_iterator_find_column_name(
            iter, project.column_names[i], &column_id);
        mapped_ids[i] = column_id;
      }

      if (!found)
      {
        deallocate(mapped_ids, sizeof(*mapped_ids) * project.tuple_length);
        status = DATABASE_QUERY_COLUMN_NOT_FOUND;
        break;
      }

      iterators[query] =
          project_tuple_iterator(iter, project.tuple_length, mapped_ids);
    }
    break;

    case QUERY_OPERATOR_SELECT:
    {
      SelectQueryParameter select = parameters[query].select;
      TupleIterator *iter = &iterators[select.query_index];
      PredicateOperatorGranular op = PREDICATE_OPERATOR_GRANULAR_INVALID;
      TupleIteratorSelectPredicate predicate;
      switch (select.operator)
      {
      case PREDICATE_OPERATOR_EQUAL_CONSTANT:
      {
        if (!tuple_iterator_find_column_name(
                iter,
                select.constant.column_name,
                &predicate.constant.column_index))
        {
          status = DATABASE_QUERY_COLUMN_NOT_FOUND;
          break;
        }

        predicate.constant.value = select.constant.value;

        switch (
            tuple_iterator_column_type(iter, predicate.constant.column_index))
        {
        case COLUMN_TYPE_INTEGER:
          op = PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL;
          break;

        case COLUMN_TYPE_STRING:
          op = PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL;
          break;
        }
      }
      break;

      case PREDICATE_OPERATOR_STRING_PREFIX_EQUAL:
      {
        if (!tuple_iterator_find_column_name(
                iter,
                select.constant.column_name,
                &predicate.constant.column_index))
        {
          status = DATABASE_QUERY_COLUMN_NOT_FOUND;
          break;
        }

        predicate.constant.value = select.constant.value;

        if (tuple_iterator_column_type(iter, predicate.constant.column_index)
            != COLUMN_TYPE_STRING)
        {
          status = DATABASE_QUERY_OPERATOR_TYPE_MISMATCH;
          break;
        }

        op = PREDICATE_OPERATOR_GRANULAR_STRING_PREFIX_EQUAL;
      }
      break;

      case PREDICATE_OPERATOR_EQUAL_COLUMNS:
      {
        if (!tuple_iterator_find_column_name(
                iter,
                select.two_columns.lhs_column_name,
                &predicate.two_columns.lhs_column_index))
        {
          status = DATABASE_QUERY_COLUMN_NOT_FOUND;
          break;
        }

        if (!tuple_iterator_find_column_name(
                iter,
                select.two_columns.rhs_column_name,
                &predicate.two_columns.rhs_column_index))
        {
          status = DATABASE_QUERY_COLUMN_NOT_FOUND;
          break;
        }

        op = PREDICATE_OPERATOR_GRANULAR_TWO_COLUMNS_EQUAL;
      }
      break;
      }

      if (status == DATABASE_QUERY_OK)
      {
        iterators[query] = select_tuple_iterator(iter, op, predicate);
      }
    }
    break;

    case QUERY_OPERATOR_CARTESIAN_PRODUCT:
    {
      CartesianProductQueryParameter cartesian_product =
          parameters[query].cartesian_product;
      iterators[query] = cartesian_product_tuple_iterator(
          &iterators[cartesian_product.lhs_index],
          &iterators[cartesian_product.rhs_index]);
    }
    break;
    }
  }

  if (status != DATABASE_QUERY_OK)
  {
    // Last query is not initialized
    for (size_t j = 0; j < query - 1; ++j)
    {
      tuple_iterator_destroy(&iterators[j]);
    }
    return status;
  }

  *it = (QueryIterator){
      .length = length,
      .iterators = iterators,
  };

  // The first tuple may not be valid for some iterators, such as select. This
  // way each iterators starts in a good state without having to account for it
  // into the initialization code above
  tuple_iterator_reset(&it->iterators[it->length - 1]);

  return DATABASE_QUERY_OK;
}

void query_iterator_destroy(QueryIterator *it)
{
  for (size_t i = 0; i < it->length; ++i)
  {
    tuple_iterator_destroy(&it->iterators[i]);
  }
}

TupleIterator *query_iterator_get_output_iterator(QueryIterator *it)
{
  return &it->iterators[it->length - 1];
}

// ----- Query -----

#define LOGICAL_H
#endif
