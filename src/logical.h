#ifndef LOGICAL_H

#include "physical.h"
#include "std.h"

#define RELATION_NAME_QUALIFIER '.'

#define RELATIONS_RELATION_ID 0
#define RELATION_COLUMNS_RELATION_ID 1
#define RESERVED_RELATION_IDS 1024

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
  DiskBufferPool pool;
} Database;

static DiskBufferPoolCreateRelationError
database_new(Database *db, StringSlice path, void *data, size_t length)
{
  assert(db != NULL);
  assert(data != NULL);
  size_t buffers_length = length / (PAGE_SIZE + sizeof(MappedBuffer));
  assert(length == buffers_length * (PAGE_SIZE + sizeof(MappedBuffer)));

  *db = (Database){
      .pool =
          {
              .save_path = {},
              .buffers_length = buffers_length,
              .buffers = data,
              .buffer_pages = data + (buffers_length * sizeof(MappedBuffer)),
          },
  };

  memory_copy_forward(db->pool.save_path_buffer, path.data, path.length);

  db->pool.save_path = (StringSlice){
      .data = db->pool.save_path_buffer,
      .length = path.length,
  };

  for (size_t i = 0; i < db->pool.buffers_length; ++i)
  {
    db->pool.buffers[i] = (MappedBuffer){.status = MAPPED_BUFFER_STATUS_FREE};
  }

  DiskBufferPoolCreateRelationError error =
      relation_create(&db->pool, RELATIONS_RELATION_ID, false);
  if (error != RELATION_CREATE_OK)
  {
    return error;
  }

  return relation_create(&db->pool, RELATION_COLUMNS_RELATION_ID, false);
}

typedef struct
{
  RelationId relation_id;
  RelationIteratorStatus status;
} QueryRelationIdByNameResult;

static QueryRelationIdByNameResult
query_relation_id_by_name(Database db, StringSlice name)
{
  RelationIterator i;
  for (i = relation_iterate(&db.pool, RELATIONS_RELATION_ID);
       i.status == RELATION_ITERATOR_STATUS_OK;
       relation_iterator_next(&i))
  {
    ColumnValue tuple_name = relation_iterator_get(
        &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 1);
    if (string_slice_eq(name, tuple_name.string))
    {
      ColumnValue tuple_id = relation_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

      relation_iterator_close(&i);
      return (QueryRelationIdByNameResult){
          .status = RELATION_ITERATOR_STATUS_OK,
          .relation_id = tuple_id.integer,
      };
    }
  }
  relation_iterator_close(&i);

  return (QueryRelationIdByNameResult){
      .status = i.status,
  };
}

typedef enum
{
  DATABASE_CREATE_TABLE_OK,
  DATABASE_CREATE_TABLE_OUT_OF_MEMORY,
  DATABASE_CREATE_TABLE_NO_SPACE,
  DATABASE_CREATE_TABLE_READING_DISK,
  DATABASE_CREATE_TABLE_CREATING_FILE,
  DATABASE_CREATE_TABLE_REALTION_ALREADY_EXISTS,
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

  int64_t relation_id = RESERVED_RELATION_IDS;
  {
    RelationIterator i;
    bool32 exists = false;
    for (i = relation_iterate(&db->pool, RELATIONS_RELATION_ID);
         i.status == RELATION_ITERATOR_STATUS_OK;
         relation_iterator_next(&i))
    {
      ColumnValue tuple_id = relation_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

      if (relation_id <= tuple_id.integer)
      {
        relation_id = tuple_id.integer + 1;
      }

      ColumnValue tuple_name = relation_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 1);

      if (string_slice_eq(name, tuple_name.string))
      {
        exists = true;
        break;
      }
    }

    relation_iterator_close(&i);

    if (exists)
    {
      return DATABASE_CREATE_TABLE_REALTION_ALREADY_EXISTS;
    }

    switch (i.status)
    {
    case RELATION_ITERATOR_STATUS_OK:
      assert(false);
      break;

    case RELATION_ITERATOR_STATUS_ERROR:
      return DATABASE_CREATE_TABLE_READING_DISK;

    case RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
      break;
    }
  }

  assert(relation_id >= RESERVED_RELATION_IDS);

  DiskBufferPoolCreateRelationError error =
      relation_create(&db->pool, relation_id, true);
  if (error != RELATION_CREATE_OK)
  {
    return DATABASE_CREATE_TABLE_CREATING_FILE;
  }

  RelationInsertTupleError insert_error = RELATION_INSERT_TUPLE_OK;
  for (int16_t column = 0;
       column < length && insert_error == RELATION_INSERT_TUPLE_OK;
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

    insert_error = relation_insert_tuple(
        &db->pool,
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_column_types,
        relation_column_values,
        ARRAY_LENGTH(relation_column_values));
  }

  if (insert_error == RELATION_INSERT_TUPLE_OK)
  {
    ColumnValue relations_values[] = {
        {.integer = relation_id},
        {.string = name},
    };
    STATIC_ASSERT(
        ARRAY_LENGTH(relations_values) == ARRAY_LENGTH(relations_column_types));

    insert_error = relation_insert_tuple(
        &db->pool,
        RELATIONS_RELATION_ID,
        relations_column_types,
        relations_values,
        ARRAY_LENGTH(relations_values));
  }

  if (insert_error != RELATION_INSERT_TUPLE_OK)
  {
    relation_delete_tuples(
        &db->pool,
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        0,
        (ColumnValue){.integer = relation_id});

    relation_delete_tuples(
        &db->pool,
        RELATIONS_RELATION_ID,
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
  DATABASE_DROP_TABLE_OUT_OF_MEMORY,
  DATABASE_DROP_TABLE_NOT_FOUND,
  DATABASE_DROP_TABLE_READING_DISK,
} DatabaseDropTableError;

static DatabaseDropTableError
database_drop_table(Database *db, StringSlice name)
{
  assert(db != NULL);
  assert(name.length > 0);

  QueryRelationIdByNameResult result = query_relation_id_by_name(*db, name);
  switch (result.status)
  {
  case RELATION_ITERATOR_STATUS_OK:
    break;

  case RELATION_ITERATOR_STATUS_ERROR:
    return DATABASE_DROP_TABLE_READING_DISK;

  case RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
    return DATABASE_DROP_TABLE_NOT_FOUND;
  }

  relation_delete_tuples(
      &db->pool,
      RELATION_COLUMNS_RELATION_ID,
      relation_columns_column_types,
      ARRAY_LENGTH(relation_columns_column_types),
      0,
      (ColumnValue){.integer = result.relation_id});

  relation_delete_tuples(
      &db->pool,
      RELATIONS_RELATION_ID,
      relations_column_types,
      ARRAY_LENGTH(relations_column_types),
      0,
      (ColumnValue){.integer = result.relation_id});

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

typedef enum
{
  DATABASE_GET_RELATION_COLUMN_METADATA_OK,
  DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY,
  DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND,
  DATABASE_GET_RELATION_COLUMN_METADATA_READING_DISK,
} DatabaseGetRelationColumnMetadataError;

typedef struct
{
  ColumnsLength tuple_length;
  RelationId relation_id;
  ColumnType *types;
  String *names;
  DatabaseGetRelationColumnMetadataError error;
} DatabaseGetRelationColumnMetadataResult;

static DatabaseGetRelationColumnMetadataResult
database_get_static_relation_column_metadata(
    StringSlice relation_name, bool32 write_names)
{
  RelationId relation_id = 0;
  size_t tuple_length = 0;
  const char *const *column_names = NULL;
  const ColumnType *column_types = NULL;

  if (string_slice_eq(
          relation_name, string_slice_from_ptr(relations_relation_name)))
  {
    relation_id = RELATIONS_RELATION_ID;
    tuple_length = ARRAY_LENGTH(relations_column_types);
    column_names = relations_column_names;
    column_types = relations_column_types;
  }
  else if (string_slice_eq(
               relation_name,
               string_slice_from_ptr(relation_columns_relation_name)))
  {
    relation_id = RELATION_COLUMNS_RELATION_ID;
    tuple_length = ARRAY_LENGTH(relation_columns_column_types);
    column_names = relation_columns_column_names;
    column_types = relation_columns_column_types;
  }
  else
  {
    return (DatabaseGetRelationColumnMetadataResult){
        .error = DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND};
  }

  ColumnType *types = NULL;
  String *names = NULL;

  AllocateError status =
      allocate((void **)&types, sizeof(*types) * tuple_length);
  if (status == ALLOCATE_OK && write_names)
  {
    status = allocate((void **)&names, sizeof(*names) * tuple_length);
  }

  size_t column_index = 0;
  for (; column_index < tuple_length && status == ALLOCATE_OK; ++column_index)
  {
    types[column_index] = column_types[column_index];

    if (write_names)
    {
      String *string = &names[column_index];
      status = allocate_column_table_name(
          &names[column_index],
          relation_name,
          string_slice_from_ptr(column_names[column_index]));
    }
  }

  if (status != ALLOCATE_OK)
  {
    deallocate(types, sizeof(*types) * tuple_length);
    if (write_names)
    {
      for (size_t i = 0; i < column_index; ++i) { string_destroy(&names[i]); }
      deallocate(names, sizeof(*names) * tuple_length);
    }
    return (DatabaseGetRelationColumnMetadataResult){
        .error = DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY};
  }

  return (DatabaseGetRelationColumnMetadataResult){
      .tuple_length = (ColumnsLength)tuple_length,
      .relation_id = relation_id,
      .types = types,
      .names = names,
      .error = DATABASE_GET_RELATION_COLUMN_METADATA_OK};
}

static DatabaseGetRelationColumnMetadataResult
database_get_relation_column_metadata(
    Database *db, StringSlice relation_name, bool32 write_names)
{
  assert(db != NULL);

  {
    DatabaseGetRelationColumnMetadataResult result =
        database_get_static_relation_column_metadata(
            relation_name, write_names);

    switch (result.error)
    {
    case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
    case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
    case DATABASE_GET_RELATION_COLUMN_METADATA_READING_DISK:
      return result;

    case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
      break;
    }
  }

  QueryRelationIdByNameResult result =
      query_relation_id_by_name(*db, relation_name);
  switch (result.status)
  {
  case RELATION_ITERATOR_STATUS_OK:
    break;

  case RELATION_ITERATOR_STATUS_ERROR:
    return (DatabaseGetRelationColumnMetadataResult){
        .error = DATABASE_GET_RELATION_COLUMN_METADATA_READING_DISK};

  case RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
    return (DatabaseGetRelationColumnMetadataResult){
        .error = DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND};
  }

  size_t tuple_length = 0;
  size_t largest_column_id = 0;
  {
    RelationIterator i;
    for (i = relation_iterate(&db->pool, RELATION_COLUMNS_RELATION_ID);
         i.status == RELATION_ITERATOR_STATUS_OK;
         relation_iterator_next(&i))
    {
      ColumnValue tuple_relation_id = relation_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          0);

      if (tuple_relation_id.integer != result.relation_id)
      {
        continue;
      }

      ColumnValue column_id = relation_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          1);

      largest_column_id = MAX(column_id.integer, largest_column_id);
      tuple_length += 1;
    }
    relation_iterator_close(&i);
  }

  // We don't allow relations without columns to exist
  assert(tuple_length > 0);
  assert(tuple_length == largest_column_id + 1);

  ColumnType *types = NULL;
  String *names = NULL;

  AllocateError status =
      allocate((void **)&types, sizeof(*types) * tuple_length);

  if (status == ALLOCATE_OK && write_names)
  {
    status = allocate((void **)&names, sizeof(*names) * tuple_length);

    for (size_t i = 0; i < tuple_length; ++i)
    {
      names[i] = (String){
          .data = NULL,
          .length = 0,
      };
    }
  }

  bool32 failed = false;
  {
    RelationIterator i;
    for (i = relation_iterate(&db->pool, RELATION_COLUMNS_RELATION_ID);
         i.status == RELATION_ITERATOR_STATUS_OK && status == ALLOCATE_OK;
         relation_iterator_next(&i))
    {
      ColumnValue tuple_relation_id = relation_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          0);

      if (tuple_relation_id.integer != result.relation_id)
      {
        continue;
      }

      ColumnValue column_id = relation_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          1);

      ColumnValue type = relation_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          3);

      types[column_id.integer] = type.integer;

      if (write_names)
      {
        ColumnValue name = relation_iterator_get(
            &i,
            relation_columns_column_types,
            ARRAY_LENGTH(relation_columns_column_types),
            2);

        status = allocate_column_table_name(
            names + column_id.integer, relation_name, name.string);
      }
    }

    switch (i.status)
    {
    case RELATION_ITERATOR_STATUS_OK:
      // All tuple should be consumed, so iterator should fail or finish
      assert(false);
      break;

    case RELATION_ITERATOR_STATUS_ERROR:
      failed = true;
      break;

    case RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
      failed = false;
      break;
    }

    relation_iterator_close(&i);
  }

  if (status != ALLOCATE_OK || failed)
  {
    deallocate(types, sizeof(*types) * tuple_length);

    if (write_names)
    {
      for (size_t i = 0; i < tuple_length; ++i) { string_destroy(names + i); }
      deallocate(names, sizeof(*names) * tuple_length);
    }
    return (DatabaseGetRelationColumnMetadataResult){
        .error = DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY};
  }

  return (DatabaseGetRelationColumnMetadataResult){
      .tuple_length = (ColumnsLength)tuple_length,
      .relation_id = result.relation_id,
      .types = types,
      .names = names,
      .error = DATABASE_GET_RELATION_COLUMN_METADATA_OK};
}

typedef enum
{
  DATABASE_INSERT_TUPLE_OK,
  DATABASE_INSERT_TUPLE_OUT_OF_MEMORY,
  DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH,
  DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH,
  DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND,
  DATABASE_INSERT_TUPLE_READING_DISK,
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
  DatabaseGetRelationColumnMetadataResult result =
      database_get_relation_column_metadata(db, name, false);

  switch (result.error)
  {
  case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
    break;

  case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
    return DATABASE_INSERT_TUPLE_OUT_OF_MEMORY;

  case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
    return DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND;

  case DATABASE_GET_RELATION_COLUMN_METADATA_READING_DISK:
    return DATABASE_INSERT_TUPLE_READING_DISK;
  }

  if (result.relation_id < RESERVED_RELATION_IDS)
  {
    return DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND;
  }

  if (length != result.tuple_length)
  {
    return DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH;
  }

  for (size_t i = 0; i < length; ++i)
  {
    if (types[i] != result.types[i])
    {
      deallocate(result.types, sizeof(*result.types) * result.tuple_length);
      return DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH;
    }
  }

  deallocate(result.types, sizeof(*result.types) * result.tuple_length);

  RelationInsertTupleError insert_error = relation_insert_tuple(
      &db->pool, result.relation_id, types, values, length);
  // TODO: Use transactions to handle failure
  assert(insert_error == RELATION_INSERT_TUPLE_OK);

  return DATABASE_INSERT_TUPLE_OK;
}

typedef enum
{
  DATABASE_DELETE_TUPLES_OK,
  DATABASE_DELETE_TUPLES_OUT_OF_MEMORY,
  DATABASE_DELETE_TUPLES_COLUMN_INDEX_OUT_OF_RANGE,
  DATABASE_DELETE_TUPLES_COLUMN_TYPE_MISMATCH,
  DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND,
  DATABASE_DELETE_TUPLES_READING_DISK,
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

  DatabaseGetRelationColumnMetadataResult result =
      database_get_relation_column_metadata(db, name, false);

  switch (result.error)
  {
  case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
    break;

  case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
    return DATABASE_DELETE_TUPLES_OUT_OF_MEMORY;

  case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
    return DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND;

  case DATABASE_GET_RELATION_COLUMN_METADATA_READING_DISK:
    return DATABASE_DELETE_TUPLES_READING_DISK;
  }

  if (result.relation_id < RESERVED_RELATION_IDS)
  {
    deallocate(result.types, sizeof(*result.types) * result.tuple_length);
    return DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND;
  }

  if (column_index >= result.tuple_length)
  {
    deallocate(result.types, sizeof(*result.types) * result.tuple_length);
    return DATABASE_DELETE_TUPLES_COLUMN_INDEX_OUT_OF_RANGE;
  }

  if (type != result.types[column_index])
  {
    deallocate(result.types, sizeof(*result.types) * result.tuple_length);
    return DATABASE_DELETE_TUPLES_COLUMN_TYPE_MISMATCH;
  }

  relation_delete_tuples(
      &db->pool,
      result.relation_id,
      result.types,
      result.tuple_length,
      column_index,
      value);

  deallocate(result.types, sizeof(*result.types) * result.tuple_length);

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
  PREDICATE_OPERATOR_TRUE,
  PREDICATE_OPERATOR_EQUAL,
  PREDICATE_OPERATOR_STRING_LIKE,
} PredicateOperator;

typedef enum
{
  PREDICATE_OPERATOR_GRANULAR_TRUE,
  PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_LIKE,
} PredicateOperatorGranular;

typedef enum
{
  PREDICATE_VARIABLE_TYPE_CONSTANT,
  PREDICATE_VARIABLE_TYPE_COLUMN,
} PredicateVariableType;

static bool32 string_like_operator(StringSlice string, StringSlice pattern)
{
  size_t pi = 0;
  for (size_t si = 0, pi = 0; si < string.length && pi < pattern.length;
       ++si, ++pi)
  {
    if (pattern.data[pi] != '%')
    {
      if (string.data[si] != pattern.data[pi])
      {
        return false;
      }
      continue;
    }

    pi += 1;
    if (pi == pattern.length)
    {
      return true;
    }

    if (pattern.data[pi] == '%')
    {
      if (string.data[si] != '%')
      {
        return false;
      }
      continue;
    }

    StringSlice from = (StringSlice){
        .data = string.data + si,
        .length = string.length - si,
    };
    StringSlice remaining_pattern = (StringSlice){
        .data = pattern.data + pi + 1,
        .length = pattern.length - pi - 1,
    };
    while (from.length > 0)
    {
      from = string_slice_find_past(from, pattern.data[pi]);
      if (string_like_operator(from, remaining_pattern))
      {
        return true;
      }
    }
    return false;
  }

  return pi == pattern.length;
}

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
  PredicateVariableType type;
  union
  {
    StringSlice column_name;
    struct
    {
      ColumnType type;
      ColumnValue value;
    } constant;
  };
} SelectQueryParameterVariable;

typedef struct
{
  PredicateOperator operator;
  SelectQueryParameterVariable lhs;
  SelectQueryParameterVariable rhs;
} SelectQueryCondition;

typedef struct
{
  size_t query_index;
  size_t length;
  SelectQueryCondition *conditions;
} SelectQueryParameter;

typedef struct
{
  size_t lhs_index;
  size_t rhs_index;
} CartesianProductQueryParameter;

typedef struct
{
  QueryOperator operator;
  union
  {
    StringSlice read_relation_name;
    ProjectQueryParameter project;
    SelectQueryParameter select;
    CartesianProductQueryParameter cartesian_product;
  };
} QueryParameter;

typedef enum
{
  TUPLE_ITERATOR_OK,
  TUPLE_ITERATOR_SELECT_COLUMN_NOT_FOUND,
  TUPLE_ITERATOR_SELECT_COLUMN_TYPE_MISMATCH,
} TupleIteratorError;

typedef struct
{
  PredicateVariableType type;
  union
  {
    ColumnsLength column_index;
    ColumnValue constant;
  };
} TupleIteratorPredicateVariable;

typedef struct
{
  PredicateOperatorGranular operator;
  TupleIteratorPredicateVariable lhs;
  TupleIteratorPredicateVariable rhs;
} TupleIteratorCondition;

typedef struct TupleIterator
{
  QueryOperator operator;
  union
  {
    struct
    {
      RelationIterator it;
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
      size_t length;
      TupleIteratorCondition *conditions;
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
    relation_iterator_close(&it->read.it);
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
        sizeof(*it->project.mapped_ids) * it->project.tuple_length);
  }
  break;

  case QUERY_OPERATOR_SELECT:
  {
    deallocate(
        it->select.conditions,
        sizeof(*it->select.conditions) * it->select.length);
  }
  break;

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
    break;
  }
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

static RelationIteratorStatus tuple_iterator_valid(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return it->read.it.status;

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
  assert(tuple_iterator_valid(it) == RELATION_ITERATOR_STATUS_OK);
  assert(column_id < tuple_iterator_tuple_length(it));

  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return relation_iterator_get(
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
  for (size_t i = 0; i < it->select.length; ++i)
  {
    TupleIteratorCondition condition = it->select.conditions[i];

    ColumnValue lhs = {};
    switch (condition.lhs.type)
    {
    case PREDICATE_VARIABLE_TYPE_CONSTANT:
      lhs = condition.lhs.constant;
      break;

    case PREDICATE_VARIABLE_TYPE_COLUMN:
      lhs = tuple_iterator_column_value(
          it->select.it, condition.lhs.column_index);
      break;
    }

    ColumnValue rhs = {};
    switch (condition.rhs.type)
    {
    case PREDICATE_VARIABLE_TYPE_CONSTANT:
      rhs = condition.rhs.constant;
      break;

    case PREDICATE_VARIABLE_TYPE_COLUMN:
      rhs = tuple_iterator_column_value(
          it->select.it, condition.rhs.column_index);
      break;
    }

    bool32 satisfied = false;
    switch (condition.operator)
    {
    case PREDICATE_OPERATOR_GRANULAR_TRUE:
      satisfied = true;
      break;

    case PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL:
      satisfied = lhs.integer == rhs.integer;
      break;

    case PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL:
      satisfied = string_slice_eq(lhs.string, rhs.string);
      break;

    case PREDICATE_OPERATOR_GRANULAR_STRING_LIKE:
      satisfied = string_like_operator(lhs.string, rhs.string);
      break;
    }

    if (satisfied)
    {
      return true;
    }
  }

  return false;
}

static void tuple_iterator_reset(TupleIterator *it);

static void tuple_iterator_next(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    relation_iterator_next(&it->read.it);
    break;

  case QUERY_OPERATOR_PROJECT:
    tuple_iterator_next(it->project.it);
    break;

  case QUERY_OPERATOR_SELECT:
  {
    for (tuple_iterator_next(it->select.it);
         tuple_iterator_valid(it->select.it) == RELATION_ITERATOR_STATUS_OK
         && !tuple_iterator_select_tuple(it);
         tuple_iterator_next(it->select.it))
    {
    }
  }
  break;

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    tuple_iterator_next(it->cartesian_product.rhs);
    if (tuple_iterator_valid(it->cartesian_product.rhs)
        == RELATION_ITERATOR_STATUS_NO_MORE_TUPLES)
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
  {
    MappedBuffer *buffer = it->read.it.pool->buffers + it->read.it.buffer_index;
    relation_iterator_close(&it->read.it);
    it->read.it = relation_iterate(it->read.it.pool, it->read.it.relation_id);
  }
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
  DATABASE_QUERY_READING_DISK,
} DatabaseQueryError;

static DatabaseQueryError database_query_select_fill_predicate_variable(
    TupleIterator *it,
    SelectQueryParameterVariable in,
    TupleIteratorPredicateVariable *out,
    ColumnType *column_type)
{
  switch (in.type)
  {
  case PREDICATE_VARIABLE_TYPE_CONSTANT:
    *out = (TupleIteratorPredicateVariable){
        .type = in.type,
        .constant = in.constant.value,
    };
    *column_type = in.constant.type;
    break;

  case PREDICATE_VARIABLE_TYPE_COLUMN:
  {
    ColumnsLength column_index = 0;
    if (!tuple_iterator_find_column_name(it, in.column_name, &column_index))
    {
      return DATABASE_QUERY_COLUMN_NOT_FOUND;
    }

    *out = (TupleIteratorPredicateVariable){
        .type = in.type,
        .column_index = column_index,
    };
    *column_type = tuple_iterator_column_type(it, column_index);
  }
  break;
  }

  return DATABASE_QUERY_OK;
}

DatabaseQueryError database_query_select(
    SelectQueryParameter select, TupleIterator *it, TupleIterator *result)
{
  TupleIteratorCondition *conditions = NULL;
  if (allocate((void **)&conditions, sizeof(*conditions) * select.length)
      != ALLOCATE_OK)
  {
    return DATABASE_QUERY_OUT_OF_MEMORY;
  }

  for (size_t i = 0; i < select.length; ++i)
  {
    SelectQueryCondition query_condition = select.conditions[i];
    if (query_condition.operator== PREDICATE_OPERATOR_TRUE)
    {
      conditions[i] = (TupleIteratorCondition){
          .operator= PREDICATE_OPERATOR_GRANULAR_TRUE,
          .lhs = {},
          .rhs = {},
      };
      continue;
    }

    TupleIteratorPredicateVariable lhs = {};
    ColumnType lhs_column_type = {};
    {
      DatabaseQueryError error = database_query_select_fill_predicate_variable(
          it, query_condition.lhs, &lhs, &lhs_column_type);
      if (error != DATABASE_QUERY_OK)
      {
        return error;
      }
    }

    TupleIteratorPredicateVariable rhs = {};
    ColumnType rhs_column_type = {};
    {
      DatabaseQueryError error = database_query_select_fill_predicate_variable(
          it, query_condition.rhs, &rhs, &rhs_column_type);
      if (error != DATABASE_QUERY_OK)
      {
        return error;
      }
    }

    PredicateOperatorGranular operator= {};
    switch (query_condition.operator)
    {
    case PREDICATE_OPERATOR_TRUE:
      assert(false);
      break;

    case PREDICATE_OPERATOR_EQUAL:
      if (lhs_column_type != rhs_column_type)
      {
        return DATABASE_QUERY_OPERATOR_TYPE_MISMATCH;
      }

      switch (lhs_column_type)
      {
      case COLUMN_TYPE_INTEGER:
        operator= PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL;
        break;

      case COLUMN_TYPE_STRING:
        operator= PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL;
        break;
      }
      break;

    case PREDICATE_OPERATOR_STRING_LIKE:
      if (lhs_column_type != COLUMN_TYPE_STRING)
      {
        return DATABASE_QUERY_OPERATOR_TYPE_MISMATCH;
      }

      if (rhs_column_type != COLUMN_TYPE_STRING)
      {
        return DATABASE_QUERY_OPERATOR_TYPE_MISMATCH;
      }

      operator= PREDICATE_OPERATOR_GRANULAR_STRING_LIKE;
      break;
    }

    conditions[i] = (TupleIteratorCondition){
        .operator= operator,
        .lhs = lhs,
        .rhs = rhs,
    };
  }

  *result = (TupleIterator){
      .operator= QUERY_OPERATOR_SELECT,
      .select =
          {
              .it = it,
              .length = select.length,
              .conditions = conditions,
          },
  };

  return DATABASE_QUERY_OK;
}

static DatabaseQueryError database_query(
    Database *db,
    size_t length,
    const QueryParameter *parameters,
    QueryIterator *it)
{
  // TODO: Make sure the dependendencies have no loops
  assert(db != NULL);
  assert(length > 0);
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
    switch (parameters[query].operator)
    {
    case QUERY_OPERATOR_READ:
    {
      DatabaseGetRelationColumnMetadataResult result =
          database_get_relation_column_metadata(
              db, parameters[query].read_relation_name, true);
      switch (result.error)
      {
      case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
        iterators[query] = (TupleIterator){
            .operator= QUERY_OPERATOR_READ,
            .read =
                {
                    .it = relation_iterate(&db->pool, result.relation_id),
                    .column_names = result.names,
                    .column_types = result.types,
                    .tuple_length = result.tuple_length,
                },
        };
        break;

      case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
        status = DATABASE_QUERY_OUT_OF_MEMORY;
        break;

      case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
        status = DATABASE_QUERY_RELATION_NOT_FOUND;
        break;

      case DATABASE_GET_RELATION_COLUMN_METADATA_READING_DISK:
        status = DATABASE_QUERY_READING_DISK;
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

      iterators[query] = (TupleIterator){
          .operator= QUERY_OPERATOR_PROJECT,
          .project =
              {
                  .it = iter,
                  .tuple_length = project.tuple_length,
                  .mapped_ids = mapped_ids,
              },
      };
    }
    break;

    case QUERY_OPERATOR_SELECT:
    {
      SelectQueryParameter select = parameters[query].select;
      TupleIterator *iter = &iterators[select.query_index];
      status = database_query_select(select, iter, &iterators[query]);
    }
    break;

    case QUERY_OPERATOR_CARTESIAN_PRODUCT:
    {
      CartesianProductQueryParameter cartesian_product =
          parameters[query].cartesian_product;
      iterators[query] = (TupleIterator){
          .operator= QUERY_OPERATOR_CARTESIAN_PRODUCT,
          .cartesian_product =
              {
                  .lhs = &iterators[cartesian_product.lhs_index],
                  .rhs = &iterators[cartesian_product.rhs_index],
              },
      };
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
