#include "logical.h"
#include "physical.h"
#include "std.h"

#define RELATION_NAME_QUALIFIER '.'

#define RELATIONS_RELATION_ID 0
#define RELATION_COLUMNS_RELATION_ID 1
#define RESERVED_RELATION_IDS 1024

// ---------- Schema types ----------

const char *const relations_relation_name = "relation";

const char *const relations_names[] = {
    "id",
    "name",
};

const ColumnType relations_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
};

const bool32 relations_primary_keys[] = {
    true,
    false,
};

STATIC_ASSERT(ARRAY_LENGTH(relations_names) == ARRAY_LENGTH(relations_types));

STATIC_ASSERT(
    ARRAY_LENGTH(relations_names) == ARRAY_LENGTH(relations_primary_keys));

const char *const relation_columns_relation_name = "relation_column";

const char *const relation_columns_names[] = {
    "relation_id",
    "id",
    "name",
    "type",
    "is_primary_key",
};

const ColumnType relation_columns_types[] = {
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_STRING,
    COLUMN_TYPE_INTEGER, // TODO: Implement enums
    COLUMN_TYPE_BOOLEAN,
};

const bool32 relation_columns_primary_keys[] = {
    true,
    true,
    false,
    false,
    false,
};

STATIC_ASSERT(
    ARRAY_LENGTH(relation_columns_names)
    == ARRAY_LENGTH(relation_columns_types));

STATIC_ASSERT(
    ARRAY_LENGTH(relation_columns_names)
    == ARRAY_LENGTH(relation_columns_primary_keys));

// ---------- Schema types ----------

// ---------- Logical Relation ----------

typedef struct
{
  RelationId relation_id;
  PhysicalRelationIteratorStatus status;
} QueryRelationIdByNameResult;

internal QueryRelationIdByNameResult
query_relation_id_by_name(DiskBufferPool *pool, StringSlice name)
{
  PhysicalRelationIterator i;
  for (i = physical_relation_iterate(pool, RELATIONS_RELATION_ID);
       i.status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       physical_relation_iterator_next(&i))
  {
    ColumnValue tuple_name = physical_relation_iterator_get(
        &i, relations_types, ARRAY_LENGTH(relations_types), 1);
    if (string_slice_eq(name, tuple_name.string))
    {
      ColumnValue tuple_id = physical_relation_iterator_get(
          &i, relations_types, ARRAY_LENGTH(relations_types), 0);

      physical_relation_iterator_close(&i);
      return (QueryRelationIdByNameResult){
          .status = PHYSICAL_RELATION_ITERATOR_STATUS_OK,
          .relation_id = tuple_id.integer,
      };
    }
  }
  physical_relation_iterator_close(&i);

  return (QueryRelationIdByNameResult){
      .status = i.status,
  };
}

typedef enum
{
  QUERY_NEW_RELATION_ID_OK,
  QUERY_NEW_RELATION_ID_ALREADY_EXISTS,
  QUERY_NEW_RELATION_ID_READING,
  QUERY_NEW_RELATION_ID_PROGRAM_ERROR,
} QueryNewRelationIdByNameError;

typedef struct
{
  RelationId relation_id;
  QueryNewRelationIdByNameError error;
} QueryNewRelationIdByNameResult;

internal QueryNewRelationIdByNameResult
query_new_relation_id(DiskBufferPool *pool, StringSlice name)
{
  int64_t relation_id = RESERVED_RELATION_IDS;

  PhysicalRelationIterator i;
  for (i = physical_relation_iterate(pool, RELATIONS_RELATION_ID);
       i.status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       physical_relation_iterator_next(&i))
  {
    ColumnValue tuple_name = physical_relation_iterator_get(
        &i, relations_types, ARRAY_LENGTH(relations_types), 1);

    if (string_slice_eq(name, tuple_name.string))
    {
      physical_relation_iterator_close(&i);
      return (QueryNewRelationIdByNameResult){
          .error = QUERY_NEW_RELATION_ID_ALREADY_EXISTS,
      };
    }

    ColumnValue tuple_id = physical_relation_iterator_get(
        &i, relations_types, ARRAY_LENGTH(relations_types), 0);

    relation_id = tuple_id.integer + 1;
  }
  physical_relation_iterator_close(&i);

  switch (i.status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    assert(false);
    return (QueryNewRelationIdByNameResult){
        .error = QUERY_NEW_RELATION_ID_PROGRAM_ERROR,
    };

  case PHYSICAL_RELATION_ITERATOR_STATUS_ERROR:
    return (QueryNewRelationIdByNameResult){
        .error = QUERY_NEW_RELATION_ID_READING,
    };

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
    return (QueryNewRelationIdByNameResult){
        .error = QUERY_NEW_RELATION_ID_OK,
        .relation_id = relation_id,
    };
  }
}

internal AllocateError allocate_column_table_name(
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
  RELATION_METADATA_OK,
  RELATION_METADATA_OUT_OF_MEMORY,
  RELATION_METADATA_RELATION_NOT_FOUND,
  RELATION_METADATA_IO,
} RelationMetadataError;

typedef struct
{
  ColumnsLength tuple_length;
  RelationId relation_id;
  ColumnType *types;
  String *names;
  bool32 *primary_keys;
  RelationMetadataError error;
} RelationMetadataResult;

internal RelationMetadataResult relation_static_metadata(
    StringSlice relation_name, bool32 write_names, bool32 write_primary_keys)
{
  RelationId relation_id = 0;
  size_t tuple_length = 0;
  const char *const *column_names = NULL;
  const ColumnType *column_types = NULL;
  const bool32 *column_primary_keys = NULL;

  if (string_slice_eq(
          relation_name, string_slice_from_ptr(relations_relation_name)))
  {
    relation_id = RELATIONS_RELATION_ID;
    tuple_length = ARRAY_LENGTH(relations_types);
    column_names = relations_names;
    column_types = relations_types;
    column_primary_keys = relations_primary_keys;
  }
  else if (string_slice_eq(
               relation_name,
               string_slice_from_ptr(relation_columns_relation_name)))
  {
    relation_id = RELATION_COLUMNS_RELATION_ID;
    tuple_length = ARRAY_LENGTH(relation_columns_types);
    column_names = relation_columns_names;
    column_types = relation_columns_types;
    column_primary_keys = relation_columns_primary_keys;
  }
  else
  {
    return (RelationMetadataResult){.error =
                                        RELATION_METADATA_RELATION_NOT_FOUND};
  }

  ColumnType *types = NULL;
  String *names = NULL;
  bool32 *primary_keys = NULL;

  AllocateError status =
      allocate((void **)&types, sizeof(*types) * tuple_length);

  if (status == ALLOCATE_OK && write_names)
  {
    status = allocate((void **)&names, sizeof(*names) * tuple_length);
  }

  if (status == ALLOCATE_OK && write_primary_keys)
  {
    status =
        allocate((void **)&primary_keys, sizeof(*primary_keys) * tuple_length);
  }

  size_t column_index = 0;
  for (; column_index < tuple_length && status == ALLOCATE_OK; ++column_index)
  {
    types[column_index] = column_types[column_index];

    if (write_primary_keys)
    {
      primary_keys[column_index] = column_primary_keys[column_index];
    }

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
    if (write_primary_keys)
    {
      deallocate(primary_keys, sizeof(*primary_keys) * tuple_length);
    }

    if (write_names)
    {
      for (size_t i = 0; i < column_index; ++i) { string_destroy(&names[i]); }
      deallocate(names, sizeof(*names) * tuple_length);
    }
    return (RelationMetadataResult){.error = RELATION_METADATA_OUT_OF_MEMORY};
  }

  return (RelationMetadataResult){
      .tuple_length = (ColumnsLength)tuple_length,
      .relation_id = relation_id,
      .types = types,
      .names = names,
      .primary_keys = primary_keys,
      .error = RELATION_METADATA_OK,
  };
}

internal RelationMetadataResult relation_metadata(
    DiskBufferPool *pool,
    StringSlice relation_name,
    bool32 write_names,
    bool32 write_primary_keys)
{
  assert(pool != NULL);

  {
    RelationMetadataResult result = relation_static_metadata(
        relation_name, write_names, write_primary_keys);

    switch (result.error)
    {
    case RELATION_METADATA_OK:
    case RELATION_METADATA_OUT_OF_MEMORY:
    case RELATION_METADATA_IO:
      return result;

    case RELATION_METADATA_RELATION_NOT_FOUND:
      break;
    }
  }

  QueryRelationIdByNameResult result =
      query_relation_id_by_name(pool, relation_name);
  switch (result.status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_ERROR:
    return (RelationMetadataResult){.error = RELATION_METADATA_IO};

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
    return (RelationMetadataResult){.error =
                                        RELATION_METADATA_RELATION_NOT_FOUND};
  }

  size_t tuple_length = 0;
  size_t largest_column_id = 0;
  {
    PhysicalRelationIterator i;
    for (i = physical_relation_iterate(pool, RELATION_COLUMNS_RELATION_ID);
         i.status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
         physical_relation_iterator_next(&i))
    {
      ColumnValue tuple_relation_id = physical_relation_iterator_get(
          &i, relation_columns_types, ARRAY_LENGTH(relation_columns_types), 0);

      if (tuple_relation_id.integer != result.relation_id)
      {
        continue;
      }

      ColumnValue column_id = physical_relation_iterator_get(
          &i, relation_columns_types, ARRAY_LENGTH(relation_columns_types), 1);

      largest_column_id = MAX(column_id.integer, largest_column_id);
      tuple_length += 1;
    }
    physical_relation_iterator_close(&i);
  }

  // We don't allow relations without columns to exist
  assert(tuple_length > 0);
  assert(tuple_length == largest_column_id + 1);

  ColumnType *types = NULL;
  String *names = NULL;
  bool32 *primary_keys = NULL;

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

  if (status == ALLOCATE_OK && write_primary_keys)
  {
    status =
        allocate((void **)&primary_keys, sizeof(*primary_keys) * tuple_length);
  }

  bool32 failed = false;
  {
    PhysicalRelationIterator i;
    for (i = physical_relation_iterate(pool, RELATION_COLUMNS_RELATION_ID);
         i.status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
         && status == ALLOCATE_OK;
         physical_relation_iterator_next(&i))
    {
      ColumnValue tuple_relation_id = physical_relation_iterator_get(
          &i, relation_columns_types, ARRAY_LENGTH(relation_columns_types), 0);

      if (tuple_relation_id.integer != result.relation_id)
      {
        continue;
      }

      ColumnValue column_id = physical_relation_iterator_get(
          &i, relation_columns_types, ARRAY_LENGTH(relation_columns_types), 1);

      ColumnValue type = physical_relation_iterator_get(
          &i, relation_columns_types, ARRAY_LENGTH(relation_columns_types), 3);

      types[column_id.integer] = type.integer;

      if (write_primary_keys)
      {
        ColumnValue primary_key = physical_relation_iterator_get(
            &i,
            relation_columns_types,
            ARRAY_LENGTH(relation_columns_types),
            4);

        primary_keys[column_id.integer] = primary_key.boolean > 0;
      }

      if (write_names)
      {
        ColumnValue name = physical_relation_iterator_get(
            &i,
            relation_columns_types,
            ARRAY_LENGTH(relation_columns_types),
            2);

        status = allocate_column_table_name(
            names + column_id.integer, relation_name, name.string);
      }
    }

    switch (i.status)
    {
    case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
      // All tuple should be consumed, so iterator should fail or finish
      assert(false);
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_ERROR:
      failed = true;
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
      failed = false;
      break;
    }

    physical_relation_iterator_close(&i);
  }

  if (status != ALLOCATE_OK || failed)
  {
    deallocate(types, sizeof(*types) * tuple_length);

    if (write_names)
    {
      for (size_t i = 0; i < tuple_length; ++i) { string_destroy(names + i); }
      deallocate(names, sizeof(*names) * tuple_length);
    }

    if (write_primary_keys)
    {
      deallocate(primary_keys, sizeof(*primary_keys) * tuple_length);
    }

    return (RelationMetadataResult){.error = RELATION_METADATA_OUT_OF_MEMORY};
  }

  return (RelationMetadataResult){
      .tuple_length = (ColumnsLength)tuple_length,
      .relation_id = result.relation_id,
      .types = types,
      .names = names,
      .primary_keys = primary_keys,
      .error = RELATION_METADATA_OK,
  };
}

internal void deallocate_relation_metadata(RelationMetadataResult *result)
{

  if (result->names != NULL)
  {
    for (size_t i = 0; i < result->tuple_length; ++i)
    {
      string_destroy(result->names + i);
    }
  }

  deallocate(result->types, sizeof(*result->types) * result->tuple_length);
  deallocate(result->names, sizeof(*result->names) * result->tuple_length);
  deallocate(
      result->primary_keys,
      sizeof(*result->primary_keys) * result->tuple_length);
}

internal bool32 has_at_least_one_primary_key(
    const bool32 *primary_keys, ColumnsLength tuple_length)
{
  assert(primary_keys != NULL);

  for (ColumnsLength i = 0; i < tuple_length; ++i)
  {
    if (primary_keys[i])
    {
      return true;
    }
  }

  return false;
}

bool32 relation_create_schema_relations(DiskBufferPool *pool)
{
  assert(has_at_least_one_primary_key(
      relations_primary_keys, ARRAY_LENGTH(relations_primary_keys)));

  assert(has_at_least_one_primary_key(
      relation_columns_primary_keys,
      ARRAY_LENGTH(relation_columns_primary_keys)));

  // TODO: Use logical_relation_create or a subset of it to enforce checks
  PhysicalRelationCreateError error =
      physical_relation_create(pool, RELATIONS_RELATION_ID, false);
  switch (error)
  {
  case PHYSICAL_RELATION_CREATE_OK:
  case PHYSICAL_RELATION_CREATE_ALREADY_EXISTS:
    break;

  case PHYSICAL_RELATION_CREATE_FAILED_TO_CREATE:
  case PHYSICAL_RELATION_CREATE_FAILED_TO_STAT:
  case PHYSICAL_RELATION_CREATE_PROGRAM_ERROR:
  case PHYSICAL_RELATION_CREATE_FAILED_TO_WRITE:
    return false;
  }

  // TODO: Use logical_relation_create or a subset of it to enforce checks
  error = physical_relation_create(pool, RELATION_COLUMNS_RELATION_ID, false);
  switch (error)
  {
  case PHYSICAL_RELATION_CREATE_OK:
  case PHYSICAL_RELATION_CREATE_ALREADY_EXISTS:
    break;

  case PHYSICAL_RELATION_CREATE_FAILED_TO_CREATE:
  case PHYSICAL_RELATION_CREATE_FAILED_TO_STAT:
  case PHYSICAL_RELATION_CREATE_PROGRAM_ERROR:
  case PHYSICAL_RELATION_CREATE_FAILED_TO_WRITE:
    return false;
  }

  return true;
}

// TODO: Check that column names are unique
LogicalRelationCreateError logical_relation_create(
    DiskBufferPool *pool,
    StringSlice relation_name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length)
{
  assert(pool != NULL);
  assert(relation_name.length > 0);
  assert(names != NULL);
  assert(types != NULL);
  assert(primary_keys != NULL);
  assert(tuple_length > 0);

  QueryNewRelationIdByNameResult result =
      query_new_relation_id(pool, relation_name);
  switch (result.error)
  {
  case QUERY_NEW_RELATION_ID_OK:
    break;

  case QUERY_NEW_RELATION_ID_ALREADY_EXISTS:
    return LOGICAL_RELATION_CREATE_ALREADY_EXISTS;

  case QUERY_NEW_RELATION_ID_READING:
    return LOGICAL_RELATION_CREATE_IO;

  case QUERY_NEW_RELATION_ID_PROGRAM_ERROR:
    return LOGICAL_RELATION_CREATE_PROGRAM_ERROR;
  }

  assert(result.relation_id >= RESERVED_RELATION_IDS);

  assert(pool != NULL);
  assert(names != NULL);
  assert(types != NULL);
  assert(primary_keys != NULL);
  assert(tuple_length > 0);

  if (!has_at_least_one_primary_key(primary_keys, tuple_length))
  {
    return LOGICAL_RELATION_CREATE_NO_PRIMARY_KEY;
  }

  switch (physical_relation_create(pool, result.relation_id, true))
  {
  case PHYSICAL_RELATION_CREATE_OK:
    break;

  case PHYSICAL_RELATION_CREATE_FAILED_TO_CREATE:
  case PHYSICAL_RELATION_CREATE_FAILED_TO_STAT:
  case PHYSICAL_RELATION_CREATE_FAILED_TO_WRITE:
  case PHYSICAL_RELATION_CREATE_ALREADY_EXISTS:
    return LOGICAL_RELATION_CREATE_IO;

  case PHYSICAL_RELATION_CREATE_PROGRAM_ERROR:
    return LOGICAL_RELATION_CREATE_PROGRAM_ERROR;
  }

  PhysicalRelationInsertTupleError insert_error =
      PHYSICAL_RELATION_INSERT_TUPLE_OK;
  for (int16_t column = 0; column < tuple_length
                           && insert_error == PHYSICAL_RELATION_INSERT_TUPLE_OK;
       ++column)
  {
    ColumnValue relation_column_values[] = {
        {.integer = result.relation_id},
        {.integer = column},
        {.string = names[column]},
        {.integer = types[column]},
        {.boolean = (StoreBoolean)(primary_keys[column] != 0)},
    };
    STATIC_ASSERT(
        ARRAY_LENGTH(relation_column_values)
        == ARRAY_LENGTH(relation_columns_types));

    // TODO: Use logical_relation_insert_tuple or a subset of it to enforce
    // checks
    insert_error = physical_relation_insert_tuple(
        pool,
        RELATION_COLUMNS_RELATION_ID,
        relation_columns_types,
        relation_column_values,
        relation_columns_primary_keys,
        ARRAY_LENGTH(relation_column_values));
  }

  if (insert_error == PHYSICAL_RELATION_INSERT_TUPLE_OK)
  {
    ColumnValue relations_values[] = {
        {.integer = result.relation_id},
        {.string = relation_name},
    };
    STATIC_ASSERT(
        ARRAY_LENGTH(relations_values) == ARRAY_LENGTH(relations_types));

    // TODO: Use logical_relation_insert_tuple or a subset of it to enforce
    insert_error = physical_relation_insert_tuple(
        pool,
        RELATIONS_RELATION_ID,
        relations_types,
        relations_values,
        relations_primary_keys,
        ARRAY_LENGTH(relations_values));
  }

  // TODO: Use transactions to handle failure
  assert(insert_error == PHYSICAL_RELATION_INSERT_TUPLE_OK);

  return LOGICAL_RELATION_CREATE_OK;
}

LogicalRelationDropError
logical_relation_drop(DiskBufferPool *pool, StringSlice relation_name)
{
  assert(pool != NULL);
  assert(relation_name.length > 0);

  QueryRelationIdByNameResult result =
      query_relation_id_by_name(pool, relation_name);
  switch (result.status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_ERROR:
    return LOGICAL_RELATION_DROP_IO;

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES:
    return LOGICAL_RELATION_DROP_NOT_FOUND;
  }

  // TODO: Use logical_relation_delete_tuples or a subset of it to enforce
  PhysicalRelationDeleteTuplesError error = physical_relation_delete_tuples(
      pool,
      RELATION_COLUMNS_RELATION_ID,
      relation_columns_types,
      ARRAY_LENGTH(relation_columns_types),
      0,
      (ColumnValue){.integer = result.relation_id});

  // TODO: Use transactions to handle failure
  assert(error == PHYSICAL_RELATION_DELETE_TUPLES_OK);

  // TODO: Use logical_relation_delete_tuples or a subset of it to enforce
  error = physical_relation_delete_tuples(
      pool,
      RELATIONS_RELATION_ID,
      relations_types,
      ARRAY_LENGTH(relations_types),
      0,
      (ColumnValue){.integer = result.relation_id});

  // TODO: Use transactions to handle failure
  assert(error == PHYSICAL_RELATION_DELETE_TUPLES_OK);

  return LOGICAL_RELATION_DROP_OK;
}

internal bool32 tuple_violates_primary_key(
    DiskBufferPool *pool,
    RelationId relation_id,
    ColumnsLength tuple_length,
    const ColumnType *types,
    const ColumnValue *values,
    const bool32 *primary_keys)
{
  PhysicalRelationIterator i;
  for (i = physical_relation_iterate(pool, relation_id);
       i.status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       physical_relation_iterator_next(&i))
  {
    bool32 matches_all = true;
    for (ColumnsLength column = 0; column < tuple_length && matches_all;
         ++column)
    {
      if (!primary_keys[column])
      {
        continue;
      }

      ColumnValue value =
          physical_relation_iterator_get(&i, types, tuple_length, column);

      switch (types[column])
      {
      case COLUMN_TYPE_INTEGER:
        matches_all = value.integer == values[column].integer;
        break;

      case COLUMN_TYPE_BOOLEAN:
        matches_all = value.boolean == values[column].boolean;
        break;

      case COLUMN_TYPE_STRING:
        matches_all = string_slice_eq(values[column].string, value.string);
        break;
      }
    }

    if (matches_all)
    {
      physical_relation_iterator_close(&i);
      return true;
    }
  }
  physical_relation_iterator_close(&i);

  return false;
}

// TODO: Take relation as argument
LogicalRelationInsertTupleError logical_relation_insert_tuple(
    DiskBufferPool *pool,
    StringSlice relation_name,
    const ColumnType *types,
    const ColumnValue *values,
    ColumnsLength tuple_length)
{
  assert(pool != NULL);
  assert(types != NULL);
  assert(values != NULL);
  assert(tuple_length > 0);

  RelationMetadataResult result =
      relation_metadata(pool, relation_name, false, true);

  switch (result.error)
  {
  case RELATION_METADATA_OK:
    break;

  case RELATION_METADATA_OUT_OF_MEMORY:
    return LOGICAL_RELATION_INSERT_TUPLE_OUT_OF_MEMORY;

  case RELATION_METADATA_RELATION_NOT_FOUND:
    return LOGICAL_RELATION_INSERT_TUPLE_NOT_FOUND;

  case RELATION_METADATA_IO:
    return LOGICAL_RELATION_INSERT_TUPLE_IO;
  }

  if (result.relation_id < RESERVED_RELATION_IDS)
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_INSERT_TUPLE_NOT_FOUND;
  }

  if (tuple_length != result.tuple_length)
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_INSERT_TUPLE_TUPLE_LENGTH_MISMATCH;
  }

  for (size_t i = 0; i < tuple_length; ++i)
  {
    if (types[i] != result.types[i])
    {
      deallocate_relation_metadata(&result);
      return LOGICAL_RELATION_INSERT_TUPLE_COLUMN_TYPE_MISMATCH;
    }
  }

  if (tuple_violates_primary_key(
          pool,
          result.relation_id,
          result.tuple_length,
          result.types,
          values,
          result.primary_keys))
  {
    return LOGICAL_RELATION_INSERT_TUPLE_PRIMARY_KEY_VIOLATION;
  }

  PhysicalRelationInsertTupleError insert_error =
      physical_relation_insert_tuple(
          pool,
          result.relation_id,
          result.types,
          values,
          result.primary_keys,
          tuple_length);

  deallocate_relation_metadata(&result);

  switch (insert_error)
  {
  case PHYSICAL_RELATION_INSERT_TUPLE_OK:
    return LOGICAL_RELATION_INSERT_TUPLE_OK;

  case PHYSICAL_RELATION_INSERT_TUPLE_SAVING:
  case PHYSICAL_RELATION_INSERT_TUPLE_OPENING_BUFFER:
  case PHYSICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL:
    // Use transactions to handle failure
    assert(false);
    return LOGICAL_RELATION_INSERT_TUPLE_IO;

  case PHYSICAL_RELATION_INSERT_TUPLE_TOO_BIG:
    return LOGICAL_RELATION_INSERT_TUPLE_TOO_BIG;
  }
}

LogicalRelationDeleteTuplesError logical_relation_delete_tuples(
    DiskBufferPool *pool,
    StringSlice relation_name,
    // TDOO: take column name instead of index
    ColumnsLength column_index,
    ColumnType type,
    ColumnValue value)
{
  assert(pool != NULL);

  RelationMetadataResult result =
      relation_metadata(pool, relation_name, false, false);

  switch (result.error)
  {
  case RELATION_METADATA_OK:
    break;

  case RELATION_METADATA_OUT_OF_MEMORY:
    return LOGICAL_RELATION_DELETE_TUPLES_OUT_OF_MEMORY;

  case RELATION_METADATA_RELATION_NOT_FOUND:
    return LOGICAL_RELATION_DELETE_TUPLES_NOT_FOUND;

  case RELATION_METADATA_IO:
    return LOGICAL_RELATION_DELETE_TUPLES_IO;
  }

  if (result.relation_id < RESERVED_RELATION_IDS)
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_DELETE_TUPLES_NOT_FOUND;
  }

  if (column_index >= result.tuple_length)
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_DELETE_TUPLES_TUPLE_LENGTH_MISMATCH;
  }

  if (type != result.types[column_index])
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_DELETE_TUPLES_COLUMN_TYPE_MISMATCH;
  }

  PhysicalRelationDeleteTuplesError error = physical_relation_delete_tuples(
      pool,
      result.relation_id,
      result.types,
      result.tuple_length,
      column_index,
      value);
  // TODO: Use transactions to handle failure
  assert(error == PHYSICAL_RELATION_DELETE_TUPLES_OK);

  deallocate_relation_metadata(&result);

  return LOGICAL_RELATION_DELETE_TUPLES_OK;
}

// ---------- Logical Relation ----------

// ----- Query -----

internal bool32 string_like_operator(StringSlice string, StringSlice pattern)
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
      PhysicalRelationIterator it;
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

internal void tuple_iterator_destroy(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
  {
    physical_relation_iterator_close(&it->read.it);
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

ColumnsLength tuple_iterator_tuple_length(TupleIterator *it)
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

PhysicalRelationIteratorStatus tuple_iterator_valid(TupleIterator *it)
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

internal void
cartesian_product_tuple_iterator_get_target_iterator_and_column_id(
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

StringSlice
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

ColumnType
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

ColumnValue
tuple_iterator_column_value(TupleIterator *it, ColumnsLength column_id)
{
  assert(tuple_iterator_valid(it) == PHYSICAL_RELATION_ITERATOR_STATUS_OK);
  assert(column_id < tuple_iterator_tuple_length(it));

  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return physical_relation_iterator_get(
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

internal bool32 tuple_iterator_select_tuple(TupleIterator *it)
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

    case PREDICATE_OPERATOR_GRANULAR_BOOLEAN_EQUAL:
      satisfied = lhs.boolean == rhs.boolean;
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

internal void tuple_iterator_reset(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
  {
    physical_relation_iterator_close(&it->read.it);
    it->read.it =
        physical_relation_iterate(it->read.it.pool, it->read.it.relation_id);
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

void tuple_iterator_next(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    physical_relation_iterator_next(&it->read.it);
    break;

  case QUERY_OPERATOR_PROJECT:
    tuple_iterator_next(it->project.it);
    break;

  case QUERY_OPERATOR_SELECT:
  {
    for (tuple_iterator_next(it->select.it);
         tuple_iterator_valid(it->select.it)
             == PHYSICAL_RELATION_ITERATOR_STATUS_OK
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
        == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_TUPLES)
    {
      tuple_iterator_reset(it->cartesian_product.rhs);
      tuple_iterator_next(it->cartesian_product.lhs);
    }
  }
  break;
  }
}

// TODO: Raise errors for ambiguous names. Example:
//   search_name = user_id
//   columns = users.user_id, roles.user_id
internal bool32
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

internal bool32 tuple_iterator_find_column_name(
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

internal QueryIteratorError database_query_select_fill_predicate_variable(
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
      return QUERY_ITERATOR_COLUMN_NOT_FOUND;
    }

    *out = (TupleIteratorPredicateVariable){
        .type = in.type,
        .column_index = column_index,
    };
    *column_type = tuple_iterator_column_type(it, column_index);
  }
  break;
  }

  return QUERY_ITERATOR_OK;
}

QueryIteratorError database_query_select(
    SelectQueryParameter select, TupleIterator *it, TupleIterator *result)
{
  TupleIteratorCondition *conditions = NULL;
  if (allocate((void **)&conditions, sizeof(*conditions) * select.length)
      != ALLOCATE_OK)
  {
    return QUERY_ITERATOR_OUT_OF_MEMORY;
  }

  for (size_t i = 0; i < select.length; ++i)
  {
    SelectQueryCondition query_condition = select.conditions[i];
    if (query_condition.operator == PREDICATE_OPERATOR_TRUE)
    {
      conditions[i] = (TupleIteratorCondition){
          .operator = PREDICATE_OPERATOR_GRANULAR_TRUE,
          .lhs = {},
          .rhs = {},
      };
      continue;
    }

    TupleIteratorPredicateVariable lhs = {};
    ColumnType lhs_column_type = {};
    {
      QueryIteratorError error = database_query_select_fill_predicate_variable(
          it, query_condition.lhs, &lhs, &lhs_column_type);
      if (error != QUERY_ITERATOR_OK)
      {
        return error;
      }
    }

    TupleIteratorPredicateVariable rhs = {};
    ColumnType rhs_column_type = {};
    {
      QueryIteratorError error = database_query_select_fill_predicate_variable(
          it, query_condition.rhs, &rhs, &rhs_column_type);
      if (error != QUERY_ITERATOR_OK)
      {
        return error;
      }
    }

    PredicateOperatorGranular operator = {};
    switch (query_condition.operator)
    {
    case PREDICATE_OPERATOR_TRUE:
      assert(false);
      break;

    case PREDICATE_OPERATOR_EQUAL:
      if (lhs_column_type != rhs_column_type)
      {
        return QUERY_ITERATOR_OPERATOR_TYPE_MISMATCH;
      }

      switch (lhs_column_type)
      {
      case COLUMN_TYPE_INTEGER:
        operator = PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL;
        break;

      case COLUMN_TYPE_BOOLEAN:
        operator = PREDICATE_OPERATOR_GRANULAR_BOOLEAN_EQUAL;
        break;

      case COLUMN_TYPE_STRING:
        operator = PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL;
        break;
      }
      break;

    case PREDICATE_OPERATOR_STRING_LIKE:
      if (lhs_column_type != COLUMN_TYPE_STRING)
      {
        return QUERY_ITERATOR_OPERATOR_TYPE_MISMATCH;
      }

      if (rhs_column_type != COLUMN_TYPE_STRING)
      {
        return QUERY_ITERATOR_OPERATOR_TYPE_MISMATCH;
      }

      operator = PREDICATE_OPERATOR_GRANULAR_STRING_LIKE;
      break;
    }

    conditions[i] = (TupleIteratorCondition){
        .operator = operator,
        .lhs = lhs,
        .rhs = rhs,
    };
  }

  *result = (TupleIterator){
      .operator = QUERY_OPERATOR_SELECT,
      .select =
          {
              .it = it,
              .length = select.length,
              .conditions = conditions,
          },
  };

  return QUERY_ITERATOR_OK;
}

QueryIteratorError query_iterator_new(
    DiskBufferPool *pool,
    size_t length,
    const QueryParameter *parameters,
    QueryIterator *it)
{
  // TODO: Make sure the dependendencies have no loops
  assert(pool != NULL);
  assert(length > 0);
  assert(parameters != NULL);
  assert(it != NULL);

  TupleIterator *iterators = NULL;
  if (allocate((void **)&iterators, sizeof(TupleIterator) * length)
      != ALLOCATE_OK)
  {
    return QUERY_ITERATOR_OUT_OF_MEMORY;
  }

  QueryIteratorError status = QUERY_ITERATOR_OK;
  size_t query = 0;
  for (; status == QUERY_ITERATOR_OK && query < length; ++query)
  {
    switch (parameters[query].operator)
    {
    case QUERY_OPERATOR_READ:
    {
      RelationMetadataResult result = relation_metadata(
          pool, parameters[query].read_relation_name, true, false);
      switch (result.error)
      {
      case RELATION_METADATA_OK:
        iterators[query] = (TupleIterator){
            .operator = QUERY_OPERATOR_READ,
            .read =
                {
                    .it = physical_relation_iterate(pool, result.relation_id),
                    .column_names = result.names,
                    .column_types = result.types,
                    .tuple_length = result.tuple_length,
                },
        };
        break;

      case RELATION_METADATA_OUT_OF_MEMORY:
        status = QUERY_ITERATOR_OUT_OF_MEMORY;
        break;

      case RELATION_METADATA_RELATION_NOT_FOUND:
        status = QUERY_ITERATOR_RELATION_NOT_FOUND;
        break;

      case RELATION_METADATA_IO:
        status = QUERY_ITERATOR_READING_DISK;
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
        status = QUERY_ITERATOR_OUT_OF_MEMORY;
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
        status = QUERY_ITERATOR_COLUMN_NOT_FOUND;
        break;
      }

      iterators[query] = (TupleIterator){
          .operator = QUERY_OPERATOR_PROJECT,
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
          .operator = QUERY_OPERATOR_CARTESIAN_PRODUCT,
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

  if (status != QUERY_ITERATOR_OK)
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
  // way each iterators starts in a good state without having to account for
  // it into the initialization code above
  tuple_iterator_reset(&it->iterators[it->length - 1]);

  return QUERY_ITERATOR_OK;
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
