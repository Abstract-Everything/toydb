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

internal bool32 tuple_violates_primary_key(
    DiskBufferPool *pool,
    RelationId relation_id,
    const bool32 *primary_keys,
    Tuple tuple)
{
  PhysicalRelationIterator it =
      physical_relation_iterator(pool, relation_id, tuple.length, tuple.types);

  PhysicalRelationIteratorStatus status = physical_relation_iterate_tuples(&it);

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = physical_relation_iterator_next_tuple(&it))
  {
    bool32 matches_all = true;

    Tuple stored_tuple = physical_relation_iterator_get(&it);

    for (ColumnsLength column = 0; column < tuple.length && matches_all;
         ++column)
    {
      if (!primary_keys[column])
      {
        continue;
      }

      ColumnValue value = tuple_get(stored_tuple, column);

      switch (tuple.types[column])
      {
      case COLUMN_TYPE_INTEGER:
        matches_all = value.integer == tuple_get(tuple, column).integer;
        break;

      case COLUMN_TYPE_BOOLEAN:
        matches_all = value.boolean == tuple_get(tuple, column).boolean;
        break;

      case COLUMN_TYPE_STRING:
        matches_all =
            string_slice_eq(tuple_get(tuple, column).string, value.string);
        break;
      }
    }

    if (matches_all)
    {
      physical_relation_iterator_close(&it);
      return true;
    }
  }
  physical_relation_iterator_close(&it);

  // TODO: return error
  assert(status == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS);

  return false;
}

internal LogicalRelationInsertTupleError insert_tuple(
    DiskBufferPool *pool,
    WriteAheadLog *log,
    RelationId relation_id,
    ColumnsLength tuple_length,
    const ColumnType *types,
    const bool32 *primary_keys,
    Tuple tuple)
{
  assert(pool != NULL);

  if (tuple.length != tuple_length)
  {
    return LOGICAL_RELATION_INSERT_TUPLE_TUPLE_LENGTH_MISMATCH;
  }

  for (size_t i = 0; i < tuple.length; ++i)
  {
    if (tuple.types[i] != types[i])
    {
      return LOGICAL_RELATION_INSERT_TUPLE_COLUMN_TYPE_MISMATCH;
    }
  }

  if (tuple_violates_primary_key(pool, relation_id, primary_keys, tuple))
  {
    return LOGICAL_RELATION_INSERT_TUPLE_PRIMARY_KEY_VIOLATION;
  }

  bool32 empty_block_visited = false;

  PhysicalRelationIterator it =
      physical_relation_iterator(pool, relation_id, tuple_length, types);

  PhysicalRelationIteratorStatus status =
      physical_relation_iterator_open(&it, 0);

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = physical_relation_iterator_next_block(&it))
  {
    if (physical_relation_iterator_insert_tuple_fits(&it, tuple))
    {
      break;
    }

    empty_block_visited =
        empty_block_visited || physical_relation_iterator_is_block_empty(&it);
  }

  LogicalRelationInsertTupleError error = LOGICAL_RELATION_INSERT_TUPLE_OK;

  switch (status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
  {
    // If an empty block insert has already been attempted creating a new block
    // will fail since it will have the same space available
    if (empty_block_visited)
    {
      error = LOGICAL_RELATION_INSERT_TUPLE_TOO_BIG;
      break;
    }

    switch (physical_relation_iterator_new_block(&it))
    {
    case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
      assert(false);
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
      error = LOGICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
    case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
      error = LOGICAL_RELATION_INSERT_TUPLE_IO;
      break;
    }
  }
  break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
    error = LOGICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
  case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
    error = LOGICAL_RELATION_INSERT_TUPLE_IO;
    break;
  }

  if (error == LOGICAL_RELATION_INSERT_TUPLE_OK)
  {
    WalWriteResult wal_result = wal_write_tuple_entry(
        log,
        WAL_ENTRY_INSERT_TUPLE,
        relation_id,
        physical_relation_iterator_block_index(&it),
        tuple);
    switch (wal_result.error)
    {
    case WAL_WRITE_ENTRY_OK:
      physical_relation_iterator_insert(&it, wal_result.lsn, tuple);
      error = LOGICAL_RELATION_INSERT_TUPLE_OK;
      break;

    case WAL_WRITE_ENTRY_PROGRAM_ERROR:
    case WAL_WRITE_ENTRY_TOO_BIG:
    case WAL_WRITE_ENTRY_WRITING_SEGMENT:
    case WAL_WRITE_ENTRY_READING_SEGMENT:
      error = LOGICAL_RELATION_INSERT_TUPLE_IO;
      break;
    }
  }

  physical_relation_iterator_close(&it);

  return error;
}

internal LogicalRelationDeleteTuplesError delete_tuples(
    DiskBufferPool *pool,
    WriteAheadLog *log,
    RelationId relation_id,
    ColumnsLength tuple_length,
    const ColumnType *types,
    const bool32 *compare_column,
    Tuple tuple)
{
  assert(pool != NULL);

  for (ColumnsLength i = 0; i < tuple.length; ++i)
  {
    if (tuple.types[i] != types[i])
    {
      return LOGICAL_RELATION_DELETE_TUPLES_COLUMN_TYPE_MISMATCH;
    }
  }

  LogicalRelationDeleteTuplesError error = LOGICAL_RELATION_DELETE_TUPLES_IO;

  PhysicalRelationIterator it =
      physical_relation_iterator(pool, relation_id, tuple_length, types);

  PhysicalRelationIteratorStatus status = physical_relation_iterate_tuples(&it);

  while (status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
  {
    Tuple stored_tuple = physical_relation_iterator_get(&it);

    bool32 delete = true;
    for (ColumnsLength i = 0; i < tuple.length && delete; ++i)
    {
      if (compare_column[i])
      {
        delete = column_value_eq(
            tuple.types[i], tuple_get(stored_tuple, i), tuple_get(tuple, i));
      }
    }

    if (delete)
    {
      WalWriteResult delete_result = wal_write_tuple_entry(
          log,
          WAL_ENTRY_DELETE_TUPLE,
          relation_id,
          physical_relation_iterator_block_index(&it),
          stored_tuple);
      switch (delete_result.error)
      {
      case WAL_WRITE_ENTRY_OK:
        status = physical_relation_iterator_delete(&it, delete_result.lsn);
        error = LOGICAL_RELATION_DELETE_TUPLES_OK;
        break;

      case WAL_WRITE_ENTRY_PROGRAM_ERROR:
      case WAL_WRITE_ENTRY_TOO_BIG:
      case WAL_WRITE_ENTRY_WRITING_SEGMENT:
      case WAL_WRITE_ENTRY_READING_SEGMENT:
        error = LOGICAL_RELATION_DELETE_TUPLES_IO;
        break;
      }
    }
    else
    {
      status = physical_relation_iterator_next_tuple(&it);
    }
  }
  physical_relation_iterator_close(&it);

  if (error != LOGICAL_RELATION_DELETE_TUPLES_OK)
  {
    return error;
  }

  switch (status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
    return LOGICAL_RELATION_DELETE_TUPLES_OK;

  case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
  case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
    return LOGICAL_RELATION_DELETE_TUPLES_IO;

  case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
    return LOGICAL_RELATION_DELETE_TUPLES_BUFFER_POOL_FULL;
  }
}

typedef struct
{
  RelationId relation_id;
  PhysicalRelationIteratorStatus status;
} QueryRelationIdByNameResult;

internal QueryRelationIdByNameResult
query_relation_id_by_name(DiskBufferPool *pool, StringSlice name)
{
  RelationId relation_id = 0;

  PhysicalRelationIterator it = physical_relation_iterator(
      pool,
      RELATIONS_RELATION_ID,
      ARRAY_LENGTH(relations_types),
      relations_types);

  PhysicalRelationIteratorStatus status = physical_relation_iterate_tuples(&it);

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = physical_relation_iterator_next_tuple(&it))
  {
    Tuple tuple = physical_relation_iterator_get(&it);

    if (string_slice_eq(name, tuple_get_string(tuple, 1)))
    {
      relation_id = tuple_get_integer(tuple, 0);
      break;
    }
  }
  physical_relation_iterator_close(&it);

  return (QueryRelationIdByNameResult){
      .status = status,
      .relation_id = relation_id,
  };
}

typedef struct
{
  String name;
  PhysicalRelationIteratorStatus status;
} QueryRelationNameByIdResult;

internal QueryRelationNameByIdResult
query_relation_name_by_id(DiskBufferPool *pool, RelationId id)
{
  String name = {};

  if (id == RELATIONS_RELATION_ID)
  {
    assert(
        string_from_string_slice(
            string_slice_from_ptr(relations_relation_name), &name)
        == ALLOCATE_OK);

    return (QueryRelationNameByIdResult){
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_OK,
        .name = name,
    };
  }

  if (id == RELATION_COLUMNS_RELATION_ID)
  {
    assert(
        string_from_string_slice(
            string_slice_from_ptr(relation_columns_relation_name), &name)
        == ALLOCATE_OK);

    return (QueryRelationNameByIdResult){
        .status = PHYSICAL_RELATION_ITERATOR_STATUS_OK,
        .name = name,
    };
  }

  PhysicalRelationIterator it = physical_relation_iterator(
      pool,
      RELATIONS_RELATION_ID,
      ARRAY_LENGTH(relations_types),
      relations_types);

  PhysicalRelationIteratorStatus status = physical_relation_iterate_tuples(&it);

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = physical_relation_iterator_next_tuple(&it))
  {
    Tuple tuple = physical_relation_iterator_get(&it);

    if (id == tuple_get_integer(tuple, 0))
    {
      assert(
          string_from_string_slice(tuple_get_string(tuple, 1), &name)
          == ALLOCATE_OK);
      break;
    }
  }
  physical_relation_iterator_close(&it);

  return (QueryRelationNameByIdResult){
      .status = status,
      .name = name,
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

  PhysicalRelationIterator it = physical_relation_iterator(
      pool,
      RELATIONS_RELATION_ID,
      ARRAY_LENGTH(relations_types),
      relations_types);

  PhysicalRelationIteratorStatus status = physical_relation_iterate_tuples(&it);

  for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
       status = physical_relation_iterator_next_tuple(&it))
  {
    Tuple tuple = physical_relation_iterator_get(&it);

    if (string_slice_eq(name, tuple_get_string(tuple, 1)))
    {
      physical_relation_iterator_close(&it);
      return (QueryNewRelationIdByNameResult){
          .error = QUERY_NEW_RELATION_ID_ALREADY_EXISTS,
      };
    }

    relation_id = tuple_get_integer(tuple, 0) + 1;
  }
  physical_relation_iterator_close(&it);

  switch (status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    assert(false);
    return (QueryNewRelationIdByNameResult){
        .error = QUERY_NEW_RELATION_ID_PROGRAM_ERROR,
    };

  case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
  case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
  case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
    return (QueryNewRelationIdByNameResult){
        .error = QUERY_NEW_RELATION_ID_READING,
    };

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
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
  RELATION_METADATA_PROGRAM_ERROR,
  RELATION_METADATA_BUFFER_POOL_FULL,
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
    case RELATION_METADATA_PROGRAM_ERROR:
    case RELATION_METADATA_BUFFER_POOL_FULL:
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

  case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
  case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
  case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
    return (RelationMetadataResult){.error = RELATION_METADATA_IO};

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
    return (RelationMetadataResult){
        .error = RELATION_METADATA_RELATION_NOT_FOUND,
    };
  }

  size_t tuple_length = 0;
  size_t largest_column_id = 0;
  {
    PhysicalRelationIterator it = physical_relation_iterator(
        pool,
        RELATION_COLUMNS_RELATION_ID,
        ARRAY_LENGTH(relation_columns_types),
        relation_columns_types);

    PhysicalRelationIteratorStatus iterator_status =
        physical_relation_iterate_tuples(&it);

    for (; iterator_status == PHYSICAL_RELATION_ITERATOR_STATUS_OK;
         iterator_status = physical_relation_iterator_next_tuple(&it))
    {
      Tuple tuple = physical_relation_iterator_get(&it);

      if (tuple_get_integer(tuple, 0) != result.relation_id)
      {
        continue;
      }

      largest_column_id = MAX(tuple_get_integer(tuple, 1), largest_column_id);
      tuple_length += 1;
    }
    physical_relation_iterator_close(&it);

    switch (iterator_status)
    {
    case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
      // All tuple should be consumed, so iterator should fail or finish
      assert(false);
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
    case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
    case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
      return (RelationMetadataResult){.error = RELATION_METADATA_IO};
    }
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
    PhysicalRelationIterator it = physical_relation_iterator(
        pool,
        RELATION_COLUMNS_RELATION_ID,
        ARRAY_LENGTH(relation_columns_types),
        relation_columns_types);

    PhysicalRelationIteratorStatus iterator_status =
        physical_relation_iterate_tuples(&it);

    for (; iterator_status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
           && status == ALLOCATE_OK;
         iterator_status = physical_relation_iterator_next_tuple(&it))
    {
      Tuple tuple = physical_relation_iterator_get(&it);

      if (tuple_get_integer(tuple, 0) != result.relation_id)
      {
        continue;
      }

      StoreInteger column_id = tuple_get_integer(tuple, 1);

      types[column_id] = tuple_get_integer(tuple, 3);

      if (write_primary_keys)
      {
        primary_keys[column_id] = tuple_get_boolean(tuple, 4) > 0;
      }

      if (write_names)
      {
        status = allocate_column_table_name(
            names + column_id, relation_name, tuple_get_string(tuple, 2));
      }
    }
    physical_relation_iterator_close(&it);

    switch (iterator_status)
    {
    case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
      // All tuple should be consumed, so iterator should fail or finish
      assert(false);
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
    case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
    case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
      failed = true;
      break;

    case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
      failed = false;
      break;
    }
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

internal RelationMetadataResult relation_metadata_from_id(
    DiskBufferPool *pool,
    RelationId relation_id,
    bool32 write_names,
    bool32 write_primary_keys)
{
  QueryRelationNameByIdResult query_result =
      query_relation_name_by_id(pool, relation_id);

  RelationMetadataError error = RELATION_METADATA_OK;
  switch (query_result.status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
    error = RELATION_METADATA_RELATION_NOT_FOUND;
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
  case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
    error = RELATION_METADATA_IO;
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
    error = RELATION_METADATA_BUFFER_POOL_FULL;
    break;
  }

  if (error != RELATION_METADATA_OK)
  {
    return (RelationMetadataResult){.error = error};
  }

  RelationMetadataResult result = relation_metadata(
      pool,
      string_slice_from_string(query_result.name),
      write_names,
      write_primary_keys);

  string_destroy(&query_result.name);

  return result;
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

  DiskResourceCreateError error = disk_buffer_pool_resource_create(
      pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .id = RELATIONS_RELATION_ID,
      },
      false);

  switch (error)
  {
  case DISK_RESOURCE_CREATE_OK:
  case DISK_RESOURCE_CREATE_ALREADY_EXISTS:
    break;

  case DISK_RESOURCE_CREATE_OPENING:
  case DISK_RESOURCE_CREATE_STAT:
  case DISK_RESOURCE_CREATE_PROGRAM_ERROR:
  case DISK_RESOURCE_CREATE_TRUNCATING:
  case DISK_RESOURCE_CREATE_CLOSING:
    return false;
  }

  error = disk_buffer_pool_resource_create(
      pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .id = RELATION_COLUMNS_RELATION_ID,
      },
      false);

  switch (error)
  {
  case DISK_RESOURCE_CREATE_OK:
  case DISK_RESOURCE_CREATE_ALREADY_EXISTS:
    break;

  case DISK_RESOURCE_CREATE_OPENING:
  case DISK_RESOURCE_CREATE_STAT:
  case DISK_RESOURCE_CREATE_PROGRAM_ERROR:
  case DISK_RESOURCE_CREATE_TRUNCATING:
  case DISK_RESOURCE_CREATE_CLOSING:
    return false;
  }

  return true;
}

// TODO: Check that column names are unique
LogicalRelationCreateError logical_relation_create(
    DiskBufferPool *pool,
    WriteAheadLog *log,
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

  if (!has_at_least_one_primary_key(primary_keys, tuple_length))
  {
    return LOGICAL_RELATION_CREATE_NO_PRIMARY_KEY;
  }

  WalWriteResult write_result = wal_write_entry(
      log,
      (WalEntry){
          .header.tag = WAL_ENTRY_CREATE_RELATION_FILE,
          .payload.relation_id = result.relation_id,
      },
      0,
      NULL);
  switch (write_result.error)
  {
  case WAL_WRITE_ENTRY_OK:
    break;

  case WAL_WRITE_ENTRY_PROGRAM_ERROR:
  case WAL_WRITE_ENTRY_TOO_BIG:
  case WAL_WRITE_ENTRY_WRITING_SEGMENT:
  case WAL_WRITE_ENTRY_READING_SEGMENT:
    return LOGICAL_RELATION_CREATE_IO;
  }

  switch (disk_buffer_pool_resource_create(
      pool,
      (DiskResource){.type = RESOURCE_TYPE_RELATION, .id = result.relation_id},
      true))
  {
  case DISK_RESOURCE_CREATE_OK:
    break;

  case DISK_RESOURCE_CREATE_OPENING:
  case DISK_RESOURCE_CREATE_STAT:
  case DISK_RESOURCE_CREATE_ALREADY_EXISTS:
  case DISK_RESOURCE_CREATE_TRUNCATING:
  case DISK_RESOURCE_CREATE_CLOSING:
    return LOGICAL_RELATION_CREATE_IO;

  case DISK_RESOURCE_CREATE_PROGRAM_ERROR:
    return LOGICAL_RELATION_CREATE_PROGRAM_ERROR;
  }

  LogicalRelationInsertTupleError insert_error =
      LOGICAL_RELATION_INSERT_TUPLE_OK;
  for (int16_t column = 0; column < tuple_length
                           && insert_error == LOGICAL_RELATION_INSERT_TUPLE_OK;
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

    // TODO: Use arena allocator
    size_t data_length = tuple_data_length(
        ARRAY_LENGTH(relation_columns_types),
        relation_columns_types,
        relation_column_values);
    char data[data_length] = {};

    insert_error = insert_tuple(
        pool,
        log,
        RELATION_COLUMNS_RELATION_ID,
        ARRAY_LENGTH(relation_column_values),
        relation_columns_types,
        relation_columns_primary_keys,
        tuple_from_data(
            ARRAY_LENGTH(relation_column_values),
            relation_columns_types,
            data_length,
            data,
            relation_column_values));
  }

  if (insert_error == LOGICAL_RELATION_INSERT_TUPLE_OK)
  {
    ColumnValue relations_values[] = {
        {.integer = result.relation_id},
        {.string = relation_name},
    };
    STATIC_ASSERT(
        ARRAY_LENGTH(relations_values) == ARRAY_LENGTH(relations_types));

    // TODO: Use arena allocator
    size_t data_length = tuple_data_length(
        ARRAY_LENGTH(relations_types), relations_types, relations_values);
    char data[data_length] = {};

    insert_error = insert_tuple(
        pool,
        log,
        RELATIONS_RELATION_ID,
        ARRAY_LENGTH(relations_values),
        relations_types,
        relations_primary_keys,
        tuple_from_data(
            ARRAY_LENGTH(relations_values),
            relations_types,
            data_length,
            data,
            relations_values));
  }

  return LOGICAL_RELATION_CREATE_OK;
}

LogicalRelationDropError logical_relation_drop(
    DiskBufferPool *pool, WriteAheadLog *log, StringSlice relation_name)
{
  assert(pool != NULL);
  assert(relation_name.length > 0);

  QueryRelationIdByNameResult result =
      query_relation_id_by_name(pool, relation_name);
  switch (result.status)
  {
  case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
    break;

  case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
  case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
  case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
    return LOGICAL_RELATION_DROP_IO;

  case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
    return LOGICAL_RELATION_DROP_NOT_FOUND;
  }

  LogicalRelationDeleteTuplesError error = LOGICAL_RELATION_DELETE_TUPLES_OK;
  if (error == LOGICAL_RELATION_DELETE_TUPLES_OK)
  {
    ColumnsLength tuple_length = 1;
    int16_t column_index = 0;
    ColumnType type = relation_columns_types[column_index];
    ColumnValue value = {.integer = result.relation_id};

    // TODO: Use arena allocator
    size_t data_length = tuple_data_length(tuple_length, &type, &value);
    char data[data_length] = {};

    error = delete_tuples(
        pool,
        log,
        RELATION_COLUMNS_RELATION_ID,
        ARRAY_LENGTH(relation_columns_types),
        relation_columns_types,
        relation_columns_primary_keys,
        tuple_from_data(tuple_length, &type, data_length, data, &value));
  }

  if (error == LOGICAL_RELATION_DELETE_TUPLES_OK)
  {
    ColumnsLength tuple_length = 1;
    int16_t column_index = 0;
    ColumnType type = relations_types[column_index];
    ColumnValue value = {.integer = result.relation_id};

    // TODO: Use arena allocator
    size_t data_length = tuple_data_length(tuple_length, &type, &value);
    char data[data_length] = {};

    error = delete_tuples(
        pool,
        log,
        RELATIONS_RELATION_ID,
        ARRAY_LENGTH(relations_types),
        relations_types,
        relations_primary_keys,
        tuple_from_data(tuple_length, &type, data_length, data, &value));
  }

  if (error != LOGICAL_RELATION_DELETE_TUPLES_OK)
  {
    switch (error)
    {
    case LOGICAL_RELATION_DELETE_TUPLES_OK:
      break;

    case LOGICAL_RELATION_DELETE_TUPLES_OUT_OF_MEMORY:
      return LOGICAL_RELATION_DROP_OUT_OF_MEMORY;

    case LOGICAL_RELATION_DELETE_TUPLES_NOT_FOUND:
      return LOGICAL_RELATION_DROP_NOT_FOUND;

    case LOGICAL_RELATION_DELETE_TUPLES_IO:
      return LOGICAL_RELATION_DROP_IO;

    case LOGICAL_RELATION_DELETE_TUPLES_COLUMN_TYPE_MISMATCH:
    case LOGICAL_RELATION_DELETE_TUPLES_PROGRAM_ERROR:
      return LOGICAL_RELATION_DROP_PROGRAM_ERROR;

    case LOGICAL_RELATION_DELETE_TUPLES_BUFFER_POOL_FULL:
      return LOGICAL_RELATION_DROP_BUFFER_POOL_FULL;
    }
  }

  WalWriteResult write_result = wal_write_entry(
      log,
      (WalEntry){
          .header.tag = WAL_ENTRY_DELETE_RELATION_FILE,
          .payload.relation_id = result.relation_id,
      },
      0,
      NULL);

  switch (write_result.error)
  {
  case WAL_WRITE_ENTRY_OK:
    break;

  case WAL_WRITE_ENTRY_PROGRAM_ERROR:
  case WAL_WRITE_ENTRY_TOO_BIG:
  case WAL_WRITE_ENTRY_WRITING_SEGMENT:
  case WAL_WRITE_ENTRY_READING_SEGMENT:
    return LOGICAL_RELATION_DROP_IO;
  }

  switch (disk_buffer_pool_resource_soft_delete(
      pool,
      (DiskResource){
          .type = RESOURCE_TYPE_RELATION,
          .id = result.relation_id,
      }))
  {
  case DISK_RESOURCE_SOFT_DELETE_OK:
    return LOGICAL_RELATION_DROP_OK;

  case DISK_RESOURCE_SOFT_DELETE_NO_MEMORY:
    return LOGICAL_RELATION_DROP_OUT_OF_MEMORY;

  case DISK_RESOURCE_SOFT_DELETE_TEMPORARAY_FAILURE:
  case DISK_RESOURCE_SOFT_DELETE_DENIED:
  case DISK_RESOURCE_SOFT_DELETE_DISK_FULL:
    return LOGICAL_RELATION_DROP_IO;

  case DISK_RESOURCE_SOFT_DELETE_PROGRAM_ERROR:
    return LOGICAL_RELATION_DROP_PROGRAM_ERROR;
  }
}

LogicalRelationInsertTupleError logical_relation_insert_tuple(
    DiskBufferPool *pool,
    WriteAheadLog *log,
    StringSlice relation_name,
    Tuple tuple)
{
  assert(pool != NULL);

  RelationMetadataResult result =
      relation_metadata(pool, relation_name, false, true);

  switch (result.error)
  {
  case RELATION_METADATA_OK:
    break;

  case RELATION_METADATA_OUT_OF_MEMORY:
    return LOGICAL_RELATION_INSERT_TUPLE_OUT_OF_MEMORY;

  case RELATION_METADATA_RELATION_NOT_FOUND:
    return LOGICAL_RELATION_INSERT_TUPLE_RELATION_NOT_FOUND;

  case RELATION_METADATA_IO:
    return LOGICAL_RELATION_INSERT_TUPLE_IO;

  case RELATION_METADATA_PROGRAM_ERROR:
    return LOGICAL_RELATION_INSERT_TUPLE_PROGRAM_ERROR;

  case RELATION_METADATA_BUFFER_POOL_FULL:
    return LOGICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL;
  }

  if (result.relation_id < RESERVED_RELATION_IDS)
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_INSERT_TUPLE_RELATION_NOT_FOUND;
  }

  LogicalRelationInsertTupleError insert_error = insert_tuple(
      pool,
      log,
      result.relation_id,
      result.tuple_length,
      result.types,
      result.primary_keys,
      tuple);

  deallocate_relation_metadata(&result);

  return insert_error;
}

LogicalRelationDeleteTuplesError logical_relation_delete_tuples(
    DiskBufferPool *pool,
    WriteAheadLog *log,
    StringSlice relation_name,
    // TDOO: take column names instead of indices
    const bool32 *compare_column,
    Tuple tuple)
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

  case RELATION_METADATA_PROGRAM_ERROR:
    return LOGICAL_RELATION_DELETE_TUPLES_PROGRAM_ERROR;

  case RELATION_METADATA_BUFFER_POOL_FULL:
    return LOGICAL_RELATION_DELETE_TUPLES_BUFFER_POOL_FULL;
  }

  if (result.relation_id < RESERVED_RELATION_IDS)
  {
    deallocate_relation_metadata(&result);
    return LOGICAL_RELATION_DELETE_TUPLES_NOT_FOUND;
  }

  LogicalRelationDeleteTuplesError error = delete_tuples(
      pool,
      log,
      result.relation_id,
      result.tuple_length,
      result.types,
      compare_column,
      tuple);

  deallocate_relation_metadata(&result);

  return error;
}

LogicalRelationRecoverError logical_recover(
    DiskBufferPool *pool,
    WriteAheadLog *log,
    void *memory,
    size_t memory_length)
{
  WalWriteResult write_result = wal_recover(log, memory, memory_length);
  switch (write_result.error)
  {
  case WAL_WRITE_ENTRY_OK:
    break;

  case WAL_WRITE_ENTRY_PROGRAM_ERROR:
  case WAL_WRITE_ENTRY_TOO_BIG:
    assert(false);
    return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;

  case WAL_WRITE_ENTRY_WRITING_SEGMENT:
  case WAL_WRITE_ENTRY_READING_SEGMENT:
    return LOGICAL_RELATION_RECOVER_IO;
  }

  WalIterator it = wal_iterate(log, memory, memory_length);
  WalIteratorStatus it_status = wal_iterator_open(&it, write_result.lsn);

  if (it_status == WAL_ITERATOR_STATUS_OK)
  {
    // TODO: Algorithm sucks, we should keep a list of LSNs and only redo
    // those, not have to iterate all over the place to check whether this LSN
    // set should be done or not
    while (wal_iterator_previous(&it) == WAL_ITERATOR_STATUS_OK) {}
  }

  for (; it_status == WAL_ITERATOR_STATUS_OK;
       it_status = wal_iterator_next(&it))
  {
    enum
    {
      RECOVER_FOUND_FIRST_ENTRY_SENTINEL,
      RECOVER_FOUND_ABORT,
      RECOVER_FOUND_COMMIT,
      RECOVER_FOUND_UNDO,
      RECOVER_NOT_FOUND,
    } recover_status = RECOVER_NOT_FOUND;

    LogSequenceNumber transaction_lsn = it.current;

    while (recover_status == RECOVER_NOT_FOUND)
    {
      switch (wal_iterator_get(&it)->header.tag)
      {
      case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
        recover_status = RECOVER_FOUND_FIRST_ENTRY_SENTINEL;
        break;

      case WAL_ENTRY_UNDO:
        recover_status = RECOVER_FOUND_UNDO;
        break;

      case WAL_ENTRY_COMMIT:
        recover_status = RECOVER_FOUND_COMMIT;
        break;

      case WAL_ENTRY_ABORT:
        recover_status = RECOVER_FOUND_ABORT;
        break;

      case WAL_ENTRY_START:
        assert(lsn_cmp(it.current, transaction_lsn) == CMP_EQUAL);

      case WAL_ENTRY_CREATE_RELATION_FILE:
      case WAL_ENTRY_DELETE_RELATION_FILE:
      case WAL_ENTRY_INSERT_TUPLE:
      case WAL_ENTRY_DELETE_TUPLE:
        it_status = wal_iterator_next(&it);
        break;
      }
    }

    assert(
        recover_status != RECOVER_NOT_FOUND
        && (it_status == WAL_ITERATOR_STATUS_NO_MORE_ENTRIES
            || it_status == WAL_ITERATOR_STATUS_OK));

    switch (recover_status)
    {
    case RECOVER_NOT_FOUND:
    case RECOVER_FOUND_FIRST_ENTRY_SENTINEL:
      assert(false);
      // TODO return error
      break;

    case RECOVER_FOUND_UNDO:
      logical_relation_undo(pool, &it);
      break;

    case RECOVER_FOUND_ABORT:
      break;

    case RECOVER_FOUND_COMMIT:
    {
      it_status = wal_iterator_open(&it, transaction_lsn);
      assert(wal_iterator_get(&it)->header.tag == WAL_ENTRY_START);

      if (it_status != WAL_ITERATOR_STATUS_OK)
      {
        break;
      }

      it_status = wal_iterator_next(&it);

      for (bool32 loop = true; loop && it_status == WAL_ITERATOR_STATUS_OK;
           it_status = wal_iterator_next(&it))
      {
        WalEntry *entry = wal_iterator_get(&it);
        switch (entry->header.tag)
        {
        case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
        case WAL_ENTRY_ABORT:
        case WAL_ENTRY_UNDO:
        case WAL_ENTRY_START:
          assert(false);
          break;

        case WAL_ENTRY_COMMIT:
          loop = false;
          break;

        case WAL_ENTRY_CREATE_RELATION_FILE:
          switch (disk_buffer_pool_resource_create(
              pool,
              (DiskResource){
                  .type = RESOURCE_TYPE_RELATION,
                  .id = entry->payload.relation_id,
              },
              false))
          {
          case DISK_RESOURCE_CREATE_OK:
            break;

          case DISK_RESOURCE_CREATE_OPENING:
          case DISK_RESOURCE_CREATE_STAT:
          case DISK_RESOURCE_CREATE_TRUNCATING:
          case DISK_RESOURCE_CREATE_CLOSING:
            return LOGICAL_RELATION_RECOVER_IO;

          case DISK_RESOURCE_CREATE_ALREADY_EXISTS:
          case DISK_RESOURCE_CREATE_PROGRAM_ERROR:
            return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;
          }
          break;

        case WAL_ENTRY_DELETE_RELATION_FILE:
          switch (disk_buffer_pool_resource_delete(
              pool,
              (DiskResource){
                  .type = RESOURCE_TYPE_RELATION,
                  .id = entry->payload.relation_id,
              },
              false))
          {
          case DISK_RESOURCE_DELETE_OK:
            break;

          case DISK_RESOURCE_DELETE_DENIED:
          case DISK_RESOURCE_DELETE_TEMPORARAY_FAILURE:
            return LOGICAL_RELATION_RECOVER_IO;

          case DISK_RESOURCE_DELETE_NO_MEMORY:
            return LOGICAL_RELATION_RECOVER_OUT_OF_MEMORY;

          case DISK_RESOURCE_DELETE_PROGRAM_ERROR:
            return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;
          }
          break;

        case WAL_ENTRY_INSERT_TUPLE:
        {
          RelationMetadataResult result = relation_metadata_from_id(
              pool, entry->payload.tuple.relation_id, false, false);

          switch (result.error)
          {
          case RELATION_METADATA_OK:
            break;

          case RELATION_METADATA_RELATION_NOT_FOUND:
          case RELATION_METADATA_PROGRAM_ERROR:
            return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;

          case RELATION_METADATA_BUFFER_POOL_FULL:
            return LOGICAL_RELATION_RECOVER_BUFFER_POOL_FULL;

          case RELATION_METADATA_OUT_OF_MEMORY:
            return LOGICAL_RELATION_RECOVER_OUT_OF_MEMORY;

          case RELATION_METADATA_IO:
            return LOGICAL_RELATION_RECOVER_IO;
          }

          Tuple tuple = wal_iterator_get_tuple(&it, result.types);

          PhysicalRelationIterator prit = physical_relation_iterator(
              pool,
              entry->payload.tuple.relation_id,
              tuple.length,
              tuple.types);

          LogicalRelationRecoverError error = LOGICAL_RELATION_RECOVER_OK;
          switch (physical_relation_iterator_open(
              &prit, entry->payload.tuple.block))
          {
          case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
            break;

          case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
          {
            switch (physical_relation_iterator_new_block(&prit))
            {
            case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
              break;

            case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
              assert(false);
              break;

            case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
              error = LOGICAL_RELATION_RECOVER_BUFFER_POOL_FULL;
              break;

            case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
            case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
              error = LOGICAL_RELATION_RECOVER_IO;
              break;
            }

            assert(
                physical_relation_iterator_block_index(&prit)
                == entry->payload.tuple.block);
          }
          break;

          case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
          case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
            error = LOGICAL_RELATION_RECOVER_IO;
            break;

          case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
            error = LOGICAL_RELATION_RECOVER_BUFFER_POOL_FULL;
            break;
          }

          switch (
              lsn_cmp(physical_relation_iterator_block_lsn(&prit), it.current))
          {
          case CMP_SMALLER:
          {
            assert(physical_relation_iterator_insert_tuple_fits(&prit, tuple));
            physical_relation_iterator_insert(&prit, it.current, tuple);
          }
          break;

          case CMP_EQUAL:
          case CMP_GREATER:
            break;
          }

          physical_relation_iterator_close(&prit);

          deallocate_relation_metadata(&result);

          if (error != LOGICAL_RELATION_RECOVER_OK)
          {
            return error;
          }
        }
        break;

        case WAL_ENTRY_DELETE_TUPLE:
        {
          RelationMetadataResult result = relation_metadata_from_id(
              pool, entry->payload.tuple.relation_id, false, false);

          switch (result.error)
          {
          case RELATION_METADATA_OK:
            break;

          case RELATION_METADATA_RELATION_NOT_FOUND:
          case RELATION_METADATA_PROGRAM_ERROR:
            return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;

          case RELATION_METADATA_BUFFER_POOL_FULL:
            return LOGICAL_RELATION_RECOVER_BUFFER_POOL_FULL;

          case RELATION_METADATA_OUT_OF_MEMORY:
            return LOGICAL_RELATION_RECOVER_OUT_OF_MEMORY;

          case RELATION_METADATA_IO:
            return LOGICAL_RELATION_RECOVER_IO;
          }

          Tuple tuple = wal_iterator_get_tuple(&it, result.types);
          BlockIndex block = entry->payload.tuple.block;

          PhysicalRelationIterator prit = physical_relation_iterator(
              pool,
              entry->payload.tuple.relation_id,
              tuple.length,
              tuple.types);

          PhysicalRelationIteratorStatus prit_status =
              physical_relation_iterator_open(
                  &prit, entry->payload.tuple.block);

          if (prit_status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
          {
            switch (lsn_cmp(
                physical_relation_iterator_block_lsn(&prit), it.current))
            {
            case CMP_SMALLER:
            {
              Tuple stored_tuple = physical_relation_iterator_get(&prit);

              bool32 delete = true;
              for (ColumnsLength i = 0; i < tuple.length && delete; ++i)
              {
                delete = column_value_eq(
                    tuple.types[i],
                    tuple_get(stored_tuple, i),
                    tuple_get(tuple, i));
              }

              if (delete)
              {
                prit_status =
                    physical_relation_iterator_delete(&prit, it.current);
              }
            }
            break;

            case CMP_EQUAL:
            case CMP_GREATER:
              break;
            }
          }

          physical_relation_iterator_close(&prit);

          deallocate_relation_metadata(&result);

          switch (prit_status)
          {
          case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
            break;

          case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
            assert(false);
            return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;

          case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
          case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
            return LOGICAL_RELATION_RECOVER_IO;

          case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
            return LOGICAL_RELATION_RECOVER_BUFFER_POOL_FULL;
          }
        }
        break;
        }
      }
    }
    break;
    }
  }

  switch (it_status)
  {
  case WAL_ITERATOR_STATUS_OK:
    assert(false);
    return LOGICAL_RELATION_RECOVER_PROGRAM_ERROR;

  case WAL_ITERATOR_STATUS_NO_MORE_ENTRIES:
    return LOGICAL_RELATION_RECOVER_OK;

  case WAL_ITERATOR_STATUS_ERROR:
    assert(false);
    return LOGICAL_RELATION_RECOVER_IO;
  }
}

LogicalRelationUndoError
logical_relation_undo(DiskBufferPool *pool, WalIterator *uit)
{
  assert(wal_iterator_get(uit)->header.tag == WAL_ENTRY_UNDO);

  WalEntry *entry = wal_iterator_get(uit);
  WalUndoEntry undo_entry = wal_iterator_get_undo_entry(uit);

  switch (entry->payload.undo.tag)
  {
  case WAL_ENTRY_FIRST_ENTRY_SENTINEL:
  case WAL_ENTRY_START:
  case WAL_ENTRY_COMMIT:
  case WAL_ENTRY_ABORT:
  case WAL_ENTRY_UNDO:
    assert(false);
    return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;

  case WAL_ENTRY_CREATE_RELATION_FILE:
    switch (disk_buffer_pool_resource_delete(
        pool,
        (DiskResource){
            .type = RESOURCE_TYPE_RELATION,
            .id = undo_entry.payload->relation_id,
        },
        false))
    {
    case DISK_RESOURCE_DELETE_OK:
      return LOGICAL_RELATION_UNDO_OK;

    case DISK_RESOURCE_DELETE_PROGRAM_ERROR:
      return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;

    case DISK_RESOURCE_DELETE_DENIED:
    case DISK_RESOURCE_DELETE_NO_MEMORY:
    case DISK_RESOURCE_DELETE_TEMPORARAY_FAILURE:
      return LOGICAL_RELATION_UNDO_IO;
    }
    break;

  case WAL_ENTRY_DELETE_RELATION_FILE:
    switch (disk_buffer_pool_resource_restore(
        pool,
        (DiskResource){
            .type = RESOURCE_TYPE_RELATION,
            .id = undo_entry.payload->relation_id,
        }))
    {
    case DISK_RESOURCE_RESTORE_OK:
      return LOGICAL_RELATION_UNDO_OK;

    case DISK_RESOURCE_RESTORE_NO_MEMORY:
      return LOGICAL_RELATION_UNDO_OUT_OF_MEMORY;

    case DISK_RESOURCE_RESTORE_DENIED:
    case DISK_RESOURCE_RESTORE_TEMPORARAY_FAILURE:
    case DISK_RESOURCE_RESTORE_DISK_FULL:
      return LOGICAL_RELATION_UNDO_IO;

    case DISK_RESOURCE_RESTORE_PROGRAM_ERROR:
      return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;
    }
    break;

  case WAL_ENTRY_INSERT_TUPLE:
  {
    RelationMetadataResult result = relation_metadata_from_id(
        pool, undo_entry.payload->tuple.relation_id, false, false);

    switch (result.error)
    {
    case RELATION_METADATA_OK:
      break;

    case RELATION_METADATA_RELATION_NOT_FOUND:
    case RELATION_METADATA_PROGRAM_ERROR:
      return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;

    case RELATION_METADATA_BUFFER_POOL_FULL:
      return LOGICAL_RELATION_UNDO_BUFFER_POOL_FULL;

    case RELATION_METADATA_OUT_OF_MEMORY:
      return LOGICAL_RELATION_UNDO_OUT_OF_MEMORY;

    case RELATION_METADATA_IO:
      return LOGICAL_RELATION_UNDO_IO;
    }

    Tuple tuple = wal_iterator_get_tuple(uit, result.types);

    PhysicalRelationIterator it = physical_relation_iterator(
        pool, undo_entry.payload->tuple.relation_id, tuple.length, tuple.types);

    PhysicalRelationIteratorStatus iterator_status =
        physical_relation_iterator_open(&it, undo_entry.payload->tuple.block);

    bool32 deleted = false;
    if (iterator_status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
    {
      switch (lsn_cmp(
          physical_relation_iterator_block_lsn(&it), entry->payload.undo.lsn))
      {
      case CMP_SMALLER:
        break;

      case CMP_EQUAL:
      case CMP_GREATER:
      {
        while (iterator_status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
               && !deleted)
        {
          Tuple stored_tuple = physical_relation_iterator_get(&it);

          bool32 delete = true;
          for (ColumnsLength i = 0; i < tuple.length && delete; ++i)
          {
            delete = column_value_eq(
                tuple.types[i],
                tuple_get(stored_tuple, i),
                tuple_get(tuple, i));
          }

          if (delete)
          {
            iterator_status =
                physical_relation_iterator_delete(&it, uit->current);
            deleted = true;
          }
          else
          {
            iterator_status = physical_relation_iterator_next_tuple(&it);
          }
        }
      }
      break;
      }
    }
    physical_relation_iterator_close(&it);

    deallocate_relation_metadata(&result);

    switch (iterator_status)
    {
    case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
      return LOGICAL_RELATION_UNDO_OK;

    case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
      if (deleted)
      {
        return LOGICAL_RELATION_UNDO_OK;
      }
      else
      {
        assert(false);
        return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;
      }

    case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
    case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
      return LOGICAL_RELATION_UNDO_IO;

    case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
      return LOGICAL_RELATION_UNDO_BUFFER_POOL_FULL;
    }
  }
  break;

  case WAL_ENTRY_DELETE_TUPLE:
  {
    RelationMetadataResult result = relation_metadata_from_id(
        pool, undo_entry.payload->tuple.relation_id, false, false);

    switch (result.error)
    {
    case RELATION_METADATA_OK:
      break;

    case RELATION_METADATA_RELATION_NOT_FOUND:
    case RELATION_METADATA_PROGRAM_ERROR:
      return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;

    case RELATION_METADATA_BUFFER_POOL_FULL:
      return LOGICAL_RELATION_UNDO_BUFFER_POOL_FULL;

    case RELATION_METADATA_OUT_OF_MEMORY:
      return LOGICAL_RELATION_UNDO_OUT_OF_MEMORY;

    case RELATION_METADATA_IO:
      return LOGICAL_RELATION_UNDO_IO;
    }

    Tuple tuple = wal_iterator_get_tuple(uit, result.types);

    PhysicalRelationIterator it = physical_relation_iterator(
        pool, undo_entry.payload->tuple.relation_id, tuple.length, tuple.types);

    PhysicalRelationIteratorStatus iterator_status =
        physical_relation_iterator_open(&it, undo_entry.payload->tuple.block);

    if (iterator_status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
    {
      switch (lsn_cmp(
          physical_relation_iterator_block_lsn(&it), entry->payload.undo.lsn))
      {
      case CMP_SMALLER:
        break;

      case CMP_EQUAL:
      case CMP_GREATER:
      {
        assert(physical_relation_iterator_insert_tuple_fits(&it, tuple));
        physical_relation_iterator_insert(&it, uit->current, tuple);
      }
      break;
      }
    }
    physical_relation_iterator_close(&it);

    deallocate_relation_metadata(&result);

    switch (iterator_status)
    {
    case PHYSICAL_RELATION_ITERATOR_STATUS_OK:
      return LOGICAL_RELATION_UNDO_OK;

    case PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS:
      assert(false);
      return LOGICAL_RELATION_UNDO_PROGRAM_ERROR;

    case PHYSICAL_RELATION_ITERATOR_STATUS_LOADING_PAGE:
    case PHYSICAL_RELATION_ITERATOR_STATUS_IO:
      return LOGICAL_RELATION_UNDO_IO;

    case PHYSICAL_RELATION_ITERATOR_STATUS_BUFFER_POOL_FULL:
      return LOGICAL_RELATION_UNDO_BUFFER_POOL_FULL;
    }
  }
  break;
  }
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
  assert(column_id < tuple_iterator_tuple_length(it));

  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return tuple_get(physical_relation_iterator_get(&it->read.it), column_id);

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

internal PhysicalRelationIteratorStatus tuple_iterator_reset(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return physical_relation_iterate_tuples(&it->read.it);

  case QUERY_OPERATOR_PROJECT:
    return tuple_iterator_reset(it->project.it);

  case QUERY_OPERATOR_SELECT:
  {
    PhysicalRelationIteratorStatus status = tuple_iterator_reset(it->select.it);
    if (status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
        && !tuple_iterator_select_tuple(it))
    {
      return tuple_iterator_next(it);
    }

    return status;
  }
  break;

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    PhysicalRelationIteratorStatus status =
        tuple_iterator_reset(it->cartesian_product.rhs);
    if (status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
    {
      status = tuple_iterator_reset(it->cartesian_product.lhs);
    }
    return status;
  }
  break;
  }
}

PhysicalRelationIteratorStatus tuple_iterator_next(TupleIterator *it)
{
  switch (it->operator)
  {
  case QUERY_OPERATOR_READ:
    return physical_relation_iterator_next_tuple(&it->read.it);

  case QUERY_OPERATOR_PROJECT:
    return tuple_iterator_next(it->project.it);

  case QUERY_OPERATOR_SELECT:
  {
    PhysicalRelationIteratorStatus status = tuple_iterator_next(it->select.it);
    for (; status == PHYSICAL_RELATION_ITERATOR_STATUS_OK
           && !tuple_iterator_select_tuple(it);
         status = tuple_iterator_next(it->select.it))
    {
    }
    return status;
  }
  break;

  case QUERY_OPERATOR_CARTESIAN_PRODUCT:
  {
    PhysicalRelationIteratorStatus status =
        tuple_iterator_next(it->cartesian_product.rhs);

    if (status == PHYSICAL_RELATION_ITERATOR_STATUS_NO_MORE_BLOCKS)
    {
      status = tuple_iterator_reset(it->cartesian_product.rhs);
      if (status == PHYSICAL_RELATION_ITERATOR_STATUS_OK)
      {
        status = tuple_iterator_next(it->cartesian_product.lhs);
      }
    }
    return status;
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
                    .it = physical_relation_iterator(
                        pool,
                        result.relation_id,
                        result.tuple_length,
                        result.types),
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

      case RELATION_METADATA_PROGRAM_ERROR:
        status = QUERY_ITERATOR_PROGRAM_ERROR;
        break;

      case RELATION_METADATA_BUFFER_POOL_FULL:
        status = QUERY_ITERATOR_BUFFER_POOL_FULL;
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

  return QUERY_ITERATOR_OK;
}

void query_iterator_destroy(QueryIterator *it)
{
  for (size_t i = 0; i < it->length; ++i)
  {
    tuple_iterator_destroy(&it->iterators[i]);
  }
}

QueryIteratorStartResult query_iterator_start(QueryIterator *query_it)
{
  TupleIterator *it = query_it->iterators + query_it->length - 1;
  return (QueryIteratorStartResult){
      .it = it,
      .status = tuple_iterator_reset(it),
  };
}

// ----- Query -----
