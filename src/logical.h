#ifndef LOGICAL_H

#include "physical.h"
#include "std.h"

// ----- Relation -----

typedef struct
{
  size_t length;
  ColumnsLength tuple_length;
  MemorySlice *names;
  ColumnType *types;
  ColumnValue2 *values;
  size_t data_length;
  void *data;
} Relation;

void relation_destroy(Relation *relation)
{
  deallocate(relation->data, relation->data_length);

  deallocate(
      relation->values,
      sizeof(relation->values[0]) * relation->tuple_length * relation->length);

  deallocate(
      relation->types, sizeof(*relation->types) * relation->tuple_length);

  deallocate(
      relation->names, sizeof(*relation->names) * relation->tuple_length);
}

AllocateError relation_append_tuple(Relation *relation, ColumnValue2 **tuple)
{
  assert(tuple != NULL);
  assert(*tuple == NULL);

  size_t allocated = relation->tuple_length * relation->length;
  AllocateError error = reallocate(
      sizeof(relation->values[0]),
      (void **)&relation->values,
      allocated,
      relation->tuple_length + allocated);

  if (error != ALLOCATE_OK)
  {
    return error;
  }

  *tuple = relation->values + (relation->tuple_length * relation->length);

  relation->length += 1;

  return ALLOCATE_OK;
}

AllocateError relation_append_string(
    Relation *relation, StringSlice string, MemorySlice *memory_string)
{
  assert(memory_string != NULL);
  *memory_string = (MemorySlice){
      .length = string.length,
      .offset = relation->data_length,
  };

  if (reallocate_update_length(
          sizeof(*string.data),
          &relation->data,
          &relation->data_length,
          relation->data_length + string.length)
      != ALLOCATE_OK)
  {
    return ALLOCATE_OUT_OF_MEMORY;
  }

  memory_copy_forward(
      &relation->data[memory_string->offset], string.data, string.length);

  return ALLOCATE_OK;
}

void relation_update_variable_data(
    Relation *relation, MemorySlice *slice, size_t *offset)
{
  if (*offset < slice->offset)
  {
    memory_copy_forward(
        &relation->data[*offset],
        &relation->data[slice->offset],
        slice->length);
  }
  slice->offset = *offset;
  *offset += slice->length;
}

bool32 relation_get_column_index(
    Relation *relation, StringSlice column_name, ColumnsLength *column_index)
{
  for (ColumnsLength column = 0; column < relation->tuple_length; ++column)
  {
    MemorySlice name = relation->names[column];
    StringSlice slice = (StringSlice){
        .length = name.length,
        .data = &relation->data[name.offset],
    };
    if (string_slice_eq(slice, column_name))
    {
      *column_index = column;
      return true;
    }
  }

  return false;
}

typedef enum
{
  RELATION_PROJECT_OK,
  RELATION_PROJECT_OUT_OF_MEMORY,
  RELATION_PROJECT_COMPLETED_BUT_OUT_OF_MEMORY,
  RELATION_PROJECT_NON_EXISTING_COLUMNS,
} RelationProjectError;

RelationProjectError
relation_project(Relation *relation, const StringSlice *names, size_t length)
{
  bool32 *to_keep = NULL;
  size_t to_keep_length = 0;
  ColumnsLength tuple_length = 0;
  for (ColumnsLength column = 0; column < relation->tuple_length; ++column)
  {
    MemorySlice column_name = relation->names[column];

    if (reallocate_update_length(
            sizeof(*to_keep),
            (void **)&to_keep,
            &to_keep_length,
            to_keep_length + 1)
        != ALLOCATE_OK)
    {
      deallocate(to_keep, to_keep_length);
      return RELATION_PROJECT_OUT_OF_MEMORY;
    }

    StringSlice slice = (StringSlice){
        .length = column_name.length,
        .data = &relation->data[column_name.offset],
    };
    to_keep[to_keep_length - 1] = false;
    if (string_contains(names, length, slice))
    {
      to_keep[to_keep_length - 1] = true;
      tuple_length += 1;
    }
  }

  if (tuple_length != length)
  {
    deallocate(to_keep, to_keep_length);
    return RELATION_PROJECT_NON_EXISTING_COLUMNS;
  }

  if (tuple_length == relation->tuple_length)
  {
    deallocate(to_keep, to_keep_length);
    return RELATION_PROJECT_OK;
  }

  // We assume that a field that comes earlier in memory stores it's variable
  // data earlier in memory, this checks that
  size_t last_offset = 0;
  size_t data_offset = 0;

  // Move the names variable data first before the tuple variable data
  for (ColumnsLength column = 0, save_column = 0;
       column < relation->tuple_length;
       ++column)
  {
    MemorySlice *column_name = &relation->names[column];

    // First two iterations last_offset is 0, first due to initialization
    // and second because the first offset is always zero
    assert(column == 0 || column == 1 || last_offset != 0);
    assert(column < 2 || column_name->offset > last_offset);
    last_offset = column_name->offset;

    if (to_keep[column])
    {
      relation_update_variable_data(relation, column_name, &data_offset);
      relation->names[save_column] = relation->names[column];
      save_column += 1;
    }
    assert(data_offset <= relation->data_length);
  }

  for (size_t tuple = 0; tuple < relation->length; ++tuple)
  {
    for (ColumnsLength column = 0, save_column = 0;
         column < relation->tuple_length;
         ++column)
    {
      if (!to_keep[column])
      {
        continue;
      }

      ColumnValue2 *value =
          &relation->values[(relation->tuple_length * tuple) + column];
      switch (relation->types[column])
      {
      case COLUMN_TYPE_INTEGER:
        break;

      case COLUMN_TYPE_STRING:
        assert(column < 2 || value->string.offset > last_offset);
        last_offset = value->string.offset;
        relation_update_variable_data(relation, &value->string, &data_offset);
        break;
      }

      relation->values[(tuple_length * tuple) + save_column] = *value;
      save_column += 1;
    }
    assert(data_offset <= relation->data_length);
  }

  // Save the types later as we need them in the tuples loop
  for (ColumnsLength column = 0, save_column = 0;
       column < relation->tuple_length;
       ++column)
  {
    if (to_keep[column])
    {
      relation->types[save_column] = relation->types[column];
      save_column += 1;
    }
  }
  deallocate(to_keep, to_keep_length);

  AllocateError reallocate_names = reallocate(
      sizeof(*relation->names),
      (void **)&relation->names,
      relation->tuple_length,
      tuple_length);

  AllocateError reallocate_types = reallocate(
      sizeof(*relation->types),
      (void **)&relation->types,
      relation->tuple_length,
      tuple_length);

  AllocateError reallocate_values = reallocate(
      sizeof(*relation->values),
      (void **)&relation->values,
      relation->tuple_length * relation->length,
      tuple_length * relation->length);

  AllocateError reallocate_data = reallocate_update_length(
      sizeof(*relation->data),
      &relation->data,
      &relation->data_length,
      data_offset);

  relation->tuple_length = tuple_length;

  if (reallocate_names != ALLOCATE_OK || reallocate_types != ALLOCATE_OK
      || reallocate_values != ALLOCATE_OK || reallocate_data != ALLOCATE_OK)
  {
    return RELATION_PROJECT_COMPLETED_BUT_OUT_OF_MEMORY;
  }

  return RELATION_PROJECT_OK;
}

typedef enum
{
  PREDICATE_OPERATOR_GRANULAR_INVALID,
  PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_PREFIX_EQUAL,
} PredicateOperatorGranular;

typedef enum
{
  PREDICATE_OPERATOR_EQUAL,
  PREDICATE_OPERATOR_STRING_PREFIX_EQUAL,
} PredicateOperator;

typedef struct
{
  PredicateOperator operator;
  ColumnValue value;
} Predicate;

typedef enum
{
  RELATION_SELECT_OK,
  RELATION_SELECT_COMPLETED_BUT_OUT_OF_MEMORY,
  RELATION_SELECT_COLUMN_NOT_FOUND,
  RELATION_SELECT_OPERATOR_TYPE_MISMATCH,
} RelationSelectError;

RelationSelectError relation_select(
    Relation *relation, StringSlice column_name, Predicate predicate)
{
  ColumnsLength column_index = 0;
  if (!relation_get_column_index(relation, column_name, &column_index))
  {
    return RELATION_SELECT_COLUMN_NOT_FOUND;
  }

  PredicateOperatorGranular op = PREDICATE_OPERATOR_GRANULAR_INVALID;
  switch (predicate.operator)
  {
  case PREDICATE_OPERATOR_EQUAL:
    switch (relation->types[column_index])
    {
    case COLUMN_TYPE_INTEGER:
      op = PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL;
      break;

    case COLUMN_TYPE_STRING:
      op = PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL;
      break;
    }
    break;

  case PREDICATE_OPERATOR_STRING_PREFIX_EQUAL:
    if (relation->types[column_index] != COLUMN_TYPE_STRING)
    {
      return RELATION_SELECT_OPERATOR_TYPE_MISMATCH;
    }
    op = PREDICATE_OPERATOR_GRANULAR_STRING_PREFIX_EQUAL;
    break;
  }

  size_t length = 0;
  size_t data_offset = 0;
  for (size_t tuple_index = 0; tuple_index < relation->length; ++tuple_index)
  {
    ColumnValue2 *tuple =
        relation->values + (relation->tuple_length * tuple_index);

    ColumnValue2 *value = &tuple[column_index];

    bool32 keep = false;
    switch (op)
    {
    case PREDICATE_OPERATOR_GRANULAR_INVALID:
      assert(false);
      break;

    case PREDICATE_OPERATOR_GRANULAR_INTEGER_EQUAL:
      keep = value->integer == predicate.value.integer;
      break;

    case PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL:
      keep = string_slice_eq(
          (StringSlice){
              .length = value->string.length,
              .data = &relation->data[value->string.offset],
          },
          predicate.value.string);
      break;

    case PREDICATE_OPERATOR_GRANULAR_STRING_PREFIX_EQUAL:
      keep = string_slice_prefix_eq(
          (StringSlice){
              .length = value->string.length,
              .data = &relation->data[value->string.offset],
          },
          predicate.value.string);
      break;
    }

    if (!keep)
    {
      // Find the first field that needs to be overwritten
      if (data_offset == 0)
      {
        for (size_t column_index = 0;
             column_index < relation->tuple_length && data_offset == 0;
             ++column_index)
        {
          switch (relation->types[column_index])
          {
          case COLUMN_TYPE_INTEGER:
            break;

          case COLUMN_TYPE_STRING:
            data_offset = tuple[column_index].string.offset;
            break;
          }
        }
      }

      continue;
    }

    if (length < tuple_index)
    {
      assert(data_offset != 0);

      memory_copy_forward(
          relation->values + (relation->tuple_length * length),
          tuple,
          sizeof(*relation->values) * relation->tuple_length);

      for (size_t column_index = 0;
           column_index < relation->tuple_length && data_offset == 0;
           ++column_index)
      {
        switch (relation->types[column_index])
        {
        case COLUMN_TYPE_INTEGER:
          break;

        case COLUMN_TYPE_STRING:
          relation_update_variable_data(relation, &value->string, &data_offset);
          break;
        }
      }
    }

    length += 1;
  }

  AllocateError reallocate_values = reallocate(
      sizeof(*relation->values),
      (void **)&relation->values,
      relation->tuple_length * relation->length,
      relation->tuple_length * length);

  AllocateError reallocate_data = reallocate_update_length(
      sizeof(*relation->data),
      &relation->data,
      &relation->data_length,
      data_offset);

  if (reallocate_values != ALLOCATE_OK || reallocate_data != ALLOCATE_OK)
  {
    return RELATION_SELECT_COMPLETED_BUT_OUT_OF_MEMORY;
  }

  relation->length = length;

  return RELATION_SELECT_OK;
}
// ----- Relation -----

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

bool32 query_relation_id_by_name(Database db, StringSlice name, int64_t *id)
{
  for (TupleIterator i = memory_store_iterate(&db.relations); i.valid;
       tuple_iterator_next(&i))
  {
    ColumnValue tuple_name = tuple_iterator_get(
        &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 1);
    if (string_slice_eq(name, tuple_name.string))
    {
      ColumnValue tuple_id = tuple_iterator_get(
          &i, relations_column_types, ARRAY_LENGTH(relations_column_types), 0);

      *id = tuple_id.integer;
      return true;
    }
  }
  return false;
}

size_t find_user_relation_index(Database db, int64_t id)
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
  for (TupleIterator i = memory_store_iterate(&db->relations); i.valid;
       tuple_iterator_next(&i))
  {
    ColumnValue value = tuple_iterator_get(
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

static AllocateError database_get_static_relation_column_metadata(
    ColumnsLength tuple_length,
    const char *const *relation_names,
    const ColumnType *relation_types,
    ColumnType **types,
    MemorySlice **names,
    void **data,
    size_t *data_length)
{
  AllocateError status = ALLOCATE_OK;

  status = reallocate(sizeof(*types[0]), (void **)types, 0, tuple_length)
                   == ALLOCATE_OK
               ? status
               : ALLOCATE_OUT_OF_MEMORY;

  status = reallocate(sizeof(*names[0]), (void **)names, 0, tuple_length)
                   == ALLOCATE_OK
               ? status
               : ALLOCATE_OUT_OF_MEMORY;

  for (size_t column_index = 0;
       column_index < tuple_length && status == ALLOCATE_OK;
       ++column_index)
  {
    (*types)[column_index] = relation_types[column_index];

    StringSlice name = string_slice_from_ptr(relation_names[column_index]);

    MemorySlice string = (MemorySlice){
        .length = name.length,
        .offset = *data_length,
    };
    (*names)[column_index] = string;

    if (reallocate_update_length(
            1, data, data_length, *data_length + name.length)
        != ALLOCATE_OK)
    {
      status = ALLOCATE_OUT_OF_MEMORY;
      break;
    }
    memory_copy_forward(&(*data)[string.offset], name.data, name.length);
  }

  if (status != ALLOCATE_OK)
  {
    deallocate(*types, sizeof(**types) * tuple_length);
    deallocate(*names, sizeof(**names) * tuple_length);
    deallocate(*data, sizeof(**data) * *data_length);
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
    MemorySlice **names,
    void **data,
    size_t *data_length)
{
  assert(db != NULL);
  assert(store != NULL);
  assert(*store == NULL);
  assert(tuple_length_ != NULL);
  assert(*tuple_length_ == 0);
  assert(types != NULL || names != NULL);
  assert(
      names == NULL
      || (data != NULL && data_length != NULL && *data_length == 0));
  assert(names != NULL || (data == NULL && data_length == NULL));

  if (string_slice_eq(
          relation_name, string_slice_from_ptr(relations_relation_name)))
  {
    *tuple_length_ = ARRAY_LENGTH(relations_column_types);

    if (database_get_static_relation_column_metadata(
            *tuple_length_,
            relations_column_names,
            relations_column_types,
            types,
            names,
            data,
            data_length)
        != ALLOCATE_OK)
    {
      return DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY;
    }

    *store = &db->relations;
    return DATABASE_GET_RELATION_COLUMN_METADATA_OK;
  }

  if (string_slice_eq(
          relation_name, string_slice_from_ptr(relation_columns_relation_name)))
  {
    *tuple_length_ = ARRAY_LENGTH(relation_columns_column_types);

    if (database_get_static_relation_column_metadata(
            *tuple_length_,
            relation_columns_column_names,
            relation_columns_column_types,
            types,
            names,
            data,
            data_length)
        != ALLOCATE_OK)
    {
      return DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY;
    }

    *store = &db->relation_columns;
    return DATABASE_GET_RELATION_COLUMN_METADATA_OK;
  }

  int64_t relation_id = 0;
  if (!query_relation_id_by_name(*db, relation_name, &relation_id))
  {
    return DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND;
  }

  size_t tuple_length = 0;

  bool32 *initialized_columns = NULL;

  AllocateError status = ALLOCATE_OK;
  for (TupleIterator i = memory_store_iterate(&db->relation_columns);
       i.valid && status == ALLOCATE_OK;
       tuple_iterator_next(&i))
  {
    ColumnValue tuple_relation_id = tuple_iterator_get(
        &i,
        relation_columns_column_types,
        ARRAY_LENGTH(relation_columns_column_types),
        0);

    if (tuple_relation_id.integer != relation_id)
    {
      continue;
    }

    StoreInteger column_index = tuple_iterator_get(
                                    &i,
                                    relation_columns_column_types,
                                    ARRAY_LENGTH(relation_columns_column_types),
                                    1)
                                    .integer;

    size_t old_tuple_length = tuple_length;
    bool32 resize_allocation = column_index >= tuple_length;
    if (resize_allocation
        && reallocate_update_length(
               sizeof(*initialized_columns),
               (void **)&initialized_columns,
               &tuple_length,
               column_index + 1)
               != ALLOCATE_OK)
    {
      status = ALLOCATE_OUT_OF_MEMORY;
      break;
    }

    initialized_columns[column_index] = true;

    if (types != NULL)
    {
      ColumnValue type = tuple_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          3);

      if (resize_allocation)
      {
        if (reallocate(
                sizeof(*types[0]),
                (void **)types,
                old_tuple_length,
                tuple_length)
            != ALLOCATE_OK)
        {
          status = ALLOCATE_OUT_OF_MEMORY;
          break;
        }
      }
      (*types)[column_index] = type.integer;
    }

    if (names != NULL)
    {
      ColumnValue name = tuple_iterator_get(
          &i,
          relation_columns_column_types,
          ARRAY_LENGTH(relation_columns_column_types),
          2);

      if (resize_allocation)
      {
        if (reallocate(
                sizeof(*names[0]),
                (void **)names,
                old_tuple_length,
                tuple_length)
            != ALLOCATE_OK)
        {
          status = ALLOCATE_OUT_OF_MEMORY;
          break;
        }
      }

      MemorySlice string = (MemorySlice){
          .length = name.string.length,
          .offset = *data_length,
      };
      (*names)[column_index] = string;

      if (reallocate_update_length(
              1, data, data_length, *data_length + name.string.length)
          != ALLOCATE_OK)
      {
        status = ALLOCATE_OUT_OF_MEMORY;
        break;
      }
      memory_copy_forward(
          &(*data)[string.offset], name.string.data, name.string.length);
    }
  }

  if (status != ALLOCATE_OK)
  {
    if (types != NULL)
    {
      deallocate(*types, sizeof(**types) * tuple_length);
    }

    if (names != NULL)
    {
      deallocate(*names, sizeof(**names) * tuple_length);
      deallocate(*data, *data_length);
    }

    deallocate(
        initialized_columns, sizeof(*initialized_columns) * tuple_length);

    return DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY;
  }

  for (size_t column = 0; column < tuple_length; ++column)
  {
    assert(initialized_columns[column]);
  }

  deallocate(initialized_columns, sizeof(*initialized_columns) * tuple_length);

  // We don't allow relations without columns to exist
  assert(tuple_length > 0);

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
    MemorySlice **names,
    void **data,
    size_t *data_length)
{
  assert(types != NULL);
  assert(*types == NULL);
  assert(names != NULL);
  assert(*names == NULL);
  assert(data != NULL);
  assert(*data == NULL);

  return database_get_relation_column_metadata_(
      db, relation_name, store, tuple_length, types, names, data, data_length);
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
      db, relation_name, store, tuple_length, types, NULL, NULL, NULL);
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

static DatabaseReadRelationError database_read_relation(
    Database *db, Relation *relation, StringSlice relation_name)
{
  assert(db != NULL);
  assert(relation != NULL);
  assert(relation_name.length > 0);

  *relation = (Relation){
      .length = 0,
      .tuple_length = 0,
      .names = NULL,
      .types = NULL,
      .values = NULL,
      .data = NULL,
      .data_length = 0,
  };

  MemoryStore *store = NULL;
  switch (database_get_relation_column_metadata(
      db,
      relation_name,
      &store,
      &relation->tuple_length,
      &relation->types,
      &relation->names,
      &relation->data,
      &relation->data_length))
  {
  case DATABASE_GET_RELATION_COLUMN_METADATA_OK:
    break;

  case DATABASE_GET_RELATION_COLUMN_METADATA_OUT_OF_MEMORY:
    return DATABASE_READ_RELATION_OUT_OF_MEMORY;

  case DATABASE_GET_RELATION_COLUMN_METADATA_RELATION_NOT_FOUND:
    return DATABASE_READ_RELATION_RELATION_NOT_FOUND;
  }

  AllocateError status = ALLOCATE_OK;
  for (TupleIterator i = memory_store_iterate(store);
       i.valid && status == ALLOCATE_OK;
       tuple_iterator_next(&i))
  {
    ColumnValue2 *tuple = NULL;
    status = relation_append_tuple(relation, &tuple);
    if (status != ALLOCATE_OK)
    {
      status = ALLOCATE_OUT_OF_MEMORY;
      break;
    }

    for (ColumnsLength column = 0; column < relation->tuple_length; ++column)
    {
      ColumnValue field = tuple_iterator_get(
          &i, relation->types, relation->tuple_length, column);

      switch (relation->types[column])
      {
      case COLUMN_TYPE_INTEGER:
        tuple[column].integer = field.integer;
        break;

      case COLUMN_TYPE_STRING:
        status = relation_append_string(
            relation, field.string, &tuple[column].string);
        break;
      }
    }
  }

  if (status != ALLOCATE_OK)
  {
    relation_destroy(relation);
    return DATABASE_READ_RELATION_OUT_OF_MEMORY;
  }

  return DATABASE_READ_RELATION_OK;
}

#define LOGICAL_H
#endif
