#ifndef PHYSICAL_H

#include "std.h"

#define PAGE_SIZE KIBIBYTES(8)

// Limit number of columns to 2^16
typedef int16_t ColumnsLength;

// ----- Store types -----
typedef int64_t StoreInteger;

typedef struct
{
  int16_t offset;
  int16_t length;
} StoreString;

typedef union
{
  StoreInteger integer;
  StoreString string;
} StoredValue;
// ----- Store types -----

typedef enum
{
  COLUMN_TYPE_INTEGER,
  COLUMN_TYPE_STRING,
} ColumnType;

typedef union
{
  StoreInteger integer;
  StringSlice string;
} ColumnValue;

static size_t column_type_fixed_size(ColumnType type)
{
  StoredValue value;
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
    return sizeof(value.integer);
  case COLUMN_TYPE_STRING:
    return sizeof(value.string);
  }
}

static size_t column_type_variable_size(ColumnType type, ColumnValue value)
{
  switch (type)
  {
  case COLUMN_TYPE_INTEGER:
    return 0;

  case COLUMN_TYPE_STRING:
    return value.string.length;
  }
}

typedef struct
{
  size_t fixed_size;
  size_t variable_size;
} TupleSizes;

static TupleSizes tuple_column_fixed_and_variable_size(
    const ColumnType *types, const ColumnValue *values, int64_t length)
{
  assert(types != NULL);
  assert(values != NULL);

  size_t fixed_size = 0;
  size_t variable_size = 0;
  for (int64_t i = 0; i < length; ++i)
  {
    fixed_size += column_type_fixed_size(types[i]);
    variable_size += column_type_variable_size(types[i], values[i]);
  }
  return (TupleSizes){
      .fixed_size = fixed_size,
      .variable_size = variable_size,
  };
}

static size_t tuple_column_fixed_size(const ColumnType *types, int64_t length)
{
  assert(types != NULL);

  size_t fixed_size = 0;
  for (size_t i = 0; i < length; ++i)
  {
    fixed_size += column_type_fixed_size(types[i]);
  }
  return fixed_size;
}

static size_t tuple_column_fixed_byte_offset(
    const ColumnType *types, int64_t length, ColumnsLength column_index)
{
  assert(column_index < length);

  size_t offset = 0;
  for (size_t i = 0; i < column_index; ++i)
  {
    offset += column_type_fixed_size(types[i]);
  }
  return offset;
}

typedef struct
{
  int16_t allocated_records;
  int16_t variable_data_start;
} BlockHeader;

typedef struct
{
  size_t blocks;
  void *memory;
} MemoryStore;

typedef struct
{
  void *memory;
  BlockHeader *header;
} Block;

static Block memory_store_block(MemoryStore *store, size_t block)
{
  assert(store != NULL);
  void *memory = &store->memory[PAGE_SIZE * block];
  return (Block){
      .memory = memory,
      .header = memory,
  };
}

static void *memory_store_tuple(Block block, size_t fixed_size, size_t index)
{
  return &block.memory[sizeof(BlockHeader) + fixed_size * index];
}

static AllocateError memory_store_new(MemoryStore *store, size_t blocks)
{
  assert(store != NULL);
  assert(blocks > 0);

  void *memory = NULL;
  if (allocate(&memory, blocks * PAGE_SIZE) != ALLOCATE_OK)
  {
    *store = (MemoryStore){
        .blocks = 0,
        .memory = NULL,
    };
    return ALLOCATE_OUT_OF_MEMORY;
  }

  *store = (MemoryStore){
      .blocks = blocks,
      .memory = memory,
  };

  for (size_t block_index = 0; block_index < blocks; ++block_index)
  {
    Block block = memory_store_block(store, block_index);
    *block.header = (BlockHeader){
        .allocated_records = 0,
        .variable_data_start = PAGE_SIZE,
    };
  }

  return ALLOCATE_OK;
}

static void memory_store_destroy(MemoryStore *store)
{
  assert(store != NULL);

  deallocate(store->memory, store->blocks * PAGE_SIZE);
  store->blocks = 0;
  store->memory = NULL;
}

typedef enum
{
  MEMORY_STORE_INSERT_TUPLE_OK,
  MEMORY_STORE_INSERT_TUPLE_NO_SPACE,
} MemoryStoreInsertTupleError;

// TODO: This assumes that the column types do not change between inserts
static MemoryStoreInsertTupleError memory_store_insert_tuple(
    MemoryStore *store,
    const ColumnType *types,
    const ColumnValue *values,
    int64_t length)
{
  assert(store != NULL);
  assert(types != NULL);
  assert(values != NULL);
  assert(length > 0);

  TupleSizes tuple_sizes =
      tuple_column_fixed_and_variable_size(types, values, length);

  for (size_t i = 0; i < store->blocks; ++i)
  {
    Block block = memory_store_block(store, i);
    void *tuple_memory = memory_store_tuple(
        block, tuple_sizes.fixed_size, block.header->allocated_records);

    const size_t free_space =
        block.header->variable_data_start - (tuple_memory - block.memory);
    const size_t required_space =
        tuple_sizes.fixed_size + tuple_sizes.variable_size;
    if (free_space <= required_space)
    {
      continue;
    }

    for (int64_t column = 0; column < length; ++column)
    {
      const size_t column_byte_offset =
          tuple_column_fixed_byte_offset(types, length, column);

      StoredValue *field = &tuple_memory[column_byte_offset];

      switch (types[column])
      {

      case COLUMN_TYPE_INTEGER:
      {
        field->integer = values[column].integer;
      }
      break;

      case COLUMN_TYPE_STRING:
      {
        size_t field_variable_size =
            column_type_variable_size(types[column], values[column]);

        block.header->variable_data_start -= field_variable_size;

        memory_copy_forward(
            &block.memory[block.header->variable_data_start],
            values[column].string.data,
            field_variable_size);

        field->string = (StoreString){
            .length = field_variable_size,
            .offset = block.header->variable_data_start,
        };
      }
      break;
      }
    }

    block.header->allocated_records += 1;

    assert(
        memory_store_tuple(
            block, tuple_sizes.fixed_size, block.header->allocated_records)
        <= block.memory + block.header->variable_data_start);

    return MEMORY_STORE_INSERT_TUPLE_OK;
  }

  return MEMORY_STORE_INSERT_TUPLE_NO_SPACE;
}

static void memory_store_delete_tuples(
    MemoryStore *store,
    const ColumnType *types,
    int64_t length,
    ColumnsLength column_index,
    ColumnValue value)
{
  assert(store != NULL);
  assert(types != NULL);
  assert(length > 0);
  assert(column_index < length);

  size_t fixed_size = tuple_column_fixed_size(types, length);

  const size_t column_byte_offset =
      tuple_column_fixed_byte_offset(types, length, column_index);

  for (size_t i = 0; i < store->blocks; ++i)
  {
    Block block = memory_store_block(store, i);

    size_t block_variable_data_removed = 0;

    for (size_t tuple = 0; tuple < block.header->allocated_records;)
    {
      void *tuple_memory = memory_store_tuple(block, fixed_size, tuple);

      bool32 delete = false;

      StoredValue *field = &tuple_memory[column_byte_offset];
      switch (types[column_index])
      {
      case COLUMN_TYPE_INTEGER:
      {
        delete = field->integer == value.integer;
      }
      break;

      case COLUMN_TYPE_STRING:
      {
        StringSlice slice = (StringSlice){
            .length = field->string.length,
            .data = &block.memory[field->string.offset],
        };
        delete = string_slice_eq(slice, value.string);
      }
      break;
      }

      int16_t block_variable_data_end = 0;
      int16_t block_variable_data_start = 0;
      for (size_t column = 0; column < length; ++column)
      {
        const size_t column_byte_offset =
            tuple_column_fixed_byte_offset(types, length, column);
        StoredValue *field = &tuple_memory[column_byte_offset];

        switch (types[column])
        {
        case COLUMN_TYPE_INTEGER:
          break;

        case COLUMN_TYPE_STRING:
        {
          field->string.offset += block_variable_data_removed;

          if (block_variable_data_end == 0)
          {
            block_variable_data_end =
                field->string.offset + field->string.length;
            block_variable_data_start = block_variable_data_end;
          }

          // Variable data should be contiguous in memory
          assert(
              field->string.offset + field->string.length
              == block_variable_data_start);

          block_variable_data_start = field->string.offset;
        }
        break;
        }
      }

      if (delete)
      {
        memory_copy_forward(
            tuple_memory,
            tuple_memory + fixed_size,
            fixed_size * (block.header->allocated_records - (tuple + 1)));

        if (block_variable_data_end != 0)
        {
          size_t variable_data_remaining =
              block_variable_data_start - block.header->variable_data_start;

          memory_copy_backward(
              &block.memory[block_variable_data_end - 1],
              &block.memory[block_variable_data_start - 1],
              variable_data_remaining);

          size_t variable_data_removed =
              block_variable_data_end - block_variable_data_start;
          block_variable_data_removed += variable_data_removed;
          block.header->variable_data_start += variable_data_removed;
        }

        block.header->allocated_records -= 1;
      }
      else
      {
        tuple++;
      }
    }
  }
}

typedef struct
{
  MemoryStore *store;
  size_t block_index;
  size_t tuple_index;
  bool32 valid;
} TupleIterator;

static TupleIterator memory_store_iterate(MemoryStore *store)
{
  assert(store != NULL);

  for (size_t i = 0; i < store->blocks; ++i)
  {
    Block block = memory_store_block(store, i);
    if (block.header->allocated_records > 0)
    {
      return (TupleIterator){
          .store = store,
          .block_index = i,
          .tuple_index = 0,
          .valid = true,
      };
    }
  }

  return (TupleIterator){
      .store = store,
      .block_index = 0,
      .tuple_index = 0,
      .valid = false,
  };
}

static void tuple_iterator_next(TupleIterator *it)
{
  assert(it != NULL);

  for (; it->block_index < it->store->blocks;
       ++it->block_index, it->tuple_index = 0)
  {
    it->tuple_index += 1;

    Block block = memory_store_block(it->store, it->block_index);
    if (it->tuple_index < block.header->allocated_records)
    {
      return;
    }
  }

  it->valid = false;
}

static ColumnValue tuple_iterator_get(
    TupleIterator *it,
    const ColumnType *types,
    int64_t length,
    int16_t column_index)
{
  assert(it != NULL);
  assert(types != NULL);
  assert(length > 0);
  assert(column_index < length);

  size_t fixed_size = tuple_column_fixed_size(types, length);

  Block block = memory_store_block(it->store, it->block_index);
  void *tuple_memory = memory_store_tuple(block, fixed_size, it->tuple_index);

  const size_t column_byte_offset =
      tuple_column_fixed_byte_offset(types, length, column_index);
  StoredValue *field = &tuple_memory[column_byte_offset];

  switch (types[column_index])
  {
  case COLUMN_TYPE_INTEGER:
  {
    return (ColumnValue){.integer = field->integer};
  }
  break;

  case COLUMN_TYPE_STRING:
  {
    return (ColumnValue){
        .string =
            (StringSlice){
                .length = field->string.length,
                .data = &block.memory[field->string.offset],
            },
    };
  }
  break;
  }
}

#define PHYSICAL_H
#endif
