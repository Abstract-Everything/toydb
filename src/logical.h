#ifndef LOGICAL_H

#include "physical.h"
#include "std.h"

struct Database;
typedef struct Database Database;

RelationCreateError
database_new(Database *db, StringSlice path, void *data, size_t length);

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
DatabaseCreateTableError database_create_table(
    Database *db,
    StringSlice name,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength length);

typedef enum
{
  DATABASE_DROP_TABLE_OK,
  DATABASE_DROP_TABLE_OUT_OF_MEMORY,
  DATABASE_DROP_TABLE_NOT_FOUND,
  DATABASE_DROP_TABLE_READING_DISK,
} DatabaseDropTableError;

DatabaseDropTableError database_drop_table(Database *db, StringSlice name);

typedef enum
{
  DATABASE_INSERT_TUPLE_OK,
  DATABASE_INSERT_TUPLE_OUT_OF_MEMORY,
  DATABASE_INSERT_TUPLE_COLUMN_TYPE_MISMATCH,
  DATABASE_INSERT_TUPLE_COLUMNS_LENGTH_MISMATCH,
  DATABASE_INSERT_TUPLE_RELATION_NOT_FOUND,
  DATABASE_INSERT_TUPLE_READING_DISK,
  DATABASE_INSERT_TUPLE_PRIMARY_KEY_VIOLATION,
  DATABASE_INSERT_TUPLE_TOO_BIG,
} DatabaseInsertTupleError;

// TODO: Take relation as argument
DatabaseInsertTupleError database_insert_tuple(
    Database *db,
    StringSlice name,
    const ColumnType *types,
    const ColumnValue *values,
    int16_t length);

typedef enum
{
  DATABASE_DELETE_TUPLES_OK,
  DATABASE_DELETE_TUPLES_OUT_OF_MEMORY,
  DATABASE_DELETE_TUPLES_COLUMN_INDEX_OUT_OF_RANGE,
  DATABASE_DELETE_TUPLES_COLUMN_TYPE_MISMATCH,
  DATABASE_DELETE_TUPLES_RELATION_NOT_FOUND,
  DATABASE_DELETE_TUPLES_READING_DISK,
} DatabaseDeleteTuplesError;

DatabaseDeleteTuplesError database_delete_tuples(
    Database *db,
    StringSlice name,
    // TDOO: take column name instead of index
    ColumnsLength column_index,
    ColumnType type,
    ColumnValue value);

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
  PREDICATE_OPERATOR_GRANULAR_BOOLEAN_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_EQUAL,
  PREDICATE_OPERATOR_GRANULAR_STRING_LIKE,
} PredicateOperatorGranular;

typedef enum
{
  PREDICATE_VARIABLE_TYPE_CONSTANT,
  PREDICATE_VARIABLE_TYPE_COLUMN,
} PredicateVariableType;

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

struct TupleIterator;
typedef struct TupleIterator TupleIterator;

ColumnsLength tuple_iterator_tuple_length(TupleIterator *it);

RelationIteratorStatus tuple_iterator_valid(TupleIterator *it);

StringSlice
tuple_iterator_column_name(TupleIterator *it, ColumnsLength column_id);

ColumnType
tuple_iterator_column_type(TupleIterator *it, ColumnsLength column_id);

ColumnValue
tuple_iterator_column_value(TupleIterator *it, ColumnsLength column_id);

void tuple_iterator_next(TupleIterator *it);

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

DatabaseQueryError database_query(
    Database *db,
    size_t length,
    const QueryParameter *parameters,
    QueryIterator *it);

void query_iterator_destroy(QueryIterator *it);

TupleIterator *query_iterator_get_output_iterator(QueryIterator *it);

// ----- Query -----

#define LOGICAL_H
#endif
