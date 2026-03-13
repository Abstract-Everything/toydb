#ifndef LOGICAL_H

#include "physical.h"
#include "std.h"

// ----- Logical Relation -----

bool32 relation_create_schema_relations(DiskBufferPool *pool);

typedef enum
{
  LOGICAL_RELATION_CREATE_OK,
  LOGICAL_RELATION_CREATE_IO,
  LOGICAL_RELATION_CREATE_NO_PRIMARY_KEY,
  LOGICAL_RELATION_CREATE_ALREADY_EXISTS,
  LOGICAL_RELATION_CREATE_PROGRAM_ERROR,
} LogicalRelationCreateError;

LogicalRelationCreateError logical_relation_create(
    DiskBufferPool *pool,
    StringSlice relation_names,
    StringSlice const *names,
    ColumnType const *types,
    bool32 const *primary_keys,
    ColumnsLength tuple_length);

typedef enum
{
  LOGICAL_RELATION_DROP_OK,
  LOGICAL_RELATION_DROP_OUT_OF_MEMORY,
  LOGICAL_RELATION_DROP_NOT_FOUND,
  LOGICAL_RELATION_DROP_IO,
} LogicalRelationDropError;

LogicalRelationDropError
logical_relation_drop(DiskBufferPool *pool, StringSlice relation_name);

typedef enum
{
  LOGICAL_RELATION_INSERT_TUPLE_OK,
  LOGICAL_RELATION_INSERT_TUPLE_OUT_OF_MEMORY,
  LOGICAL_RELATION_INSERT_TUPLE_NOT_FOUND,
  LOGICAL_RELATION_INSERT_TUPLE_IO,
  LOGICAL_RELATION_INSERT_TUPLE_TUPLE_LENGTH_MISMATCH,
  LOGICAL_RELATION_INSERT_TUPLE_COLUMN_TYPE_MISMATCH,
  LOGICAL_RELATION_INSERT_TUPLE_TOO_BIG,
  LOGICAL_RELATION_INSERT_TUPLE_BUFFER_POOL_FULL,
  LOGICAL_RELATION_INSERT_TUPLE_PRIMARY_KEY_VIOLATION,
} LogicalRelationInsertTupleError;

LogicalRelationInsertTupleError logical_relation_insert_tuple(
    DiskBufferPool *pool, StringSlice relation_name, Tuple tuple);

typedef enum
{
  LOGICAL_RELATION_DELETE_TUPLES_OK,
  LOGICAL_RELATION_DELETE_TUPLES_OUT_OF_MEMORY,
  LOGICAL_RELATION_DELETE_TUPLES_NOT_FOUND,
  LOGICAL_RELATION_DELETE_TUPLES_IO,
  LOGICAL_RELATION_DELETE_TUPLES_TUPLE_LENGTH_MISMATCH,
  LOGICAL_RELATION_DELETE_TUPLES_COLUMN_TYPE_MISMATCH,
} LogicalRelationDeleteTuplesError;

LogicalRelationDeleteTuplesError logical_relation_delete_tuples(
    DiskBufferPool *pool,
    StringSlice relation_name,
    // TDOO: take column name instead of index
    ColumnsLength *column_indices,
    Tuple tuple);

// ----- Logical Relation -----

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

PhysicalRelationIteratorStatus tuple_iterator_valid(TupleIterator *it);

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
  QUERY_ITERATOR_OK,
  QUERY_ITERATOR_OUT_OF_MEMORY,
  QUERY_ITERATOR_RELATION_NOT_FOUND,
  QUERY_ITERATOR_COLUMN_NOT_FOUND,
  QUERY_ITERATOR_OPERATOR_TYPE_MISMATCH,
  QUERY_ITERATOR_READING_DISK,
} QueryIteratorError;

QueryIteratorError query_iterator_new(
    DiskBufferPool *pool,
    size_t length,
    const QueryParameter *parameters,
    QueryIterator *it);

void query_iterator_destroy(QueryIterator *it);

TupleIterator *query_iterator_get_output_iterator(QueryIterator *it);

// ----- Query -----

#define LOGICAL_H
#endif
