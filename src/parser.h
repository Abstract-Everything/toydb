#ifndef PARSER_H
#define PARSER_H

#include "logical.h"
#include "std.h"

typedef struct
{
  size_t length;
  SelectQueryCondition *conditions;
} ParsedWhereCondition;

typedef enum
{
  SQL_PARSE_ERROR_OK,
  SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING,
  SQL_PARSE_ERROR_UNEXPECTED_TOKEN,
  SQL_PARSE_ERROR_EITHER_SELECT_STAR_OR_COLUMN_NAMES,
  SQL_PARSE_ERROR_OUT_OF_MEMORY,
  SQL_PARSE_ERROR_TRAILING_CHARACTERS,
} SqlParseError;

typedef struct
{
  SqlParseError error;
  StringSlice *select_names;
  size_t select_length;
  StringSlice *from_names;
  size_t from_length;
  ParsedWhereCondition *conditions;
  size_t conditions_length;
  QueryParameter *parameters;
  size_t parameters_length;
} SqlParseResult;

SqlParseResult sql_parse_query(StringSlice query);

void deallocate_sql_parse_result(SqlParseResult *result);

#endif
