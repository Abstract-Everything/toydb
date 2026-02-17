#include "logical.h"
#include "std.h"

typedef enum
{
  TOKEN_END_OF_FILE,
  TOKEN_INVALID,
  TOKEN_ASTERISK,
  TOKEN_COMMA,
  TOKEN_SEMICOLON,
  TOKEN_EQUAL,
  TOKEN_IDENTIFIER,
  TOKEN_STRING,
  TOKEN_NUMBER,
  TOKEN_KEYWORD_SELECT,
  TOKEN_KEYWORD_FROM,
  TOKEN_KEYWORD_WHERE,
  TOKEN_KEYWORD_LIKE,
  TOKEN_KEYWORD_AND,
  TOKEN_KEYWORD_OR,
} Token;

typedef struct
{
  Token token;
  union
  {
    StringSlice string;
    StoreInteger integer;
  };
} TokenData;

bool32 is_whitespace(char c)
{
  return c == ' ' || c == '\t' || c == '\n';
}

bool32 is_alpha(char c)
{
  return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_'
         || c == '.';
}

bool32 is_number(char c)
{
  return '0' <= c && c <= '9';
}

TokenData token_next_(const char **const start, const char *const end)
{
  assert(start != NULL);
  assert(*start != NULL);
  assert(end != NULL);

  for (; *start != end && is_whitespace(**start); *start += 1) {}

  if (*start == end)
  {
    return (TokenData){.token = TOKEN_END_OF_FILE};
  }

  if (**start == '*')
  {
    *start += 1;
    return (TokenData){.token = TOKEN_ASTERISK};
  }

  if (**start == ',')
  {
    *start += 1;
    return (TokenData){.token = TOKEN_COMMA};
  }

  if (**start == ';')
  {
    *start += 1;
    return (TokenData){.token = TOKEN_SEMICOLON};
  }

  if (**start == '=')
  {
    *start += 1;
    return (TokenData){.token = TOKEN_EQUAL};
  }

  if (is_number(**start))
  {
    int64_t integer = 0;
    for (; *start != end && is_number(**start); *start += 1)
    {
      int64_t value = **start - '0';

      const int64_t MAX_INT_64 = 0x7fffffffffffffff;
      const int64_t BASE = 10;
      if (integer > (MAX_INT_64 / BASE)
          || value > MAX_INT_64 - (BASE * integer))
      {
        return (TokenData){.token = TOKEN_INVALID};
      }

      integer = (BASE * integer) + value;
    }
    return (TokenData){
        .token = TOKEN_NUMBER,
        .integer = integer,
    };
  }

  if (**start == '\'')
  {
    *start += 1;
    const char *string_start = *start;
    for (bool32 escape = false; *start != end && (**start != '\'' || escape);
         *start += 1)
    {
      escape = !escape && **start == '\\';
    }

    if (**start != '\'')
    {
      return (TokenData){.token = TOKEN_INVALID};
    }
    *start += 1;

    return (TokenData){
        .token = TOKEN_STRING,
        .string =
            (StringSlice){
                .data = string_start,
                .length = *start - string_start,
            },
    };
  }

  if (!is_alpha(**start))
  {
    return (TokenData){.token = TOKEN_INVALID};
  }

  StringSlice remaining = (StringSlice){
      .data = *start,
      .length = end - *start,
  };

  StringSlice SELECT = string_slice_from_ptr("SELECT");
  if (string_slice_prefix_eq(remaining, SELECT))
  {
    *start += SELECT.length;
    return (TokenData){.token = TOKEN_KEYWORD_SELECT};
  }

  StringSlice FROM = string_slice_from_ptr("FROM");
  if (string_slice_prefix_eq(remaining, FROM))
  {
    *start += FROM.length;
    return (TokenData){.token = TOKEN_KEYWORD_FROM};
  }

  StringSlice WHERE = string_slice_from_ptr("WHERE");
  if (string_slice_prefix_eq(remaining, WHERE))
  {
    *start += WHERE.length;
    return (TokenData){.token = TOKEN_KEYWORD_WHERE};
  }

  StringSlice LIKE = string_slice_from_ptr("LIKE");
  if (string_slice_prefix_eq(remaining, LIKE))
  {
    *start += LIKE.length;
    return (TokenData){.token = TOKEN_KEYWORD_LIKE};
  }

  StringSlice AND = string_slice_from_ptr("AND");
  if (string_slice_prefix_eq(remaining, AND))
  {
    *start += AND.length;
    return (TokenData){.token = TOKEN_KEYWORD_AND};
  }

  StringSlice OR = string_slice_from_ptr("OR");
  if (string_slice_prefix_eq(remaining, OR))
  {
    *start += OR.length;
    return (TokenData){.token = TOKEN_KEYWORD_OR};
  }

  const char *identifier_start = *start;
  for (; *start != end && is_alpha(**start); *start += 1) {}

  return (TokenData){
      .token = TOKEN_IDENTIFIER,
      .string =
          (StringSlice){
              .data = identifier_start,
              .length = *start - identifier_start,
          },
  };
}

TokenData token_next(StringSlice *start)
{
  const char *ptr = start->data;
  TokenData token = token_next_(&ptr, start->data + start->length);
  *start = (StringSlice){
      .data = ptr,
      .length = start->data + start->length - ptr,
  };
  return token;
}

TokenData token_peek(StringSlice remaining)
{
  return token_next(&remaining);
}

typedef enum
{
  SQL_PARSE_ERROR_OK,
  SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING,
  SQL_PARSE_ERROR_UNEXPECTED_TOKEN,
  SQL_PARSE_ERROR_EITHER_SELECT_STAR_OR_COLUMN_NAMES,
  SQL_PARSE_ERROR_OUT_OF_MEMORY,
  SQL_PARSE_ERROR_TRAILING_CHARACTERS,
} SqlParseError;

SqlParseError sql_parse_select(
    StringSlice *remaining, StringSlice **select_names, size_t *select_length)
{
  assert(remaining != NULL);
  assert(select_names != NULL);
  assert(select_length != NULL);

  switch (token_next(remaining).token)
  {
  case TOKEN_END_OF_FILE:
    return SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;

  case TOKEN_KEYWORD_FROM:
  case TOKEN_KEYWORD_LIKE:
  case TOKEN_KEYWORD_WHERE:
  case TOKEN_KEYWORD_AND:
  case TOKEN_KEYWORD_OR:
  case TOKEN_IDENTIFIER:
  case TOKEN_STRING:
  case TOKEN_NUMBER:
  case TOKEN_ASTERISK:
  case TOKEN_COMMA:
  case TOKEN_EQUAL:
  case TOKEN_SEMICOLON:
  case TOKEN_INVALID:
    return SQL_PARSE_ERROR_UNEXPECTED_TOKEN;

  case TOKEN_KEYWORD_SELECT:
    break;
  }

  if (token_peek(*remaining).token == TOKEN_ASTERISK)
  {
    token_next(remaining);
    return SQL_PARSE_ERROR_OK;
  }

  for (TokenData data = token_next(remaining);; data = token_next(remaining))
  {
    switch (data.token)
    {
    case TOKEN_END_OF_FILE:
      return SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;

    case TOKEN_KEYWORD_SELECT:
    case TOKEN_KEYWORD_FROM:
    case TOKEN_KEYWORD_WHERE:
    case TOKEN_KEYWORD_LIKE:
    case TOKEN_KEYWORD_AND:
    case TOKEN_KEYWORD_OR:
    case TOKEN_STRING:
    case TOKEN_NUMBER:
    case TOKEN_COMMA:
    case TOKEN_EQUAL:
    case TOKEN_SEMICOLON:
    case TOKEN_INVALID:
      return SQL_PARSE_ERROR_UNEXPECTED_TOKEN;

    case TOKEN_ASTERISK:
      return SQL_PARSE_ERROR_EITHER_SELECT_STAR_OR_COLUMN_NAMES;

    case TOKEN_IDENTIFIER:
      if (reallocate_update_length(
              sizeof(**select_names),
              (void **)select_names,
              select_length,
              *select_length + 1)
          != ALLOCATE_OK)
      {
        return SQL_PARSE_ERROR_OUT_OF_MEMORY;
      }
      (*select_names)[*select_length - 1] = data.string;

      if (token_peek(*remaining).token != TOKEN_COMMA)
      {
        return SQL_PARSE_ERROR_OK;
      }

      // Consume Comma
      token_next(remaining);
      break;
    }
  }

  return SQL_PARSE_ERROR_OK;
}

SqlParseError sql_parse_from(
    StringSlice *remaining, StringSlice **from_names, size_t *from_length)
{
  assert(remaining != NULL);
  assert(from_names != NULL);
  assert(from_length != NULL);

  switch (token_next(remaining).token)
  {
  case TOKEN_END_OF_FILE:
    return SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;

  case TOKEN_KEYWORD_SELECT:
  case TOKEN_KEYWORD_WHERE:
  case TOKEN_KEYWORD_LIKE:
  case TOKEN_KEYWORD_AND:
  case TOKEN_KEYWORD_OR:
  case TOKEN_IDENTIFIER:
  case TOKEN_STRING:
  case TOKEN_NUMBER:
  case TOKEN_EQUAL:
  case TOKEN_ASTERISK:
  case TOKEN_COMMA:
  case TOKEN_SEMICOLON:
  case TOKEN_INVALID:
    return SQL_PARSE_ERROR_UNEXPECTED_TOKEN;

  case TOKEN_KEYWORD_FROM:
    break;
  }

  for (TokenData data = token_next(remaining);; data = token_next(remaining))
  {
    switch (data.token)
    {
    case TOKEN_END_OF_FILE:
      return SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;

    case TOKEN_KEYWORD_SELECT:
    case TOKEN_KEYWORD_FROM:
    case TOKEN_KEYWORD_WHERE:
    case TOKEN_KEYWORD_LIKE:
    case TOKEN_KEYWORD_AND:
    case TOKEN_KEYWORD_OR:
    case TOKEN_STRING:
    case TOKEN_NUMBER:
    case TOKEN_EQUAL:
    case TOKEN_COMMA:
    case TOKEN_ASTERISK:
    case TOKEN_SEMICOLON:
    case TOKEN_INVALID:
      return SQL_PARSE_ERROR_UNEXPECTED_TOKEN;

    case TOKEN_IDENTIFIER:
      if (reallocate_update_length(
              sizeof(**from_names),
              (void **)from_names,
              from_length,
              *from_length + 1)
          != ALLOCATE_OK)
      {
        return SQL_PARSE_ERROR_OUT_OF_MEMORY;
      }
      (*from_names)[*from_length - 1] = data.string;

      if (token_peek(*remaining).token != TOKEN_COMMA)
      {
        return SQL_PARSE_ERROR_OK;
      }

      // Consume Comma
      token_next(remaining);
      break;
    }
  }

  return SQL_PARSE_ERROR_OK;
}

SqlParseError sql_parse_predicate_variable(
    StringSlice *remaining, SelectQueryParameterVariable *variable)
{
  assert(remaining != NULL);
  assert(variable != NULL);

  TokenData data = token_next(remaining);
  switch (data.token)
  {
  case TOKEN_END_OF_FILE:
    return SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;

  case TOKEN_KEYWORD_SELECT:
  case TOKEN_KEYWORD_FROM:
  case TOKEN_KEYWORD_WHERE:
  case TOKEN_KEYWORD_LIKE:
  case TOKEN_KEYWORD_AND:
  case TOKEN_KEYWORD_OR:
  case TOKEN_EQUAL:
  case TOKEN_COMMA:
  case TOKEN_ASTERISK:
  case TOKEN_SEMICOLON:
  case TOKEN_INVALID:
    return SQL_PARSE_ERROR_UNEXPECTED_TOKEN;

  case TOKEN_STRING:
    *variable = (SelectQueryParameterVariable){
        .type = PREDICATE_VARIABLE_TYPE_CONSTANT,
        .constant =
            {
                .type = COLUMN_TYPE_STRING,
                .value.string = data.string,
            },
    };
    break;

  case TOKEN_NUMBER:
    *variable = (SelectQueryParameterVariable){
        .type = PREDICATE_VARIABLE_TYPE_CONSTANT,
        .constant =
            {
                .type = COLUMN_TYPE_INTEGER,
                .value.string = data.integer,
            },
    };
    break;

  case TOKEN_IDENTIFIER:
    *variable = (SelectQueryParameterVariable){
        .type = PREDICATE_VARIABLE_TYPE_COLUMN,
        .column_name = data.string,
    };
    break;
  }

  return SQL_PARSE_ERROR_OK;
}

typedef struct
{
  size_t length;
  SelectQueryCondition *conditions;
} ParsedWhereCondition;

SqlParseError sql_parse_where(
    StringSlice *remaining,
    ParsedWhereCondition **where_conditions,
    size_t *where_length)
{
  assert(remaining != NULL);
  assert(where_conditions != NULL);
  assert(*where_conditions == NULL);
  assert(where_length != NULL);
  assert(*where_length == 0);

  switch (token_next(remaining).token)
  {
  case TOKEN_END_OF_FILE:
    return SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;

  case TOKEN_KEYWORD_SELECT:
  case TOKEN_KEYWORD_FROM:
  case TOKEN_KEYWORD_LIKE:
  case TOKEN_KEYWORD_AND:
  case TOKEN_KEYWORD_OR:
  case TOKEN_IDENTIFIER:
  case TOKEN_STRING:
  case TOKEN_NUMBER:
  case TOKEN_EQUAL:
  case TOKEN_ASTERISK:
  case TOKEN_COMMA:
  case TOKEN_SEMICOLON:
  case TOKEN_INVALID:
    return SQL_PARSE_ERROR_UNEXPECTED_TOKEN;

  case TOKEN_KEYWORD_WHERE:
    break;
  }

  SqlParseError error = SQL_PARSE_ERROR_OK;
  SelectQueryCondition *conditions = NULL;
  size_t length = 0;
  for (; error == SQL_PARSE_ERROR_OK;)
  {
    SelectQueryParameterVariable lhs = {};
    error = sql_parse_predicate_variable(remaining, &lhs);
    if (error != SQL_PARSE_ERROR_OK)
    {
      break;
    }

    PredicateOperator operator= {};
    switch (token_next(remaining).token)
    {
    case TOKEN_END_OF_FILE:
      error = SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;
      break;

    case TOKEN_KEYWORD_SELECT:
    case TOKEN_KEYWORD_FROM:
    case TOKEN_KEYWORD_WHERE:
    case TOKEN_KEYWORD_AND:
    case TOKEN_KEYWORD_OR:
    case TOKEN_ASTERISK:
    case TOKEN_IDENTIFIER:
    case TOKEN_STRING:
    case TOKEN_NUMBER:
    case TOKEN_COMMA:
    case TOKEN_SEMICOLON:
    case TOKEN_INVALID:
      error = SQL_PARSE_ERROR_UNEXPECTED_TOKEN;
      break;

    case TOKEN_EQUAL:
      operator= PREDICATE_OPERATOR_EQUAL;
      break;

    case TOKEN_KEYWORD_LIKE:
      operator= PREDICATE_OPERATOR_STRING_LIKE;
      break;
    }
    if (error != SQL_PARSE_ERROR_OK)
    {
      break;
    }

    SelectQueryParameterVariable rhs = {};
    error = sql_parse_predicate_variable(remaining, &rhs);
    if (error != SQL_PARSE_ERROR_OK)
    {
      break;
    }

    if (reallocate_update_length(
            sizeof(*conditions), (void **)&conditions, &length, length + 1)
        != ALLOCATE_OK)
    {
      error = SQL_PARSE_ERROR_OUT_OF_MEMORY;
      break;
    }

    conditions[length - 1] = (SelectQueryCondition){
        .operator= operator,
        .lhs = lhs,
        .rhs = rhs,
    };

    bool32 stop = false;
    TokenData data = token_peek(*remaining);
    if (data.token == TOKEN_SEMICOLON)
    {
      stop = true;
    }
    else
    {
      token_next(remaining);
    }

    switch (data.token)
    {
    case TOKEN_END_OF_FILE:
      error = SQL_PARSE_ERROR_UNEXPECTED_END_OF_STRING;
      break;

    case TOKEN_KEYWORD_SELECT:
    case TOKEN_KEYWORD_FROM:
    case TOKEN_KEYWORD_WHERE:
    case TOKEN_ASTERISK:
    case TOKEN_IDENTIFIER:
    case TOKEN_STRING:
    case TOKEN_NUMBER:
    case TOKEN_COMMA:
    case TOKEN_INVALID:
    case TOKEN_EQUAL:
    case TOKEN_KEYWORD_LIKE:
      error = SQL_PARSE_ERROR_UNEXPECTED_TOKEN;
      break;

    case TOKEN_KEYWORD_OR:
      break;

    case TOKEN_SEMICOLON:
    case TOKEN_KEYWORD_AND:
      if (reallocate_update_length(
              sizeof(**where_conditions),
              (void **)where_conditions,
              where_length,
              *where_length + 1)
          != ALLOCATE_OK)
      {
        error = SQL_PARSE_ERROR_OUT_OF_MEMORY;
        break;
      }

      (*where_conditions)[*where_length - 1] = (ParsedWhereCondition){
          .length = length,
          .conditions = conditions,
      };

      conditions = NULL;
      length = 0;

      break;
    }

    if (stop)
    {
      break;
    }
  }

  if (error != SQL_PARSE_ERROR_OK)
  {
    deallocate(conditions, sizeof(*conditions) * (length + 1));
    return error;
  }

  return SQL_PARSE_ERROR_OK;
}

SqlParseError sql_parse_query(
    StringSlice query,
    StringSlice **const select_names,
    size_t *const select_length,
    StringSlice **const from_names,
    size_t *const from_length,
    ParsedWhereCondition **conditions,
    size_t *conditions_length,
    QueryParameter **parameters,
    size_t *parameters_length)
{
  assert(parameters_length != NULL);
  assert(parameters != NULL);
  assert(select_names != NULL);
  assert(*select_names == NULL);
  assert(select_length != NULL);
  assert(*select_length == 0);
  assert(from_names != NULL);
  assert(*from_names == NULL);
  assert(from_length != NULL);
  assert(*from_length == 0);
  assert(conditions != NULL);
  assert(*conditions == NULL);
  assert(conditions_length != NULL);
  assert(*conditions_length == 0);

  StringSlice remaining = query;
  SqlParseError status =
      sql_parse_select(&remaining, select_names, select_length);
  if (status != SQL_PARSE_ERROR_OK)
  {
    return status;
  }

  status = sql_parse_from(&remaining, from_names, from_length);
  if (status != SQL_PARSE_ERROR_OK)
  {
    return status;
  }

  if (token_peek(remaining).token == TOKEN_KEYWORD_WHERE)
  {
    status = sql_parse_where(&remaining, conditions, conditions_length);
    if (status != SQL_PARSE_ERROR_OK)
    {
      return status;
    }
  }

  if (token_next(&remaining).token != TOKEN_SEMICOLON)
  {
    return SQL_PARSE_ERROR_TRAILING_CHARACTERS;
  }

  if (token_peek(remaining).token != TOKEN_END_OF_FILE)
  {
    return SQL_PARSE_ERROR_TRAILING_CHARACTERS;
  }

  for (size_t i = 0; i < *from_length; ++i)
  {
    bool32 add_cartesian_product = i != 0;
    if (reallocate_update_length(
            sizeof(**parameters),
            (void **)parameters,
            parameters_length,
            *parameters_length + (add_cartesian_product ? 2 : 1))
        != ALLOCATE_OK)
    {
      return SQL_PARSE_ERROR_OUT_OF_MEMORY;
    }

    (*parameters)
        [add_cartesian_product ? *parameters_length - 2
                               : *parameters_length - 1] = (QueryParameter){
            .operator= QUERY_OPERATOR_READ,
            .read_relation_name = (*from_names)[i],
        };

    if (add_cartesian_product)
    {
      (*parameters)[*parameters_length - 1] = (QueryParameter){
          .operator= QUERY_OPERATOR_CARTESIAN_PRODUCT,
          .cartesian_product =
              {
                  .lhs_index = *parameters_length - 3,
                  .rhs_index = *parameters_length - 2,
              },
      };
    }
  }

  for (size_t i = 0; i < *conditions_length; ++i)
  {
    if (reallocate_update_length(
            sizeof(**parameters),
            (void **)parameters,
            parameters_length,
            *parameters_length + 1)
        != ALLOCATE_OK)
    {
      return SQL_PARSE_ERROR_OUT_OF_MEMORY;
    }

    (*parameters)[*parameters_length - 1] = (QueryParameter){
        .operator= QUERY_OPERATOR_SELECT,
        .select =
            {
                .query_index = *parameters_length - 2,
                .length = (*conditions)[i].length,
                .conditions = (*conditions)[i].conditions,
            },
    };
  }

  if (*select_names != NULL)
  {
    if (reallocate_update_length(
            sizeof(**parameters),
            (void **)parameters,
            parameters_length,
            *parameters_length + 1)
        != ALLOCATE_OK)
    {
      return SQL_PARSE_ERROR_OUT_OF_MEMORY;
    }

    (*parameters)[*parameters_length - 1] = (QueryParameter){
        .operator= QUERY_OPERATOR_PROJECT,
        .project =
            {
                .query_index = *parameters_length - 2,
                .tuple_length = (ColumnsLength)*select_length,
                .column_names = *select_names,
            },
    };
  }

  return SQL_PARSE_ERROR_OK;
}
