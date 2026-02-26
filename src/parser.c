#include "parser.h"
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

internal bool32 is_whitespace(char c)
{
  return c == ' ' || c == '\t' || c == '\n';
}

internal bool32 is_alpha(char c)
{
  return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_'
         || c == '.';
}

internal bool32 is_number(char c)
{
  return '0' <= c && c <= '9';
}

internal TokenData token_next_(const char **const start, const char *const end)
{
  assert(start != NULL);
  assert(*start != NULL);
  assert(end != NULL);

  for (; *start != end && is_whitespace(**start); *start += 1) {}

  if (*start == end)
  {
    return (TokenData){.token = TOKEN_END_OF_FILE};
  }

  const struct
  {
    Token token;
    char character;
  } tokenizable_characters[] = {
      {TOKEN_ASTERISK, '*'},
      {TOKEN_COMMA, ','},
      {TOKEN_SEMICOLON, ';'},
      {TOKEN_EQUAL, '='},
  };

  for (size_t i = 0; i < ARRAY_LENGTH(tokenizable_characters); ++i)
  {
    if (**start == tokenizable_characters[i].character)
    {
      *start += 1;
      return (TokenData){.token = tokenizable_characters[i].token};
    }
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

  const struct
  {
    Token token;
    StringSlice string;
  } tokenizable_keywords[] = {
      {TOKEN_KEYWORD_SELECT, string_slice_from_ptr("SELECT")},
      {TOKEN_KEYWORD_FROM, string_slice_from_ptr("FROM")},
      {TOKEN_KEYWORD_WHERE, string_slice_from_ptr("WHERE")},
      {TOKEN_KEYWORD_LIKE, string_slice_from_ptr("LIKE")},
      {TOKEN_KEYWORD_AND, string_slice_from_ptr("AND")},
      {TOKEN_KEYWORD_OR, string_slice_from_ptr("OR")},
  };

  for (size_t i = 0; i < ARRAY_LENGTH(tokenizable_keywords); ++i)
  {
    if (string_slice_prefix_eq(remaining, tokenizable_keywords[i].string))
    {
      *start += tokenizable_keywords[i].string.length;
      return (TokenData){.token = tokenizable_keywords[i].token};
    }
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

internal TokenData token_next(StringSlice *start)
{
  const char *ptr = start->data;
  TokenData token = token_next_(&ptr, start->data + start->length);
  *start = (StringSlice){
      .data = ptr,
      .length = start->data + start->length - ptr,
  };
  return token;
}

internal TokenData token_peek(StringSlice remaining)
{
  return token_next(&remaining);
}

internal SqlParseError sql_parse_select(
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

internal SqlParseError sql_parse_from(
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

internal SqlParseError sql_parse_predicate_variable(
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

internal SqlParseError sql_parse_where(
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

    PredicateOperator operator = {};
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
      operator = PREDICATE_OPERATOR_EQUAL;
      break;

    case TOKEN_KEYWORD_LIKE:
      operator = PREDICATE_OPERATOR_STRING_LIKE;
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
        .operator = operator,
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

SqlParseResult sql_parse_query(StringSlice query)
{
  SqlParseResult result = {.error = SQL_PARSE_ERROR_OK};

  StringSlice remaining = query;
  SqlParseError status =
      sql_parse_select(&remaining, &result.select_names, &result.select_length);
  if (status != SQL_PARSE_ERROR_OK)
  {
    result.error = status;
    return result;
  }

  status = sql_parse_from(&remaining, &result.from_names, &result.from_length);
  if (status != SQL_PARSE_ERROR_OK)
  {
    result.error = status;
    return result;
  }

  if (token_peek(remaining).token == TOKEN_KEYWORD_WHERE)
  {
    status = sql_parse_where(
        &remaining, &result.conditions, &result.conditions_length);
    if (status != SQL_PARSE_ERROR_OK)
    {
      result.error = status;
      return result;
    }
  }

  if (token_next(&remaining).token != TOKEN_SEMICOLON)
  {
    result.error = SQL_PARSE_ERROR_TRAILING_CHARACTERS;
    return result;
  }

  if (token_peek(remaining).token != TOKEN_END_OF_FILE)
  {
    result.error = SQL_PARSE_ERROR_TRAILING_CHARACTERS;
    return result;
  }

  for (size_t i = 0; i < result.from_length; ++i)
  {
    bool32 add_cartesian_product = i != 0;
    if (reallocate_update_length(
            sizeof(*result.parameters),
            (void **)&result.parameters,
            &result.parameters_length,
            result.parameters_length + (add_cartesian_product ? 2 : 1))
        != ALLOCATE_OK)
    {
      result.error = SQL_PARSE_ERROR_OUT_OF_MEMORY;
      return result;
    }

    result.parameters
        [add_cartesian_product ? result.parameters_length - 2
                               : result.parameters_length - 1] =
        (QueryParameter){
            .operator = QUERY_OPERATOR_READ,
            .read_relation_name = result.from_names[i],
        };

    if (add_cartesian_product)
    {
      result.parameters[result.parameters_length - 1] = (QueryParameter){
          .operator = QUERY_OPERATOR_CARTESIAN_PRODUCT,
          .cartesian_product =
              {
                  .lhs_index = result.parameters_length - 3,
                  .rhs_index = result.parameters_length - 2,
              },
      };
    }
  }

  for (size_t i = 0; i < result.conditions_length; ++i)
  {
    if (reallocate_update_length(
            sizeof(*result.parameters),
            (void **)&result.parameters,
            &result.parameters_length,
            result.parameters_length + 1)
        != ALLOCATE_OK)
    {
      result.error = SQL_PARSE_ERROR_OUT_OF_MEMORY;
      return result;
    }

    result.parameters[result.parameters_length - 1] = (QueryParameter){
        .operator = QUERY_OPERATOR_SELECT,
        .select =
            {
                .query_index = result.parameters_length - 2,
                .length = result.conditions[i].length,
                .conditions = result.conditions[i].conditions,
            },
    };
  }

  if (result.select_names != NULL)
  {
    if (reallocate_update_length(
            sizeof(*result.parameters),
            (void **)&result.parameters,
            &result.parameters_length,
            result.parameters_length + 1)
        != ALLOCATE_OK)
    {
      result.error = SQL_PARSE_ERROR_OUT_OF_MEMORY;
      return result;
    }

    result.parameters[result.parameters_length - 1] = (QueryParameter){
        .operator = QUERY_OPERATOR_PROJECT,
        .project =
            {
                .query_index = result.parameters_length - 2,
                .tuple_length = (ColumnsLength)result.select_length,
                .column_names = result.select_names,
            },
    };
  }

  return result;
}

void deallocate_sql_parse_result(SqlParseResult *result)
{
  deallocate(
      result->select_names,
      sizeof(*result->select_names) * result->select_length);

  deallocate(
      result->from_names, sizeof(*result->from_names) * result->from_length);

  for (size_t i = 0; i < result->conditions_length; ++i)
  {
    deallocate(
        result->conditions[i].conditions,
        sizeof(*result->conditions) * result->conditions[i].length);
  }

  deallocate(
      result->conditions,
      sizeof(*result->conditions) * result->conditions_length);

  deallocate(
      result->parameters,
      sizeof(*result->parameters) * result->parameters_length);
}
