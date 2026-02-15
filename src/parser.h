#include "logical.h"
#include "std.h"

typedef enum
{
  TOKEN_END_OF_FILE,
  TOKEN_INVALID,
  TOKEN_ASTERISK,
  TOKEN_COMMA,
  TOKEN_SEMICOLON,
  TOKEN_IDENTIFIER,
  TOKEN_KEYWORD_SELECT,
  TOKEN_KEYWORD_FROM,
} Token;

typedef struct
{
  Token token;
  union
  {
    StringSlice string;
  };
} TokenData;

bool32 is_whitespace(char c)
{
  return c == ' ' || c == '\t' || c == '\n';
}

bool32 is_alpha(char c)
{
  return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_';
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
  case TOKEN_ASTERISK:
  case TOKEN_IDENTIFIER:
  case TOKEN_COMMA:
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
    case TOKEN_COMMA:
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
  case TOKEN_ASTERISK:
  case TOKEN_IDENTIFIER:
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

SqlParseError sql_parse_query(
    StringSlice query,
    StringSlice **const select_names,
    size_t *const select_length,
    StringSlice **const from_names,
    size_t *const from_length,
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
