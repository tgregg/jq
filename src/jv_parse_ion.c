#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ion_types.h>
#include <ion.h>
#include "jv.h"

#define FAIL(x) err = (x); goto fail

jv jv_parse_sized_ion(const char *string, int length) {
  iENTER;
  hREADER reader = NULL;
  ION_TYPE type;
  BOOL is_null;
  ION_READER_OPTIONS options;
  memset(&options, 0, sizeof(ION_READER_OPTIONS));
  decContext decimal_context;
  decContextDefault(&decimal_context, DEC_INIT_DECQUAD);
  jv value = jv_invalid();
  IONCHECK(ion_reader_open_buffer(&reader, (BYTE *)string, length, &options));
  IONCHECK(ion_reader_next(reader, &type));
  if (type == tid_EOF) {
    // Exactly one value is expected.
    FAIL(IERR_UNEXPECTED_EOF);
  }
  if (ion_reader_is_null(reader, &is_null)) {
    value = jv_null();
  } else {
    switch (ION_TYPE_INT(type)) {
      case tid_NULL_INT:
        value = jv_null();
        break;
      case tid_BOOL_INT: {
        int bool_value;
        IONCHECK(ion_reader_read_bool(reader, &bool_value));
        value = jv_bool(bool_value);
        break;
      }
      case tid_INT_INT: {
        int64_t int_value;
        IONCHECK(ion_reader_read_int64(reader, &int_value));
        value = jv_number(int_value);
        break;
      }
      case tid_FLOAT_INT: {
        double float_value;
        IONCHECK(ion_reader_read_double(reader, &float_value));
        value = jv_number(float_value);
        break;
      }
      case tid_DECIMAL_INT: {
        // TODO find a more efficient way to do this.
        decQuad decimal_value;
        char string_representation[34];
        double double_value;
        IONCHECK(ion_reader_read_decimal(reader, &decimal_value));
        decQuadToString(&decimal_value, string_representation);
        double_value = strtod(string_representation, NULL);
        value = jv_number(double_value);
        break;
      }
      case tid_TIMESTAMP_INT: {
        ION_TIMESTAMP timestamp_value;
        char string_representation[ION_TIMESTAMP_STRING_LENGTH];
        IONCHECK(ion_reader_read_timestamp(reader, &timestamp_value));
        IONCHECK(ion_timestamp_to_string(&timestamp_value, string_representation, ION_TIMESTAMP_STRING_LENGTH, NULL, &decimal_context));
        value = jv_string(string_representation);
        break;
      }
      case tid_BLOB_INT:
      case tid_CLOB_INT: {
        int chunk_length;
        BYTE chunk[128];
        value = jv_string("");
        IONCHECK(ion_reader_read_lob_partial_bytes(reader, chunk, 128, &chunk_length));
        while (chunk_length > 0) {
          value = jv_string_append_buf(value, (char *)chunk, chunk_length);
          IONCHECK(ion_reader_read_lob_partial_bytes(reader, chunk, 128, &chunk_length));
        }
        break;
      }
      case tid_STRING_INT:
      case tid_SYMBOL_INT: {
        // TODO this can be collapsed to a function with the lob case, passing in the 'read_partial' functions
        int chunk_length;
        BYTE chunk[128];
        value = jv_string("");
        IONCHECK(ion_reader_read_partial_string(reader, chunk, 128, &chunk_length));
        while (chunk_length > 0) {
          value = jv_string_append_buf(value, (char *)chunk, chunk_length);
          IONCHECK(ion_reader_read_lob_partial_bytes(reader, chunk, 128, &chunk_length));
        }
        break;
      }
      case tid_LIST_INT:
      case tid_SEXP_INT: {

        break;
      }
      case tid_STRUCT_INT: {

        break;
      }
    }
  }
  IONCHECK(ion_reader_next(reader, &type));
  if (type != tid_EOF) {
    // Exactly one value is expected.
    FAIL(IERR_INVALID_ARG);
  }
fail:
  if (reader != NULL) {
    ion_reader_close(reader);
  }
  if (err != IERR_OK) {
    return jv_invalid_with_msg(jv_string(ion_error_to_str(err)));
  }
  return value;
}
