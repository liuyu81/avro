# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from avro import schema

from .errors import AvroTypeException, SchemaResolutionException

__all__ = ['read_data', 'write_data', 'validate', ]


INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


__type_to_validator__ = {
    'null': lambda expected_schema, datum: datum is None,
    'boolean': lambda expected_schema, datum: isinstance(datum, bool),
    'string': lambda expected_schema, datum: isinstance(datum, basestring),
    'bytes': lambda expected_schema, datum: isinstance(datum, str),
    'int': lambda expected_schema, datum: (isinstance(datum, (int, long)) and INT_MIN_VALUE <= datum <= INT_MAX_VALUE),
    'long': lambda expected_schema, datum: (isinstance(datum, (int, long)) and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE),
    'float': lambda expected_schema, datum: (isinstance(datum, (int, long)) or isinstance(datum, float)),
    'double': lambda expected_schema, datum: (isinstance(datum, (int, long)) or isinstance(datum, float)),
    'fixed': lambda expected_schema, datum: isinstance(datum, str) and len(datum) == expected_schema.size,
    'enum': lambda expected_schema, datum: datum in expected_schema.symbols,
    'array': lambda expected_schema, datum: (isinstance(datum, (list, tuple)) and all([validate(expected_schema.items, d) for d in datum])),
    'map': lambda expected_schema, datum: (isinstance(datum, dict) and all([isinstance(k, basestring) for k in datum.iterkeys()]) and all([validate(expected_schema.values, v) for v in datum.itervalues()])),
    'union': lambda expected_schema, datum: any([validate(s, datum) for s in expected_schema.schemas]),
    'error_union': lambda expected_schema, datum: any([validate(s, datum) for s in expected_schema.schemas]),
    'record': lambda expected_schema, datum: (isinstance(datum, dict) and all([validate(f.type, datum.get(f.name)) for f in expected_schema.fields])),
    'error': lambda expected_schema, datum: (isinstance(datum, dict) and all([validate(f.type, datum.get(f.name)) for f in expected_schema.fields])),
    'request': lambda expected_schema, datum: (isinstance(datum, dict) and all([validate(f.type, datum.get(f.name)) for f in expected_schema.fields])),
}


def validate(expected_schema, datum):
    """Determine if a python datum is an instance of a schema."""
    global __type_to_validator__
    return __type_to_validator__[expected_schema.type](expected_schema, datum)


def check_props(schema_one, schema_two, prop_list):
    return all([getattr(schema_one, prop) == getattr(schema_two, prop)
                for prop in prop_list])


__same_type_to_match_schema__ = {
    'record': lambda writers_schema, readers_schema: writers_schema.fullname == readers_schema.fullname,
    'error': lambda writers_schema, readers_schema: check_props(writers_schema, readers_schema, ['fullname']),
    'request': lambda writers_schema, readers_schema: True,
    'fixed': lambda writers_schema, readers_schema: check_props(writers_schema, readers_schema, ['fullname', 'size']),
    'enum': lambda writers_schema, readers_schema: check_props(writers_schema, readers_schema, ['fullname']),
    'map': lambda writers_schema, readers_schema: check_props(writers_schema.values, readers_schema.values, ['type']),
    'array': lambda writers_schema, readers_schema: check_props(writers_schema.items, readers_schema.items, ['type']),
    'null': lambda writers_schema, readers_schema: True,
    'boolean': lambda writers_schema, readers_schema: True,
    'string': lambda writers_schema, readers_schema: True,
    'bytes': lambda writers_schema, readers_schema: True,
    'int': lambda writers_schema, readers_schema: True,
    'long': lambda writers_schema, readers_schema: True,
    'float': lambda writers_schema, readers_schema: True,
    'double': lambda writers_schema, readers_schema: True,
    'union': lambda writers_schema, readers_schema: True,
    'error_union': lambda writers_schema, readers_schema: True,
}


__valid_schema_promotion__ = {
    ('int', 'long'): True,
    ('int', 'float'): True,
    ('int', 'double'): True,
    ('long', 'float'): True,
    ('long', 'double'): True,
    ('float', 'double'): True,
}


def __match_schema__(writers_schema, readers_schema):

    w_type = writers_schema.type
    r_type = readers_schema.type
    if w_type == r_type:
        return __same_type_to_match_schema__[w_type](writers_schema, readers_schema)

    _opt_type_tuple = (w_type, r_type)
    if 'union' in _opt_type_tuple or 'error_union' in _opt_type_tuple:
        return True

    global __valid_schema_promotion__
    return _opt_type_tuple in __valid_schema_promotion__


__match_schema_cache__ = {}


MATCH_SCHEMA_CACHE_MAX_LENGTH = 20


def match_schema(writers_schema, readers_schema):
    if writers_schema is readers_schema:
        return True

    global __match_schema_cache__
    global MATCH_SCHEMA_CACHE_MAX_LENGTH

    key = (writers_schema, readers_schema)
    value = None

    try:
        value = __match_schema_cache__[key]
    except KeyError:
        if len(__match_schema_cache__) > MATCH_SCHEMA_CACHE_MAX_LENGTH:
            __match_schema_cache__.clear()
        value = __match_schema__(*key)
        __match_schema_cache__[key] = value
    return value


def skip_data(writers_schema, decoder):
    try:
        return __type_to_skipper__[writers_schema.type](writers_schema, decoder)
    except KeyError:
        fail_msg = "Unknown schema type: %s" % writers_schema.type
        raise schema.AvroException(fail_msg)


def read_fixed(writers_schema, readers_schema, decoder):
    """
    Fixed instances are encoded using the number of bytes declared
    in the schema.
    """
    return decoder.read(writers_schema.size)


def skip_fixed(writers_schema, decoder):
    return decoder.skip(writers_schema.size)


def read_enum(writers_schema, readers_schema, decoder):
    """
    An enum is encoded by a int, representing the zero-based position
    of the symbol in the schema.
    """
    # read data
    index_of_symbol = decoder.read_int()
    if index_of_symbol >= len(writers_schema.symbols):
        fail_msg = "Can't access enum index %d for enum with %d symbols"\
            % (index_of_symbol, len(writers_schema.symbols))
        raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
    read_symbol = writers_schema.symbols[index_of_symbol]

    # schema resolution
    if read_symbol not in readers_schema.symbols:
        fail_msg = "Symbol %s not present in Reader's Schema" % read_symbol
        raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

    return read_symbol


def skip_enum(writers_schema, decoder):
    return decoder.skip_int()


def read_array(writers_schema, readers_schema, decoder):
    """
    Arrays are encoded as a series of blocks.
    
    Each block consists of a long count value,
    followed by that many array items.
    A block with count zero indicates the end of the array.
    Each item is encoded per the array's item schema.
    
    If a block's count is negative,
    then the count is followed immediately by a long block size,
    indicating the number of bytes in the block.
    The actual count in this case
    is the absolute value of the count written.
    """
    read_items = []

    _opt_decoder_read_long = decoder.read_long
    _opt_array_extend = read_items.extend
    _opt_writers_schema_items = writers_schema.items
    _opt_readers_schema_items = readers_schema.items

    block_count = _opt_decoder_read_long()
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            block_size = _opt_decoder_read_long()
        _opt_array_extend([read_data(_opt_writers_schema_items,
                                     _opt_readers_schema_items, decoder)
                           for i in range(block_count)])
        block_count = _opt_decoder_read_long()
    return read_items


def skip_array(writers_schema, decoder):

    _opt_decoder_read_long = decoder.read_long
    _opt_decoder_skip = decoder.skip
    _opt_writers_schema_items = writers_schema.items

    block_count = _opt_decoder_read_long()
    while block_count != 0:
        if block_count < 0:
            block_size = _opt_decoder_read_long()
            _opt_decoder_skip(block_size)
        else:
            for i in range(block_count):
                skip_data(_opt_writers_schema_items, decoder)
        block_count = _opt_decoder_read_long()


def read_map(writers_schema, readers_schema, decoder):
    """
    Maps are encoded as a series of blocks.
    
    Each block consists of a long count value,
    followed by that many key/value pairs.
    A block with count zero indicates the end of the map.
    Each item is encoded per the map's value schema.
    
    If a block's count is negative,
    then the count is followed immediately by a long block size,
    indicating the number of bytes in the block.
    The actual count in this case
    is the absolute value of the count written.
    """
    read_items = {}

    _opt_decoder_read_long = decoder.read_long
    _opt_decoder_read_utf8 = decoder.read_utf8
    _opt_dict_update = read_items.update

    block_count = _opt_decoder_read_long()
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            block_size = _opt_decoder_read_long()
        _opt_dict_update([(_opt_decoder_read_utf8(),
                           read_data(writers_schema.values,
                                     readers_schema.values, decoder))
                          for i in range(block_count)])
        block_count = _opt_decoder_read_long()
    return read_items


def skip_map(writers_schema, decoder):

    _opt_decoder_read_long = decoder.read_long
    _opt_decoder_skip_utf8 = decoder.skip_utf8
    _opt_decoder_skip = decoder.skip

    block_count = _opt_decoder_read_long()
    while block_count != 0:
        if block_count < 0:
            block_size = _opt_decoder_read_long()
            _opt_decoder_skip(block_size)
        else:
            for i in range(block_count):
                _opt_decoder_skip_utf8()
                skip_data(writers_schema.values, decoder)
        block_count = _opt_decoder_read_long()


def read_default_value(field_schema, default_value):
    """
    Basically a JSON Decoder?
    """
    if field_schema.type == 'null':
        return None
    elif field_schema.type == 'boolean':
        return bool(default_value)
    elif field_schema.type == 'int':
        return int(default_value)
    elif field_schema.type == 'long':
        return long(default_value)
    elif field_schema.type in ['float', 'double']:
        return float(default_value)
    elif field_schema.type in ['enum', 'fixed', 'string', 'bytes']:
        return default_value
    elif field_schema.type == 'array':
        return list([read_default_value(field_schema.items, json_val)
                     for json_val in default_value])
    elif field_schema.type == 'map':
        return dict([(key, read_default_value(field_schema.values, json_val))
                     for key, json_val in default_value.iteritems()])
    elif field_schema.type in ['union', 'error_union']:
        return read_default_value(field_schema.schemas[0], default_value)
    elif field_schema.type == 'record':
        read_record = {}
        for field in field_schema.fields:
            json_val = default_value.get(field.name)
            if json_val is None: json_val = field.default
            field_val = read_default_value(field.type, json_val)
            read_record[field.name] = field_val
        return read_record
    else:
        fail_msg = 'Unknown type: %s' % field_schema.type
        raise schema.AvroException(fail_msg)


def read_union(writers_schema, readers_schema, decoder):
    """
    A union is encoded by first writing a long value indicating
    the zero-based position within the union of the schema of its value.
    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index_of_schema = int(decoder.read_long())
    try:
        selected_writers_schema = writers_schema.schemas[index_of_schema]
    except IndexError:
        fail_msg = "Can't access branch index %d for union with %d branches"\
            % (index_of_schema, len(writers_schema.schemas))
        raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
    else:
        return read_data(selected_writers_schema, readers_schema, decoder)


def skip_union(writers_schema, decoder):
    index_of_schema = int(decoder.read_long())
    try:
        selected_writers_schema = writers_schema.schemas[index_of_schema]
    except IndexError:
        fail_msg = "Can't access branch index %d for union with %d branches"\
            % (index_of_schema, len(writers_schema.schemas))
        raise SchemaResolutionException(fail_msg, writers_schema)
    else:
        return skip_data(writers_schema.schemas[index_of_schema], decoder)


def read_record(writers_schema, readers_schema, decoder):
    """
    A record is encoded by encoding the values of its fields
    in the order that they are declared. In other words, a record
    is encoded as just the concatenation of the encodings of its fields.
    Field values are encoded per their schema.
    
    Schema Resolution:
    * the ordering of fields may be different: fields are matched by name.
    * schemas for fields with the same name in both records are resolved
    recursively.
    * if the writer's record contains a field with a name not present in the
    reader's record, the writer's value for that field is ignored.
    * if the reader's record schema has a field that contains a default value,
    and writer's schema does not have a field with the same name, then the
    reader should use the default value from its field.
    * if the reader's record schema has a field with no default value, and
    writer's schema does not have a field with the same name, then the
    field's value is unset.
    """
    if readers_schema is writers_schema:
        return dict((field.name, read_data(field.type, field.type, decoder))
                    for field in writers_schema.fields)

    # schema resolution
    readers_fields_dict = readers_schema.fields_dict
    read_record = {}
    for field in writers_schema.fields:
        try:
            readers_field = readers_fields_dict[field.name]
            field_val = read_data(field.type, readers_field.type, decoder)
            read_record[field.name] = field_val
        except KeyError:
            skip_data(field.type, decoder)

    # fill in default values
    if len(readers_fields_dict) > len(read_record):
        writers_fields_dict = writers_schema.fields_dict
        for field_name, field in readers_fields_dict.iteritems():
            if not writers_fields_dict.has_key(field_name):
                if field.has_default:
                    field_val = read_default_value(field.type, field.default)
                    read_record[field.name] = field_val
                else:
                    fail_msg = 'No default value for field %s' % field_name
                    raise SchemaResolutionException(fail_msg, writers_schema,
                                                    readers_schema)
    return read_record


def skip_record(writers_schema, decoder):
    for field in writers_schema.fields:
        skip_data(field.type, decoder)


__type_to_decoder__ = {
    'null': lambda writers_schema, readers_schema, decoder: decoder.read_null(),
    'boolean': lambda writers_schema, readers_schema, decoder: decoder.read_boolean(),
    'string': lambda writers_schema, readers_schema, decoder: decoder.read_utf8(),
    'int': lambda writers_schema, readers_schema, decoder: decoder.read_int(),
    'long': lambda writers_schema, readers_schema, decoder: decoder.read_long(),
    'float': lambda writers_schema, readers_schema, decoder: decoder.read_float(),
    'double': lambda writers_schema, readers_schema, decoder: decoder.read_double(),
    'bytes': lambda writers_schema, readers_schema, decoder: decoder.read_bytes(),
    'fixed': read_fixed,
    'enum': read_enum,
    'array': read_array,
    'map': read_map,
    'union': read_union,
    'error_union': read_union,
    'record': read_record,
    'error': read_record,
    'request': read_record,
}

__type_to_skipper__ = {
    'null': lambda writers_schema, decoder: decoder.skip_null(),
    'boolean': lambda writers_schema, decoder: decoder.skip_boolean(),
    'string': lambda writers_schema, decoder: decoder.skip_utf8(),
    'int': lambda writers_schema, decoder: decoder.skip_int(),
    'long': lambda writers_schema, decoder: decoder.skip_long(),
    'float': lambda writers_schema, decoder: decoder.skip_float(),
    'double': lambda writers_schema, decoder: decoder.skip_double(),
    'bytes': lambda writers_schema, decoder: decoder.skip_bytes(),
    'fixed': skip_fixed,
    'enum': skip_enum,
    'array': skip_array,
    'map': skip_map,
    'union': skip_union,
    'error_union': skip_union,
    'record': skip_record,
    'error': skip_record,
    'request': skip_record,
}


def read_data(writers_schema, readers_schema, decoder):
    global __type_to_decoder__
    
    if readers_schema is not writers_schema:
        
        # schema matching
        if not match_schema(writers_schema, readers_schema):
            fail_msg = 'Schemas do not match.'
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
        
        # schema resolution: reader's schema is a union, writer's schema is not
        if (writers_schema.type not in ['union', 'error_union']
            and readers_schema.type in ['union', 'error_union']):
            for s in readers_schema.schemas:
                if match_schema(writers_schema, s):
                    return read_data(writers_schema, s, decoder)
            fail_msg = 'Schemas do not match.'
            raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

    # function dispatch for reading data based on type of writer's schema
    try:
        return __type_to_decoder__[writers_schema.type](writers_schema, readers_schema, decoder)
    except KeyError as e:
        fail_msg = "Cannot read unknown schema type: %s" % writers_schema.type
        raise schema.AvroException(fail_msg)


def skip_data(writers_schema, decoder):
    global __type_to_skipper__

    try:
        return __type_to_skipper__[writers_schema.type](writers_schema, decoder)
    except KeyError:
        fail_msg = "Unknown schema type: %s" % writers_schema.type
        raise schema.AvroException(fail_msg)


def write_fixed(writers_schema, datum, encoder):
    """
    Fixed instances are encoded using the number of bytes declared
    in the schema.
    """
    encoder.write(datum)


def write_enum(writers_schema, datum, encoder):
    """
    An enum is encoded by a int, representing the zero-based position
    of the symbol in the schema.
    """
    index_of_datum = writers_schema.symbols.index(datum)
    encoder.write_int(index_of_datum)


def write_array(writers_schema, datum, encoder):
    """
    Arrays are encoded as a series of blocks.
    
    Each block consists of a long count value,
    followed by that many array items.
    A block with count zero indicates the end of the array.
    Each item is encoded per the array's item schema.
    
    If a block's count is negative,
    then the count is followed immediately by a long block size,
    indicating the number of bytes in the block.
    The actual count in this case
    is the absolute value of the count written.
    """
    _opt_writers_schema_items = writers_schema.items

    if len(datum) > 0:
        encoder.write_long(len(datum))
        for item in datum:
            write_data(_opt_writers_schema_items, item, encoder)
    encoder.write_long(0)


def write_map(writers_schema, datum, encoder):
    """
    Maps are encoded as a series of blocks.
    
    Each block consists of a long count value,
    followed by that many key/value pairs.
    A block with count zero indicates the end of the map.
    Each item is encoded per the map's value schema.
    
    If a block's count is negative,
    then the count is followed immediately by a long block size,
    indicating the number of bytes in the block.
    The actual count in this case
    is the absolute value of the count written.
    """

    _opt_encoder_write_utf8 = encoder.write_utf8
    _opt_writers_schema_values = writers_schema.values

    if len(datum) > 0:
        encoder.write_long(len(datum))
        for key, val in datum.iteritems():
            _opt_encoder_write_utf8(key)
            write_data(_opt_writers_schema_values, val, encoder)
    encoder.write_long(0)


def write_union(writers_schema, datum, encoder):
    """
    A union is encoded by first writing a long value indicating
    the zero-based position within the union of the schema of its value.
    The value is then encoded per the indicated schema within the union.
    """

    index_of_schema = -1
    selected_schema = None

    # resolve union
    for i, candidate_schema in enumerate(writers_schema.schemas):
        if validate(candidate_schema, datum):
            index_of_schema = i
            selected_schema = candidate_schema
            break

    if selected_schema is None:
        raise AvroTypeException(writers_schema, datum)

    # write data
    encoder.write_long(index_of_schema)
    write_data(selected_schema, datum, encoder)


def write_record(writers_schema, datum, encoder):
    """
    A record is encoded by encoding the values of its fields
    in the order that they are declared. In other words, a record
    is encoded as just the concatenation of the encodings of its fields.
    Field values are encoded per their schema.
    """
    for field in writers_schema.fields:
        write_data(field.type, datum.get(field.name), encoder)


__datum_to_encoder__ = {
    'null': lambda writers_schema, datum, encoder: encoder.write_null(datum),
    'boolean': lambda writers_schema, datum, encoder: encoder.write_boolean(datum),
    'string': lambda writers_schema, datum, encoder: encoder.write_utf8(datum),
    'int': lambda writers_schema, datum, encoder: encoder.write_int(datum),
    'long': lambda writers_schema, datum, encoder: encoder.write_long(datum),
    'float': lambda writers_schema, datum, encoder: encoder.write_float(datum),
    'double': lambda writers_schema, datum, encoder: encoder.write_double(datum),
    'bytes': lambda writers_schema, datum, encoder: encoder.write_bytes(datum),
    'fixed': write_fixed,
    'enum': write_enum,
    'array': write_array,
    'map': write_map,
    'union': write_union,
    'error_union': write_union,
    'record': write_record,
    'error': write_record,
    'request': write_record,
}


def write_data(writers_schema, datum, encoder):
    global __datum_to_encoder__
    # function dispatch to write datum
    try:
        __datum_to_encoder__[writers_schema.type](writers_schema, datum, encoder)
    except Exception as f:
        fail_msg = 'Unknown type: %s' % writers_schema.type
        raise schema.AvroException(fail_msg)
