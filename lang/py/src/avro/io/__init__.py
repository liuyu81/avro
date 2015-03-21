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
"""
Input/Output utilities, including:

 * i/o-specific constants
 * i/o-specific exceptions
 * schema validation
 * leaf value encoding and decoding
 * datum reader/writer stuff (?)

Also includes a generic representation for data, which
uses the following mapping:

  * Schema records are implemented as dict.
  * Schema arrays are implemented as list.
  * Schema maps are implemented as dict.
  * Schema strings are implemented as unicode.
  * Schema bytes are implemented as str.
  * Schema ints are implemented as int.
  * Schema longs are implemented as long.
  * Schema floats are implemented as float.
  * Schema doubles are implemented as float.
  * Schema booleans are implemented as bool. 
"""
import array
import struct
from avro import schema
import sys
from binascii import crc32
import operator

from ._io import read_data, write_data, validate
from .errors import AvroTypeException, SchemaResolutionException


# TODO(hammer): shouldn't ! be < for little-endian (according to spec?)
if sys.version_info >= (2, 5, 0):
  struct_class = struct.Struct
else:
  class SimpleStruct(object):
    def __init__(self, format):
      self.format = format
    def pack(self, *args):
      return struct.pack(self.format, *args)
    def unpack(self, *args):
      return struct.unpack(self.format, *args)
  struct_class = SimpleStruct


STRUCT_INT = struct_class('!I')     # big-endian unsigned int
STRUCT_LONG = struct_class('!Q')    # big-endian unsigned long long
STRUCT_FLOAT = struct_class('!f')   # big-endian float
STRUCT_DOUBLE = struct_class('!d')  # big-endian double
STRUCT_CRC32 = struct_class('>I')   # big-endian unsigned int


#
# Decoder/Encoder
#

class BinaryDecoder(object):
  """Read leaf values."""
  def __init__(self, reader):
    """
    reader is a Python object on which we can call read, seek, and tell.
    """
    self._reader = reader

  # read-only properties
  reader = property(lambda self: self._reader)

  def read(self, n):
    """
    Read n bytes.
    """
    return self._reader.read(n)

  def read_null(self):
    """
    null is written as zero bytes
    """
    return None

  def read_boolean(self):
    """
    a boolean is written as a single byte 
    whose value is either 0 (false) or 1 (true).
    """
    return ord(self._reader.read(1)) == 1

  def read_int(self):
    """
    int and long values are written using variable-length, zig-zag coding.
    """
    return self.read_long()

  def read_long(self):
    """
    int and long values are written using variable-length, zig-zag coding.
    """
    b = ord(self._reader.read(1))
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
      b = ord(self._reader.read(1))
      n |= (b & 0x7F) << shift
      shift += 7
    datum = (n >> 1) ^ -(n & 1)
    return datum

  def read_float(self):
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    shift_list = [0, 8, 16, 24]
    bit_list = [bit << places for bit, places in zip(array.array('B', self._reader.read(4)).tolist(), shift_list)]
    bits = reduce(operator.or_, bit_list, 0)
    return STRUCT_FLOAT.unpack(STRUCT_INT.pack(bits))[0]

  def read_double(self):
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    shift_list = [0, 8, 16, 24, 32, 40, 48, 56]
    bit_list = [bit << places for bit, places in zip(array.array('B', self._reader.read(8)).tolist(), shift_list)]
    bits = reduce(operator.or_, bit_list, 0)
    return STRUCT_DOUBLE.unpack(STRUCT_LONG.pack(bits))[0]

  def read_bytes(self):
    """
    Bytes are encoded as a long followed by that many bytes of data. 
    """
    return self._reader.read(self.read_long())

  def read_utf8(self):
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    return unicode(self.read_bytes(), "utf-8")

  def check_crc32(self, bytes):
    checksum = STRUCT_CRC32.unpack(self._reader.read(4))[0];
    if crc32(bytes) & 0xffffffff != checksum:
      raise schema.AvroException("Checksum failure")

  def skip_null(self):
    pass

  def skip_boolean(self):
    self.skip(1)

  def skip_int(self):
    self.skip_long()

  def skip_long(self):
    b = ord(self._reader.read(1))
    while (b & 0x80) != 0:
      b = ord(self._reader.read(1))

  def skip_float(self):
    self.skip(4)

  def skip_double(self):
    self.skip(8)

  def skip_bytes(self):
    self.skip(self.read_long())

  def skip_utf8(self):
    self.skip_bytes()

  def skip(self, n):
    self._reader.seek(self.reader.tell() + n)

class BinaryEncoder(object):
  """Write leaf values."""
  def __init__(self, writer):
    """
    writer is a Python object on which we can call write.
    """
    self._writer = writer

  # read-only properties
  writer = property(lambda self: self._writer)

  def write(self, datum):
    """Write an abritrary datum."""
    self._writer.write(datum)

  def write_null(self, datum):
    """
    null is written as zero bytes
    """
    pass
  
  def write_boolean(self, datum):
    """
    a boolean is written as a single byte 
    whose value is either 0 (false) or 1 (true).
    """
    if datum:
      self._writer.write(chr(1))
    else:
      self._writer.write(chr(0))

  def write_int(self, datum):
    """
    int and long values are written using variable-length, zig-zag coding.    
    """
    self.write_long(datum);

  def write_long(self, datum):
    """
    int and long values are written using variable-length, zig-zag coding.
    """
    lchr = chr #inspired by https://www.python.org/doc/essays/list2str/
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
      self._writer.write(lchr((datum & 0x7f) | 0x80))
      datum >>= 7
    self._writer.write(lchr(datum))

  def write_float(self, datum):
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    bits = STRUCT_INT.unpack(STRUCT_FLOAT.pack(datum))[0]
    shift_list = [0, 8, 16, 24]
    results = [((bits >> operand) & 0xFF) for operand in shift_list]
    self._writer.write(array.array('B', results).tostring())

  def write_double(self, datum):
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    bits = STRUCT_LONG.unpack(STRUCT_DOUBLE.pack(datum))[0]
    shift_list = [0, 8, 16, 24, 32, 40, 48, 56]
    results = [((bits >> operand) & 0xFF) for operand in shift_list]
    self._writer.write(array.array('B', results).tostring())


  def write_bytes(self, datum):
    """
    Bytes are encoded as a long followed by that many bytes of data. 
    """
    length = len(datum)
    self.write_long(length)
    self._writer.write(struct.pack('%ds' % length, datum))

  def write_utf8(self, datum):
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    self.write_bytes(datum.encode("utf-8"))

  def write_crc32(self, bytes):
    """
    A 4-byte, big-endian CRC32 checksum
    """
    self._writer.write(STRUCT_CRC32.pack(crc32(bytes) & 0xffffffff));


class DatumReader(object):
  """Deserialize Avro-encoded data into a Python data structure."""

  def __init__(self, writers_schema=None, readers_schema=None):
    """
    As defined in the Avro specification, we call the schema encoded
    in the data the "writer's schema", and the schema expected by the
    reader the "reader's schema".
    """
    self._writers_schema = writers_schema
    self._readers_schema = readers_schema 

  # read/write properties
  def set_writers_schema(self, writers_schema):
    self._writers_schema = writers_schema
  writers_schema = property(lambda self: self._writers_schema,
                            set_writers_schema)
  def set_readers_schema(self, readers_schema):
    self._readers_schema = readers_schema
  readers_schema = property(lambda self: self._readers_schema,
                            set_readers_schema)
  
  def read(self, decoder):
    if self.readers_schema is None:
      self.readers_schema = self.writers_schema
    return read_data(self.writers_schema, self.readers_schema, decoder)

  def read_data(self, writers_schema, readers_schema, decoder):
    return read_data(writers_schema, readers_schema, decoder)


class DatumWriter(object):
  """DatumWriter for generic python objects."""
  def __init__(self, writers_schema=None):
    self._writers_schema = writers_schema

  # read/write properties
  def set_writers_schema(self, writers_schema):
    self._writers_schema = writers_schema
  writers_schema = property(lambda self: self._writers_schema,
                            set_writers_schema)

  def write(self, datum, encoder):
    # validate datum
    if not validate(self.writers_schema, datum):
      raise AvroTypeException(self.writers_schema, datum)
    write_data(self.writers_schema, datum, encoder)

  def write_data(self, writers_schema, datum, encoder):
    return write_data(writers_schema, datum, encoder)
