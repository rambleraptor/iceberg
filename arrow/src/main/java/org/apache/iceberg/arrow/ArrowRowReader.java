/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.arrow;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class ArrowRowReader<D> {
  private final List<ArrowValueReader> readers;
  private final int numFields;

  ArrowRowReader(Schema schema, VectorSchemaRoot root) {
    this.readers = Lists.newArrayList();
    this.numFields = schema.columns().size();
    for (int i = 0; i < numFields; i++) {
      readers.add(createValueReader(schema.columns().get(i).type(), root.getVector(i)));
    }
  }

  @SuppressWarnings("unchecked")
  public D read(int rowId) {
    return (D) new ArrowStructLike(readers, rowId);
  }

  private static ArrowValueReader createValueReader(Type type, FieldVector vector) {
    switch (type.typeId()) {
      case INTEGER:
        return new IntValueReader((IntVector) vector);
      case LONG:
        return new LongValueReader((BigIntVector) vector);
      case FLOAT:
        return new FloatValueReader((Float4Vector) vector);
      case DOUBLE:
        return new DoubleValueReader((Float8Vector) vector);
      case STRING:
        return new StringValueReader((VarCharVector) vector);
      case BOOLEAN:
        return new BooleanValueReader((BitVector) vector);
      case DECIMAL:
        return new DecimalValueReader((DecimalVector) vector);
      case DATE:
        return new DateValueReader((DateDayVector) vector);
      case TIME:
        return new TimeValueReader((TimeMicroVector) vector);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return new TimestampTzValueReader((TimeStampMicroTZVector) vector);
        } else {
          return new TimestampValueReader((TimeStampMicroVector) vector);
        }
      case TIMESTAMP_NANO:
        if (((Types.TimestampNanoType) type).shouldAdjustToUTC()) {
          return new TimestampNanoTzValueReader((TimeStampNanoTZVector) vector);
        } else {
          return new TimestampNanoValueReader((TimeStampNanoVector) vector);
        }
      case BINARY:
        return new BinaryValueReader((VarBinaryVector) vector);
      case FIXED:
        return new FixedValueReader((FixedSizeBinaryVector) vector);
      case UUID:
        return new UUIDValueReader((FixedSizeBinaryVector) vector);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private interface ArrowValueReader {
    Object read(int rowId);
  }

  private static class IntValueReader implements ArrowValueReader {
    private final IntVector vector;

    IntValueReader(IntVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class LongValueReader implements ArrowValueReader {
    private final BigIntVector vector;

    LongValueReader(BigIntVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class FloatValueReader implements ArrowValueReader {
    private final Float4Vector vector;

    FloatValueReader(Float4Vector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class DoubleValueReader implements ArrowValueReader {
    private final Float8Vector vector;

    DoubleValueReader(Float8Vector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class StringValueReader implements ArrowValueReader {
    private final VarCharVector vector;

    StringValueReader(VarCharVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : new String(vector.get(rowId), StandardCharsets.UTF_8);
    }
  }

  private static class BooleanValueReader implements ArrowValueReader {
    private final BitVector vector;

    BooleanValueReader(BitVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId) != 0;
    }
  }

  private static class DecimalValueReader implements ArrowValueReader {
    private final DecimalVector vector;

    DecimalValueReader(DecimalVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.getObject(rowId);
    }
  }

  private static class DateValueReader implements ArrowValueReader {
    private final DateDayVector vector;

    DateValueReader(DateDayVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class TimeValueReader implements ArrowValueReader {
    private final TimeMicroVector vector;

    TimeValueReader(TimeMicroVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class TimestampValueReader implements ArrowValueReader {
    private final TimeStampMicroVector vector;

    TimestampValueReader(TimeStampMicroVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class TimestampTzValueReader implements ArrowValueReader {
    private final TimeStampMicroTZVector vector;

    TimestampTzValueReader(TimeStampMicroTZVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class TimestampNanoValueReader implements ArrowValueReader {
    private final TimeStampNanoVector vector;

    TimestampNanoValueReader(TimeStampNanoVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class TimestampNanoTzValueReader implements ArrowValueReader {
    private final TimeStampNanoTZVector vector;

    TimestampNanoTzValueReader(TimeStampNanoTZVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class BinaryValueReader implements ArrowValueReader {
    private final VarBinaryVector vector;

    BinaryValueReader(VarBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : ByteBuffer.wrap(vector.get(rowId));
    }
  }

  private static class FixedValueReader implements ArrowValueReader {
    private final FixedSizeBinaryVector vector;

    FixedValueReader(FixedSizeBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class UUIDValueReader implements ArrowValueReader {
    private final FixedSizeBinaryVector vector;

    UUIDValueReader(FixedSizeBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class ArrowStructLike implements StructLike {
    private final Object[] values;

    ArrowStructLike(List<ArrowValueReader> readers, int rowId) {
      this.values = new Object[readers.size()];
      for (int i = 0; i < values.length; i++) {
        this.values[i] = readers.get(i).read(rowId);
      }
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Read-only");
    }
  }
}
