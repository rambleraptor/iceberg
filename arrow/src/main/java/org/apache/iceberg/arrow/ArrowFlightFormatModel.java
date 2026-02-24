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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.ArrowFlightInputFile;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.rest.responses.FlightEndpoint;

public class ArrowFlightFormatModel<D, S> implements FormatModel<D, S> {
  private final Class<? extends D> type;
  private final Class<S> schemaType;
  private final BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter;

  public static <D, S> ArrowFlightFormatModel<D, S> create(
      Class<? extends D> type,
      Class<S> schemaType,
      BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter) {
    return new ArrowFlightFormatModel<>(type, schemaType, converter);
  }

  private ArrowFlightFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter) {
    this.type = type;
    this.schemaType = schemaType;
    this.converter = converter;
  }

  @Override
  public FileFormat format() {
    return FileFormat.ARROW_FLIGHT;
  }

  @Override
  public Class<? extends D> type() {
    return type;
  }

  @Override
  public Class<S> schemaType() {
    return schemaType;
  }

  @Override
  public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    throw new UnsupportedOperationException("Arrow Flight write support not implemented");
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ArrowFlightReadBuilder<>(inputFile, converter);
  }

  private static class ArrowFlightReadBuilder<D, S> implements ReadBuilder<D, S> {
    private final InputFile inputFile;
    private final BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter;
    private Schema projectSchema;

    private ArrowFlightReadBuilder(
        InputFile inputFile,
        BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter) {
      this.inputFile = inputFile;
      this.converter = converter;
    }

    @Override
    public ReadBuilder<D, S> split(long start, long length) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(Schema schema) {
      this.projectSchema = schema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> engineProjection(S schema) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression newFilter) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int rowsPerBatch) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      Schema finalSchema = projectSchema != null ? projectSchema : (Schema) null; // We need a way to get the full schema if not projected
      return new ArrowFlightIterable<>(inputFile, finalSchema, converter);
    }
  }

  private static class ArrowFlightIterable<D> extends CloseableGroup implements CloseableIterable<D> {
    private final InputFile inputFile;
    private final Schema projectSchema;
    private final BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter;

    private ArrowFlightIterable(
        InputFile inputFile,
        Schema projectSchema,
        BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter) {
      this.inputFile = inputFile;
      this.projectSchema = projectSchema;
      this.converter = converter;
    }

    @Override
    public CloseableIterator<D> iterator() {
      FlightEndpoint endpoint = ArrowFlightInputFile.decode(inputFile.location());
      BufferAllocator allocator = ArrowAllocation.rootAllocator();
      Location location;
      try {
        location = new Location(endpoint.locations().get(0));
      } catch (java.net.URISyntaxException e) {
        throw new RuntimeException("Invalid flight location: " + endpoint.locations().get(0), e);
      }
      FlightClient client = FlightClient.builder(allocator, location).build();
      addCloseable(client);

      FlightStream stream = client.getStream(new Ticket(endpoint.ticket()));
      addCloseable(stream);

      return new ArrowFlightIterator<>(stream, projectSchema, converter);
    }
  }

  private static class ArrowFlightIterator<D> implements CloseableIterator<D> {
    private final FlightStream stream;
    private final Schema projectSchema;
    private final BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter;
    private Iterator<D> currentBatch = null;

    private ArrowFlightIterator(
        FlightStream stream,
        Schema projectSchema,
        BiFunction<VectorSchemaRoot, Schema, CloseableIterable<D>> converter) {
      this.stream = stream;
      this.projectSchema = projectSchema;
      this.converter = converter;
    }

    @Override
    public boolean hasNext() {
      while (currentBatch == null || !currentBatch.hasNext()) {
        if (stream.next()) {
          currentBatch = converter.apply(stream.getRoot(), projectSchema).iterator();
        } else {
          return false;
        }
      }
      return true;
    }

    @Override
    public D next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return currentBatch.next();
    }

    @Override
    public void close() throws IOException {
      try {
        stream.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
