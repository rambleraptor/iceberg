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
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class Arrow {
  private Arrow() {}

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  public static DataWriteBuilder writeData(EncryptedOutputFile file) {
    return new DataWriteBuilder(file.encryptingOutputFile())
        .withKeyMetadata(file.keyMetadata());
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class WriteBuilder implements InternalData.WriteBuilder {
    private final OutputFile file;
    private Schema schema = null;
    private Map<String, String> config = Maps.newHashMap();
    private boolean overwrite = false;

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      return this;
    }

    @Override
    public WriteBuilder schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public WriteBuilder named(String name) {
      return this;
    }

    @Override
    public WriteBuilder set(String property, String value) {
      config.put(property, value);
      return this;
    }

    @Override
    public WriteBuilder meta(String property, String value) {
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    @Override
    public WriteBuilder overwrite() {
      this.overwrite = true;
      return this;
    }

    @Override
    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new ArrowFileAppender<>(file, schema, config, overwrite);
    }
  }

  public static class DataWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private PartitionSpec spec = null;
    private StructLike partition = null;
    private EncryptionKeyMetadata keyMetadata = null;
    private SortOrder sortOrder = null;

    private DataWriteBuilder(OutputFile file) {
      this.appenderBuilder = new WriteBuilder(file);
      this.location = file.location();
    }

    public DataWriteBuilder forTable(Table table) {
      schema(table.schema());
      withSpec(table.spec());
      setAll(table.properties());
      return this;
    }

    public DataWriteBuilder schema(Schema newSchema) {
      appenderBuilder.schema(newSchema);
      return this;
    }

    public DataWriteBuilder set(String property, String value) {
      appenderBuilder.set(property, value);
      return this;
    }

    public DataWriteBuilder setAll(Map<String, String> properties) {
      appenderBuilder.setAll(properties);
      return this;
    }

    public DataWriteBuilder overwrite() {
      appenderBuilder.overwrite();
      return this;
    }

    public DataWriteBuilder withSpec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public DataWriteBuilder withPartition(StructLike newPartition) {
      this.partition = newPartition;
      return this;
    }

    public DataWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public DataWriteBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder;
      return this;
    }

    public DataWriteBuilder metricsConfig(Object ignored) {
      return this;
    }

    public <T> DataWriter<T> build() throws IOException {
      Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
      Preconditions.checkArgument(
          spec.isUnpartitioned() || partition != null,
          "Partition must not be null when creating data writer for partitioned spec");

      FileAppender<T> fileAppender = appenderBuilder.build();
      return new DataWriter<>(
          fileAppender, FileFormat.ARROW, location, spec, partition, keyMetadata, sortOrder);
    }
  }

  public static class ReadBuilder implements InternalData.ReadBuilder {
    private final InputFile file;
    private Schema schema;
    private Class<? extends StructLike> rootClass;
    private final Map<Integer, Class<? extends StructLike>> customTypes = Maps.newHashMap();
    private final Map<Integer, Object> constants = Maps.newHashMap();

    private ReadBuilder(InputFile file) {
      this.file = file;
    }

    @Override
    public ReadBuilder project(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ReadBuilder split(long newStart, long newLength) {
      return this;
    }

    @Override
    public ReadBuilder reuseContainers() {
      return this;
    }

    @Override
    public ReadBuilder setRootType(Class<? extends StructLike> rootClass) {
      this.rootClass = rootClass;
      return this;
    }

    @Override
    public ReadBuilder setCustomType(int fieldId, Class<? extends StructLike> structClass) {
      customTypes.put(fieldId, structClass);
      return this;
    }

    public ReadBuilder setConstant(int fieldId, Object value) {
      constants.put(fieldId, value);
      return this;
    }

    public ReadBuilder setConstants(Map<Integer, ?> newConstants) {
      constants.putAll(newConstants);
      return this;
    }

    @Override
    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new ArrowReader<>(file, schema, rootClass, customTypes, constants);
    }
  }
}
