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
import org.apache.iceberg.InternalData;
import org.apache.iceberg.Schema;
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

  public static ArrowReader.ReadBuilder read(InputFile file) {
    return ArrowReader.read(file);
  }

  public static class WriteBuilder implements InternalData.WriteBuilder {
    private final OutputFile file;
    private Schema schema = null;
    private Map<String, String> config = Maps.newHashMap();
    private boolean overwrite = false;

    private WriteBuilder(OutputFile file) {
      this.file = file;
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
}
