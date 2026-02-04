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
import java.nio.channels.Channels;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

class ArrowFileAppender<D> implements FileAppender<D> {
  private static final int DEFAULT_BATCH_SIZE = 1024;

  private final BufferAllocator allocator;
  private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
  private final VectorSchemaRoot root;
  private final ArrowFileWriter writer;
  private final ArrowWriter<D> arrowWriter;
  private final int batchSize;
  private long numRecords = 0L;
  private int batchCount = 0;
  private boolean isClosed = false;
  private final PositionOutputStream stream;

  ArrowFileAppender(OutputFile file, Schema icebergSchema, Map<String, String> config, boolean overwrite) throws IOException {
    this.allocator = ArrowAllocation.rootAllocator().newChildAllocator("arrow-file-appender", 0, Long.MAX_VALUE);
    this.arrowSchema = ArrowSchemaUtil.convert(icebergSchema);
    this.root = VectorSchemaRoot.create(arrowSchema, allocator);
    this.stream = overwrite ? file.createOrOverwrite() : file.create();
    this.writer = new ArrowFileWriter(root, null, Channels.newChannel(stream));
    this.writer.start();
    this.arrowWriter = ArrowWriter.create(icebergSchema, root);
    this.batchSize = DEFAULT_BATCH_SIZE;
  }

  @Override
  public void add(D datum) {
    if (batchCount >= batchSize) {
      writeBatch();
    }
    arrowWriter.write(batchCount, datum);
    batchCount++;
    numRecords++;
  }

  private void writeBatch() {
    if (batchCount > 0) {
      root.setRowCount(batchCount);
      try {
        writer.writeBatch();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to write batch");
      }
      root.clear();
      batchCount = 0;
    }
  }

  @Override
  public Metrics metrics() {
    return new Metrics(numRecords, null, null, null, null);
  }

  @Override
  public long length() {
    try {
      return stream.storedLength();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get length");
    }
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      writeBatch();
      writer.end();
      writer.close();
      root.close();
      allocator.close();
      isClosed = true;
    }
  }
}
