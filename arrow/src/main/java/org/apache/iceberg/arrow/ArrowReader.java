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
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

public class ArrowReader<D> implements CloseableIterable<D> {
  private final InputFile file;
  private final Schema schema;
  private final Class<? extends StructLike> rootClass;
  private final Map<Integer, Class<? extends StructLike>> customTypes;
  private final Map<Integer, Object> constants;

  public ArrowReader(
      InputFile file,
      Schema schema,
      Class<? extends StructLike> rootClass,
      Map<Integer, Class<? extends StructLike>> customTypes,
      Map<Integer, Object> constants) {
    this.file = file;
    this.schema = schema;
    this.rootClass = rootClass;
    this.customTypes = customTypes;
    this.constants = constants;
  }

  @Override
  public CloseableIterator<D> iterator() {
    return new ArrowIPCIterator<>(file, schema, rootClass, customTypes, constants);
  }

  @Override
  public void close() throws IOException {
    // nothing to close here, the iterator handles its own resources
  }

  private static class ArrowIPCIterator<D> implements CloseableIterator<D> {
    private final BufferAllocator allocator;
    private final ArrowFileReader reader;
    private final VectorSchemaRoot root;
    private final Iterator<ArrowBlock> blockIterator;
    private int rowCountInBatch = 0;
    private int currentRowInBatch = 0;
    private long offset = 0L;
    private final ArrowRowReader<D> rowReader;

    ArrowIPCIterator(
        InputFile file,
        Schema schema,
        Class<? extends StructLike> rootClass,
        Map<Integer, Class<? extends StructLike>> customTypes,
        Map<Integer, Object> constants) {
      this.allocator =
          ArrowAllocation.rootAllocator().newChildAllocator("arrow-ipc-reader", 0, Long.MAX_VALUE);
      try {
        SeekableInputStream inputStream = file.newStream();
        this.reader =
            new ArrowFileReader(
                new SeekableInputStreamWrapper(inputStream, file.getLength()), allocator);
        this.root = reader.getVectorSchemaRoot();
        this.blockIterator = reader.getRecordBlocks().iterator();
        this.rowReader = new ArrowRowReader<>(schema, root, rootClass, customTypes, constants);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to open Arrow IPC file");
      }
    }

    @Override
    public boolean hasNext() {
      while (currentRowInBatch >= rowCountInBatch) {
        if (blockIterator.hasNext()) {
          try {
            offset += rowCountInBatch;
            reader.loadRecordBatch(blockIterator.next());
            rowCountInBatch = root.getRowCount();
            currentRowInBatch = 0;
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to load record batch");
          }
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
      D row = rowReader.read(currentRowInBatch, offset);
      currentRowInBatch++;
      return row;
    }

    @Override
    public void close() throws IOException {
      root.close();
      reader.close();
      allocator.close();
    }
  }

  private static class SeekableInputStreamWrapper implements SeekableByteChannel {
    private final SeekableInputStream stream;
    private final long size;

    SeekableInputStreamWrapper(SeekableInputStream stream, long size) {
      this.stream = stream;
      this.size = size;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (dst.hasArray()) {
        int read = stream.read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
        if (read > 0) {
          dst.position(dst.position() + read);
        }
        return read;
      } else {
        byte[] bytes = new byte[dst.remaining()];
        int read = stream.read(bytes);
        if (read > 0) {
          dst.put(bytes, 0, read);
        }
        return read;
      }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
      return stream.getPos();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      stream.seek(newPosition);
      return this;
    }

    @Override
    public long size() throws IOException {
      return size;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }
}
