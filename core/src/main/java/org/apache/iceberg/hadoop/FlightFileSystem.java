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
package org.apache.iceberg.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class FlightFileSystem extends FileSystem {
  private URI uri;

  public FlightFileSystem() {
  }

  @Override
  public void initialize(URI name, org.apache.hadoop.conf.Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = name;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return "flight";
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return new FSDataInputStream(new MockFSInputStream());
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new FSDataInputStream(new MockFSInputStream());
  }

  @Override
  public FSDataInputStream open(org.apache.hadoop.fs.PathHandle fd, int bufferSize) throws IOException {
    return new FSDataInputStream(new MockFSInputStream());
  }

  private static class MockFSInputStream extends java.io.InputStream implements org.apache.hadoop.fs.Seekable, org.apache.hadoop.fs.PositionedReadable {
    @Override
    public int read() throws IOException {
      return -1;
    }

    @Override
    public void seek(long pos) throws IOException {
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return -1;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException("FlightFileSystem.create is read-only");
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException("FlightFileSystem.append is read-only");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return new FileStatus[0];
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("/");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return new FileStatus(1024, false, 1, 1024, 0, f);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return true;
  }

  @Override
  public org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    return getFileBlockLocations(file.getPath(), start, len);
  }

  @Override
  public org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    return new org.apache.hadoop.fs.BlockLocation[] {
        new org.apache.hadoop.fs.BlockLocation(new String[] {"localhost"}, new String[] {"localhost"}, 0, len)
    };
  }

  @Override
  public org.apache.hadoop.fs.FileChecksum getFileChecksum(Path f, long length) throws IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.fs.ContentSummary getContentSummary(Path f) throws IOException {
    return new org.apache.hadoop.fs.ContentSummary.Builder()
        .length(1024).fileCount(1).directoryCount(0).spaceConsumed(1024).build();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
