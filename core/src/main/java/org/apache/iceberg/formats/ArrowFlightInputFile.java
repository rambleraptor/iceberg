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
package org.apache.iceberg.formats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.responses.FlightEndpoint;
import org.apache.iceberg.util.JsonUtil;

public class ArrowFlightInputFile implements InputFile {
  private static final Map<String, FlightEndpoint> ENDPOINTS = new ConcurrentHashMap<>();

  private final String location;

  public static void register(String taskId, FlightEndpoint endpoint) {
    ENDPOINTS.put(taskId, endpoint);
  }

  public ArrowFlightInputFile(String taskId) {
    this.location = "flight://task/" + taskId;
  }

  @Override
  public long getLength() {
    return 1024;
  }

  @Override
  public SeekableInputStream newStream() {
    return new SeekableInputStream() {
      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void seek(long newPos) throws IOException {
      }

      @Override
      public int read() throws IOException {
        return -1;
      }
    };
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    return true;
  }

  public static FlightEndpoint decode(String location) {
    Preconditions.checkArgument(location.startsWith("flight://task/"), "Invalid flight location: %s", location);
    String taskId = location.substring("flight://task/".length());
    FlightEndpoint endpoint = ENDPOINTS.get(taskId);
    Preconditions.checkNotNull(endpoint, "No flight endpoint registered for task: %s", taskId);
    return endpoint;
  }
}
