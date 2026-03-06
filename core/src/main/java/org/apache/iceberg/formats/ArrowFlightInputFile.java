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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class ArrowFlightInputFile implements InputFile {
  private final String location;

  public ArrowFlightInputFile(String location) {
    this.location = location;
  }

  @Override
  public long getLength() {
    return 1024;
  }

  @Override
  public SeekableInputStream newStream() {
    throw new UnsupportedOperationException("ArrowFlightInputFile does not support newStream()");
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    return true;
  }

  public static FlightData decode(String location) {
    Preconditions.checkArgument(location.startsWith("flight:"), "Invalid flight location: %s", location);
    try {
      URI uri = new URI(location);
      String query = uri.getRawQuery();
      Preconditions.checkNotNull(query, "Flight URI must have query parameters: %s", location);

      Map<String, String> params = Arrays.stream(query.split("&"))
          .map(s -> s.split("=", 2))
          .collect(Collectors.toMap(
              a -> a[0],
              a -> a.length > 1 ? a[1] : ""
          ));

      String ticketBase64 = params.get("ticket");
      Preconditions.checkNotNull(ticketBase64, "Flight URI must have a ticket parameter: %s", location);
      byte[] ticket = Base64.getUrlDecoder().decode(ticketBase64);

      String locationsStr = params.get("locations");
      Preconditions.checkNotNull(locationsStr, "Flight URI must have a locations parameter: %s", location);
      List<String> locations = Arrays.asList(locationsStr.split(","));

      return new FlightData(ticket, locations);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decode flight location: " + location, e);
    }
  }

  public static class FlightData {
    private final byte[] ticket;
    private final List<String> locations;

    public FlightData(byte[] ticket, List<String> locations) {
      this.ticket = ticket;
      this.locations = ImmutableList.copyOf(locations);
    }

    public byte[] ticket() {
      return ticket;
    }

    public List<String> locations() {
      return locations;
    }
  }
}
