/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.format;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/** Copies entries to another table while scanning. Initialized by the 
 * shell 'tee' command.
 */
public class TeeFormatter implements Formatter {
  private Iterator<Entry<Key,Value>> si;
  private boolean doTimestamps;
  private Connector connector = null;
  private String teeTableName = null;
  
  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    checkState(si, false);
    si = scanner.iterator();
    doTimestamps = printTimestamps;
  }
  
  public boolean hasNext() {
    checkState(si, true);
    return si.hasNext();
  }
  
  public String next() {
    checkState(si, true);
    Entry<Key,Value> entry = si.next();
    copyEntry(entry);
    return formatEntry(entry, doTimestamps);
  }
  
  public void remove() {
    checkState(si, true);
    si.remove();
  }
  
  private void copyEntry(Entry<Key,Value> entry) {
      BatchWriter wr = null;
      try {
          wr = connector.createBatchWriter(teeTableName, 10000000, 10000, 5);
          Key key = entry.getKey();
          Value value = entry.getValue();
          Mutation m = new Mutation(key.getRow());
          m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility().toString()), key.getTimestamp(), value);
          wr.addMutation(m);
      } catch (TableNotFoundException e) {
          throw new RuntimeException("Unable to find table " + teeTableName, e);
      } catch (MutationsRejectedException e) {
          throw new RuntimeException("Mutation rejected while copying entry to tee table.", e);
      } finally {
          if (wr != null) {
              try {
                  wr.close();
              } catch (MutationsRejectedException e) {
          throw new RuntimeException("Mutation rejected while closng writer to tee table.", e);
              }
          }
      }
  }
  
  static void checkState(Iterator<Entry<Key,Value>> si, boolean expectInitialized) {
    if (expectInitialized && si == null)
      throw new IllegalStateException("Not initialized");
    if (!expectInitialized && si != null)
      throw new IllegalStateException("Already initialized");
  }
  
  // this should be replaced with something like Record.toString();
  public static String formatEntry(Entry<Key,Value> entry, boolean showTimestamps) {
    StringBuilder sb = new StringBuilder();
    
    // append row
    appendText(sb, entry.getKey().getRow()).append(" ");
    
    // append column family
    appendText(sb, entry.getKey().getColumnFamily()).append(":");
    
    // append column qualifier
    appendText(sb, entry.getKey().getColumnQualifier()).append(" ");
    
    // append visibility expression
    sb.append(new ColumnVisibility(entry.getKey().getColumnVisibility()));
    
    // append timestamp
    if (showTimestamps)
      sb.append(" ").append(entry.getKey().getTimestamp());
    
    // append value
    if (entry.getValue() != null && entry.getValue().getSize() > 0) {
      sb.append("\t");
      appendValue(sb, entry.getValue());
    }
    
    return sb.toString();
  }
  
  static StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }
  
  static StringBuilder appendValue(StringBuilder sb, Value value) {
    return appendBytes(sb, value.get(), 0, value.get().length);
  }
  
  static StringBuilder appendBytes(StringBuilder sb, byte ba[], int offset, int len) {
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\')
        sb.append("\\\\");
      else if (c >= 32 && c <= 126)
        sb.append((char) c);
      else
        sb.append("\\x").append(String.format("%02X", c));
    }
    return sb;
  }
  
  public Iterator<Entry<Key,Value>> getScannerIterator() {
    return si;
  }

    /**
     * @return the connector
     */
    public Connector getConnector() {
        return connector;
    }

    /**
     * @param connector the connector to set
     */
    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public void setTeeTableName(String teeTableName) {
        this.teeTableName = teeTableName;
    }
}
