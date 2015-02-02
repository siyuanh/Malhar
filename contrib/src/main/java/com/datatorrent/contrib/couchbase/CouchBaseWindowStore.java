/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchbase;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.TransactionableStore;

import com.datatorrent.common.util.DTThrowable;

/**
 * CouchBaseWindowStore which transactional support.
 * It does not guarantee exactly once property.It only skips tuple
 * processed in previous windows and provides at least once and at most once properties.
 *
 * @since 2.0.0
 */
public class CouchBaseWindowStore extends CouchBaseStore implements TransactionableStore
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseWindowStore.class);
  private static final String DEFAULT_LAST_WINDOW_PREFIX = "last_window";
  private static String lastWindowValue;
  protected transient CouchbaseClient clientMeta;
  protected static String bucketMeta;

  public String getBucketMeta()
  {
    return bucketMeta;
  }

  public void setBucketMeta(String bucketMeta)
  {
    this.bucketMeta = bucketMeta;
  }

  public String getPasswordMeta()
  {
    return passwordMeta;
  }

  public void setPasswordMeta(String passwordMeta)
  {
    this.passwordMeta = passwordMeta;
  }

  protected String passwordMeta;

  public CouchBaseWindowStore()
  {
    clientMeta = null;
    lastWindowValue = DEFAULT_LAST_WINDOW_PREFIX;
    bucketMeta = "default";
    passwordMeta = "";
  }

  public CouchbaseClient getMetaInstance()
  {
    return clientMeta;
  }

  @Override
  public void connect() throws IOException
  {
    super.connect();

    try {
      CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
      cfb.setOpTimeout(timeout);  // wait up to 10 seconds for an operation to succeed
      cfb.setOpQueueMaxBlockTime(blockTime); // wait up to 10 second when trying to enqueue an operation
      clientMeta = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs, bucketMeta, passwordMeta));
    }
    catch (IOException e) {
      logger.error("Error connecting to Couchbase: " + e.getMessage());
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    //logger.info("in getCommittedWindowId");
    byte[] value = null;
    String key = appId + "_" + operatorId + "_" + lastWindowValue;
    logger.info("key is" + key);
    value = (byte[])clientMeta.get(key);
    logger.info("value is" + value);
    if (value != null) {
      long longval = toLong(value);
      return longval;
    }
    return -1;
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    //logger.info("in storeCommittedWindowId");
    byte[] WindowIdBytes = toBytes(windowId);
    String key = appId + "_" + operatorId + "_" + lastWindowValue;
    try {
      clientMeta.set(key, WindowIdBytes).get();
    }
    catch (InterruptedException ex) {
      DTThrowable.rethrow(ex);
    }
    catch (ExecutionException ex) {
      DTThrowable.rethrow(ex);
    }

  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
  }

  @Override
  public void beginTransaction()
  {
  }

  @Override
  public void commitTransaction()
  {
  }

  @Override
  public void rollbackTransaction()
  {
  }

  @Override
  public boolean isInTransaction()
  {
    return false;
  }

  public static long toLong(byte[] b)
  {
    ByteArrayInputStream baos = new ByteArrayInputStream(b);
    DataInputStream dos = new DataInputStream(baos);
    long result = 0;
    try {
      result = dos.readLong();
      dos.close();
    }
    catch (IOException e) {
      logger.error("error converting to long");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  public static byte[] toBytes(long l)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.SIZE / 8);
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] result = null;
    try {
      dos.writeLong(l);
      result = baos.toByteArray();
      dos.close();
    }
    catch (IOException e) {
      logger.error("error converting to byte array");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  @Override
  public void disconnect() throws IOException
  {
    clientMeta.shutdown(shutdownTimeout, TimeUnit.SECONDS);
    super.disconnect();
  }

}
