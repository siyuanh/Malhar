/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;

import com.datatorrent.lib.bucket.*;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link Deduper}
 */
public class DeduperTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DeduperTest";
  private final static String APP_ID = "DeduperTest";

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperWithTimeBuckets<DummyEvent, DummyEvent>
  {

    @Override
    public void bucketLoaded(long bucketKey)
    {
      try {
        super.bucketLoaded(bucketKey);
        eventBucketExchanger.exchange(bucketKey);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected BucketStore<DummyEvent> getBucketStore(Context.OperatorContext context)
    {
      return new HdfsBucketStore<DummyEvent>(applicationPath, 0, maxNoOfBucketsInDir, partitionKeys, partitionMask);
    }

    @Override
    public DummyEvent convert(DummyEvent dummyEvent)
    {
      return dummyEvent;
    }
  }

  private static DeduperWithTimeBuckets<DummyEvent, DummyEvent> deduper;
  private static String applicationPath;
  private static BucketManager<DummyEvent> storageManager;
  private static List<DummyEvent> events = Lists.newArrayList();

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testDedup()
  {
    CollectorTestSink collectorTestSink = new CollectorTestSink<DummyEvent>();
    deduper.output.setSink(collectorTestSink);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 10, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

    logger.debug("start round 1");
    deduper.beginWindow(1);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 0, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    //Test the sliding window
    try {
      Thread.sleep(1500);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    deduper.handleIdleTime();
    long now = System.currentTimeMillis();
    for (int i = 10; i < 15; i++) {
      events.add(new DummyEvent(i, now));
    }

    logger.debug("start round 2");
    deduper.beginWindow(2);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 5, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 2");
  }

  private void testRound(List<DummyEvent> events)
  {
    for (DummyEvent event : events) {
      deduper.input.process(event);
    }
    try {
      eventBucketExchanger.exchange(null, 1, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    catch (TimeoutException e) {
      logger.debug("Timeout Happened");
    }
  }

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    deduper = new DummyDeduper();
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();

    calendar.add(Calendar.DATE, -2);
    long bucketStart = calendar.getTimeInMillis();

    deduper.setStartOfBucketsInMillis(bucketStart);
    deduper.setBucketSpanInMillis(1000); //1 second

    int totalNumberOfBuckets = (int) ((now - deduper.getStartOfBucketsInMillis())
      / deduper.getBucketSpanInMillis());

    storageManager = new BucketManager<DummyEvent>(true, totalNumberOfBuckets, 100, 60000);
    deduper.setBucketManager(storageManager);

    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(i, now));
    }
    events.add(new DummyEvent(5, now));

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(APP_ID, applicationPath, 0));
  }


  @AfterClass
  public static void teardown()
  {
    storageManager.shutdownService();
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.get(root.toUri(), new Configuration());
      fs.delete(root, true);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
