/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sync.common.metrics;

import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieSyncMetrics {


  @Mock
  HoodieSyncConfig syncConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieSyncMetrics hoodieSyncMetrics;
  Metrics metrics;

  @BeforeEach
  void setUp() {
    when(metricsConfig.isMetricsOn()).thenReturn(true);
    when(syncConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.INMEMORY);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    when(metricsConfig.getMetricReporterMetricsNamePrefix()).thenReturn("test_prefix");
    hoodieSyncMetrics = new HoodieSyncMetrics(syncConfig, "TestHiveSyncTool");
    metrics = hoodieSyncMetrics.getMetrics();
  }

  @AfterEach
  void shutdownMetrics() {
    metrics.shutdown();
  }

  @Test
  void testUpdateRecreateAndSyncMetrics() throws InterruptedException {
    Timer.Context timerCtx = hoodieSyncMetrics.getRecreateAndSyncTimer();
    Thread.sleep(5);
    long durationInMs = hoodieSyncMetrics.getDurationInMs(timerCtx.stop());
    hoodieSyncMetrics.updateRecreateAndSyncMetrics(durationInMs);
    String metricName = hoodieSyncMetrics.getMetricsName("recreate_and_sync", "duration");
    long msec = (Long) metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
  }

  @Test
  void testEmitRecreateAndSyncFailureMetric() {
    hoodieSyncMetrics.emitRecreateAndSyncFailureMetric();
    String metricsName = hoodieSyncMetrics.getMetricsName("counter", "recreate_and_sync.failure");
    long count = metrics.getRegistry().getCounters().get(metricsName).getCount();
    assertTrue(count > 0);
  }
}
