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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieSyncMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSyncMetrics.class);
  private Metrics metrics;
  private HoodieSyncConfig config;
  private HoodieMetricsConfig metricsConfig;

  private final String syncToolName;

  public String recreateAndSyncTimerName;
  public String recreateAndSyncFailureCounterName;

  public Timer recreateAndSyncTimer;
  public Counter recreateAndSyncFailureCounter;

  public HoodieSyncMetrics(HoodieSyncConfig config, String syncToolName) {
    this.config = config;
    this.metricsConfig = config.getMetricsConfig();
    this.syncToolName = syncToolName;
    if (metricsConfig.isMetricsOn()) {
      metrics = Metrics.getInstance(metricsConfig);
      this.recreateAndSyncTimerName = getMetricsName("timer", "meta_sync.recreate_table");
      this.recreateAndSyncFailureCounterName = getMetricsName("counter", "meta_sync.recreate_table.failure");
    }
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public Timer.Context getRecreateAndSyncTimer() {
    if (metricsConfig.isMetricsOn() && recreateAndSyncTimer == null) {
      recreateAndSyncTimer = createTimer(recreateAndSyncTimerName);
    }
    return recreateAndSyncTimer == null ? null : recreateAndSyncTimer.time();
  }

  private Timer createTimer(String name) {
    return metricsConfig.isMetricsOn() ? metrics.getRegistry().timer(name) : null;
  }

  public void emitRecreateAndSyncFailureMetric() {
    recreateAndSyncFailureCounter = getCounter(recreateAndSyncFailureCounter, recreateAndSyncFailureCounterName);
    recreateAndSyncFailureCounter.inc();
  }

  public void updateRecreateAndSyncMetrics(long durationInMs) {
    if (metricsConfig.isMetricsOn()) {
      LOG.info(
          String.format("Sending recreate and sync metrics (duration=%d)", durationInMs));
      metrics.registerGauge(getMetricsName("meta_sync", "recreate_table.duration"), durationInMs);
    }
  }

  /**
   * By default, the timer context returns duration with nano seconds. Convert it to millisecond.
   */
  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }

  @VisibleForTesting
  public String getMetricsName(String action, String metric) {
    if (metricsConfig == null) {
      return null;
    }
    if (StringUtils.isNullOrEmpty(metricsConfig.getMetricReporterMetricsNamePrefix())) {
      return String.format("%s.%s.%s", action, metric, syncToolName);
    } else {
      return String.format("%s.%s.%s.%s", metricsConfig.getMetricReporterMetricsNamePrefix(), action, metric, syncToolName);
    }
  }

  public Counter getCounter(Counter counter, String name) {
    if (counter == null) {
      return metrics.getRegistry().counter(name);
    }
    return counter;
  }
}
