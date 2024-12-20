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

package org.apache.hudi.client.transaction;

import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.MetricsReporterType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TestHoodieLockMetrics {

  @Test
  public void testMetricsHappyPath() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/gdsafsd")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
        .forTable("idk").withPath("/dsfasdf/asdf")
        .withMetricsConfig(metricsConfig)
        .build());

    //lock acquired
    assertDoesNotThrow(lockMetrics::startLockApiTimerContext);
    assertDoesNotThrow(lockMetrics::updateLockAcquiredMetric);
    assertDoesNotThrow(lockMetrics::updateLockHeldTimerMetrics);

    //lock not acquired
    assertDoesNotThrow(lockMetrics::startLockApiTimerContext);
    assertDoesNotThrow(lockMetrics::updateLockNotAcquiredMetric);
  }

  @Test
  public void testMetricsMisses() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/gdsafsd")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
        .forTable("idk").withPath("/dsfasdf/asdf")
        .withMetricsConfig(metricsConfig)
        .build());

    assertDoesNotThrow(lockMetrics::updateLockHeldTimerMetrics);
    assertDoesNotThrow(lockMetrics::updateLockNotAcquiredMetric);
    assertDoesNotThrow(lockMetrics::updateLockAcquiredMetric);
  }

}
