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

package org.apache.hudi.aws.sync;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sync.common.metrics.HoodieSyncMetrics;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

import static org.apache.hudi.config.GlueCatalogSyncClientConfig.RECREATE_GLUE_TABLE_ON_ERROR;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;

/**
 * Currently Experimental. Utility class that implements syncing a Hudi Table with the
 * AWS Glue Data Catalog (https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html)
 * to enable querying via Glue ETLs, Athena etc.
 * <p>
 * Extends HiveSyncTool since most logic is similar to Hive syncing,
 * expect using a different client {@link AWSGlueCatalogSyncClient} that implements
 * the necessary functionality using Glue APIs.
 *
 * @Experimental
 */
public class AwsGlueCatalogSyncTool extends HiveSyncTool {

  public AwsGlueCatalogSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  protected void initSyncClient(HiveSyncConfig hiveSyncConfig) {
    syncClient = new AWSGlueCatalogSyncClient(hiveSyncConfig);
  }

  @Override
  protected boolean shouldRecreateAndSyncTable() {
    return config.getBooleanOrDefault(RECREATE_GLUE_TABLE_ON_ERROR);
  }

  @Override
  protected void initMetrics() {
    metrics = new HoodieSyncMetrics(config, "glue");
  }

  public static void main(String[] args) {
    final HiveSyncConfig.HiveSyncConfigParams params = new HiveSyncConfig.HiveSyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
      System.exit(0);
    }
    // HiveConf needs to load fs conf to allow instantiation via AWSGlueClientFactory
    TypedProperties props = params.toProps();
    Configuration hadoopConf = FSUtils.getFs(props.getString(META_SYNC_BASE_PATH.key()), new Configuration()).getConf();
    try (AwsGlueCatalogSyncTool tool = new AwsGlueCatalogSyncTool(props, hadoopConf)) {
      tool.syncHoodieTable();
    }
  }
}
