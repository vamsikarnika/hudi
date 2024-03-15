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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CommitMetadataUtils {

  /* In spark mor table, task retries may generate log files which are not included in write status.
   * We need to add these to CommitMetadata so that it will be synced to MDT.
   */
  public static HoodieCommitMetadata reconcileMetadataForMissingFiles(HoodieTable table, String commitActionType, String instantTime,
                                                                      HoodieCommitMetadata commitMetadata, HoodieWriteConfig config,
                                                                      HoodieEngineContext context, Configuration hadoopConf,
                                                                      String classNameForContext) throws IOException {
    if (!table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ)
        || !commitActionType.equals(HoodieActiveTimeline.DELTA_COMMIT_ACTION)) {
      return commitMetadata;
    }

    WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);
    // if there is log files in this delta commit, we search any invalid log files generated by failed spark task
    boolean hasLogFileInDeltaCommit = commitMetadata.getPartitionToWriteStats()
        .values().stream().flatMap(List::stream)
        .anyMatch(writeStat -> FSUtils.isLogFile(new Path(config.getBasePath(), writeStat.getPath()).getName()));
    if (hasLogFileInDeltaCommit) { // skip for COW table
      // get all log files generated by makers
      Set<String> allLogFilesMarkerPath = new HashSet<>(markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism()));
      Set<String> logFilesMarkerPath = new HashSet<>();
      allLogFilesMarkerPath.stream().filter(logFilePath -> !logFilePath.endsWith("cdc")).forEach(logFilesMarkerPath::add);

      // remove valid log files
      // TODO: refactor based on HoodieData
      for (Map.Entry<String, List<HoodieWriteStat>> partitionAndWriteStats : commitMetadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat hoodieWriteStat : partitionAndWriteStats.getValue()) {
          logFilesMarkerPath.remove(hoodieWriteStat.getPath());
        }
      }

      // remaining are log files generated by retried spark task, let's generate write stat for them
      if (!logFilesMarkerPath.isEmpty()) {
        SerializableConfiguration serializableConfiguration = new SerializableConfiguration(hadoopConf);
        context.setJobStatus(classNameForContext, "Preparing data for missing files to assist with generating write stats");
        // populate partition -> map (fileId -> HoodieWriteStat) // we just need one write stat per fileID to fetch some info about
        // the file slice of interest to populate WriteStat.
        HoodiePairData<String, Map<String, HoodieWriteStat>> partitionToWriteStatHoodieData = getPartitionToFileIdToFilesMap(commitMetadata, context);

        String basePathStr = config.getBasePath();
        // populate partition -> map (fileId -> List <missing log file names>)
        HoodiePairData<String, Map<String, List<String>>> partitionToMissingLogFilesHoodieData =
            getPartitionToFileIdToMissingLogFileMap(basePathStr, logFilesMarkerPath, context, config.getFileListingParallelism());

        context.setJobStatus(classNameForContext, "Generating writeStat for missing log files");

        // lets join both to generate write stats for missing log files
        List<Pair<String, List<HoodieWriteStat>>> additionalLogFileWriteStat = getWriteStatsForMissingLogFiles(partitionToWriteStatHoodieData,
            partitionToMissingLogFilesHoodieData, serializableConfiguration, basePathStr);

        for (Pair<String, List<HoodieWriteStat>> partitionDeltaStats : additionalLogFileWriteStat) {
          String partitionPath = partitionDeltaStats.getKey();
          partitionDeltaStats.getValue().forEach(ws -> commitMetadata.addWriteStat(partitionPath, ws));
        }
      }
    }
    return commitMetadata;
  }

  /**
   * Get partition path to fileId to write stat map.
   */
  private static HoodiePairData<String, Map<String, HoodieWriteStat>> getPartitionToFileIdToFilesMap(HoodieCommitMetadata commitMetadata, HoodieEngineContext context) {
    List<Map.Entry<String, List<HoodieWriteStat>>> partitionToWriteStats = new ArrayList<>(commitMetadata.getPartitionToWriteStats().entrySet());

    return context.parallelize(partitionToWriteStats)
        .mapToPair((SerializablePairFunction<Map.Entry<String, List<HoodieWriteStat>>, String, Map<String, HoodieWriteStat>>) t -> {
          Map<String, HoodieWriteStat> fileIdToWriteStat = new HashMap<>();
          t.getValue().forEach(writeStat -> {
            if (!fileIdToWriteStat.containsKey(writeStat.getFileId())) {
              fileIdToWriteStat.put(writeStat.getFileId(), writeStat);
            }
          });
          return Pair.of(t.getKey(), fileIdToWriteStat);
        });
  }

  /**
   * Get partition path to fileId to missing log file map.
   *
   * @param basePathStr        base path
   * @param logFilesMarkerPath set of log file marker paths
   * @param context            HoodieEngineContext
   * @param parallelism        parallelism
   * @return HoodiePairData of partition path to fileId to missing log file map.
   */
  private static HoodiePairData<String, Map<String, List<String>>> getPartitionToFileIdToMissingLogFileMap(String basePathStr, Set<String> logFilesMarkerPath, HoodieEngineContext context,
                                                                                                           int parallelism) {
    List<String> logFilePaths = new ArrayList<>(logFilesMarkerPath);
    HoodiePairData<String, List<String>> partitionPathLogFilePair = context.parallelize(logFilePaths).mapToPair(logFilePath -> {
      Path logFileFullPath = new Path(basePathStr, logFilePath);
      String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePathStr), logFileFullPath.getParent());
      return Pair.of(partitionPath, Collections.singletonList(logFileFullPath.getName()));
    });
    HoodiePairData<String, Map<String, List<String>>> partitionPathToFileIdAndLogFileList = partitionPathLogFilePair
        // reduce by partition paths
        .reduceByKey((SerializableBiFunction<List<String>, List<String>, List<String>>) (strings, strings2) -> {
          List<String> logFilePaths1 = new ArrayList<>(strings);
          logFilePaths1.addAll(strings2);
          return logFilePaths1;
        }, parallelism).mapToPair((SerializablePairFunction<Pair<String, List<String>>, String, Map<String, List<String>>>) t -> {
          // for each hudi partition, collect list of missing log files, fetch file size using file system calls, and populate fileId -> List<FileStatus> map

          String partitionPath = t.getKey();
          Path fullPartitionPath = StringUtils.isNullOrEmpty(partitionPath) ? new Path(basePathStr) : new Path(basePathStr, partitionPath);
          // fetch file sizes from FileSystem
          List<String> missingLogFiles = t.getValue();
          Map<String, List<String>> fileIdtologFiles = new HashMap<>();
          missingLogFiles.forEach(logFile -> {
            String fileId = FSUtils.getFileIdFromLogPath(new Path(fullPartitionPath, logFile));
            if (!fileIdtologFiles.containsKey(fileId)) {
              fileIdtologFiles.put(fileId, new ArrayList<>());
            }
            fileIdtologFiles.get(fileId).add(logFile);
          });
          return Pair.of(partitionPath, fileIdtologFiles);
        });
    return partitionPathToFileIdAndLogFileList;
  }

  /**
   * Generate write stats for missing log files. Performs an inner join on partition between existing
   * partitionToWriteStatHoodieData and partitionToMissingLogFilesHoodieData.
   * For missing log files, it does one file system call to fetch file size (FSUtils#getFileStatusesUnderPartition).
   */
  private static List<Pair<String, List<HoodieWriteStat>>> getWriteStatsForMissingLogFiles(HoodiePairData<String, Map<String, HoodieWriteStat>> partitionToWriteStatHoodieData,
                                                                                           HoodiePairData<String, Map<String, List<String>>> partitionToMissingLogFilesHoodieData,
                                                                                           SerializableConfiguration serializableConfiguration,
                                                                                           String basePathStr) {
    // lets join both to generate write stats for missing log files
    return partitionToWriteStatHoodieData
        .join(partitionToMissingLogFilesHoodieData)
        .map((SerializableFunction<Pair<String, Pair<Map<String, HoodieWriteStat>, Map<String, List<String>>>>, Pair<String, List<HoodieWriteStat>>>) v1 -> {
          final Path basePathLocal = new Path(basePathStr);
          String partitionPath = v1.getKey();
          Map<String, HoodieWriteStat> fileIdToOriginalWriteStat = v1.getValue().getKey();
          Map<String, List<String>> missingFileIdToLogFileNames = v1.getValue().getValue();
          List<String> missingLogFileNames = missingFileIdToLogFileNames.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toList());

          // fetch file sizes from FileSystem
          Path fullPartitionPath = StringUtils.isNullOrEmpty(partitionPath) ? new Path(basePathStr) : new Path(basePathStr, partitionPath);
          FileSystem fileSystem = fullPartitionPath.getFileSystem(serializableConfiguration.get());
          List<Option<FileStatus>> fileStatuesOpt = FSUtils.getFileStatusesUnderPartition(fileSystem, fullPartitionPath, new HashSet<>(missingLogFileNames), true);
          List<FileStatus> fileStatuses = fileStatuesOpt.stream().filter(fileStatusOpt -> fileStatusOpt.isPresent()).map(fileStatusOption -> fileStatusOption.get()).collect(Collectors.toList());

          // populate fileId -> List<FileStatus>
          Map<String, List<FileStatus>> missingFileIdToLogFilesList = new HashMap<>();
          fileStatuses.forEach(fileStatus -> {
            String fileId = FSUtils.getFileIdFromLogPath(fileStatus.getPath());
            missingFileIdToLogFilesList.putIfAbsent(fileId, new ArrayList<>());
            missingFileIdToLogFilesList.get(fileId).add(fileStatus);
          });

          List<HoodieWriteStat> missingWriteStats = new ArrayList();
          missingFileIdToLogFilesList.forEach((k, logFileStatuses) -> {
            String fileId = k;
            HoodieDeltaWriteStat originalWriteStat =
                (HoodieDeltaWriteStat) fileIdToOriginalWriteStat.get(fileId); // are there chances that there won't be any write stat in original list?
            logFileStatuses.forEach(fileStatus -> {
              // for every missing file, add a new HoodieDeltaWriteStat
              HoodieDeltaWriteStat writeStat = getHoodieDeltaWriteStatFromPreviousStat(fileStatus, basePathLocal,
                  partitionPath, fileId, originalWriteStat);
              missingWriteStats.add(writeStat);
            });
          });
          return Pair.of(partitionPath, missingWriteStats);
        }).collectAsList();
  }

  private static HoodieDeltaWriteStat getHoodieDeltaWriteStatFromPreviousStat(FileStatus fileStatus,
                                                                              Path basePathLocal,
                                                                              String partitionPath,
                                                                              String fileId,
                                                                              HoodieDeltaWriteStat originalWriteStat) {
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    HoodieLogFile logFile = new HoodieLogFile(fileStatus);
    writeStat.setPath(basePathLocal, logFile.getPath());
    writeStat.setPartitionPath(partitionPath);
    writeStat.setFileId(fileId);
    writeStat.setTotalWriteBytes(logFile.getFileSize());
    writeStat.setFileSizeInBytes(logFile.getFileSize());
    writeStat.setLogVersion(logFile.getLogVersion());
    List<String> logFiles = new ArrayList<>(originalWriteStat.getLogFiles());
    logFiles.add(logFile.getFileName());
    writeStat.setLogFiles(logFiles);
    writeStat.setBaseFile(originalWriteStat.getBaseFile());
    writeStat.setPrevCommit(logFile.getBaseCommitTime());
    return writeStat;
  }
}