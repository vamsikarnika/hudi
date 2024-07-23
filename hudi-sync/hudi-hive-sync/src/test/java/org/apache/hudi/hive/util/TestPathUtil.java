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

package org.apache.hudi.hive.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPathUtil {

  @Test
  void testComparePathsWithoutScheme() {
    String path1 = "s3://test_bucket_one/table/base/path";
    String path2 = "s3a://test_bucket_two/table/base/path";
    assertFalse(PathUtil.comparePathsWithoutScheme(path1, path2), "should return false since bucket names dont match");

    path1 = "s3a://test_bucket_one/table/new_base/path";
    path2 = "s3a://test_bucket_one/table/old_base/path";
    assertFalse(PathUtil.comparePathsWithoutScheme(path1, path2), "should return false since paths don't match");

    path1 = "s3://test_bucket_one/table/base/path";
    path2 = "s3a://test_bucket_one/table/base/path";
    assertTrue(PathUtil.comparePathsWithoutScheme(path1, path2), "should return false since bucket names match without file shema");

    path1 = "s3a://test_bucket_one/table/base/path";
    path2 = "s3a://test_bucket_one/table/base/path";
    assertTrue(PathUtil.comparePathsWithoutScheme(path1, path2), "should return true since bucket names and path matches");

    path1 = "gs://test_bucket_one/table/base/path";
    path2 = "gs://test_bucket_two/table/base/path";
    assertFalse(PathUtil.comparePathsWithoutScheme(path1, path2), "should return true since bucket names and path matches");

    path1 = "gs://test_bucket_one/table/base/path";
    path2 = "gs://test_bucket_one/table/base/path";
    assertTrue(PathUtil.comparePathsWithoutScheme(path1, path2), "should return true since bucket names and path matches");
  }
}
