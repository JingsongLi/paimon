/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class KeyValueSeparatedTableTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  test(s"Paimon KeyValueSeparated: normal test") {
    val tableName = "KeyValueSeparated_normal"
    withTable(tableName) {
      spark.sql(
        s"""
           |CREATE TABLE $tableName (a INT, b INT) TBLPROPERTIES (
           |'deletion-vectors.enabled' = 'true', 'primary-key' = 'a', 'key-value.separated' = 'true', 'bucket' = '1')
           |""".stripMargin)
      spark.sql(s"INSERT INTO $tableName values (1, 11), (2, 22)")
      checkAnswer(
        spark.sql(s"SELECT * FROM $tableName ORDER BY a"),
        Row(1, 11) :: Row(2, 22) :: Nil
      )
      spark.sql(s"INSERT INTO $tableName values (1, 111), (2, 222)")
      checkAnswer(
        spark.sql(s"SELECT * FROM $tableName ORDER BY a"),
        Row(1, 111) :: Row(2, 222) :: Nil
      )
      spark.sql(s"INSERT INTO $tableName values (1, 1111), (2, 2222)")
      checkAnswer(
        spark.sql(s"SELECT * FROM $tableName ORDER BY a"),
        Row(1, 1111) :: Row(2, 2222) :: Nil
      )
    }
  }
}
