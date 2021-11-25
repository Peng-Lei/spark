/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `SHOW CREATE TABLE` command to check V2 table catalogs.
 */
class ShowCreateTableSuite extends command.ShowCreateTableSuiteBase with CommandSuiteBase {
  test("SPARK-33898: SHOW CREATE TABLE AS SERDE") {
    val db = "ns1"
    val table = "tbl"
    withNamespaceAndTable(db, table) { t =>
      spark.sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      val e = intercept[AnalysisException] {
        sql(s"SHOW CREATE TABLE $t AS SERDE")
      }
      assert(e.message.contains(s"SHOW CREATE TABLE AS SERDE is not supported for v2 tables."))
    }
  }

  test("CTAS") {
    val db = "ns1"
    val table = "ddl_test"
    withNamespaceAndTable(db, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |$defaultUsing
           |PARTITIONED BY (a)
           |COMMENT 'This is a comment'
           |TBLPROPERTIES ('a' = '1')
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )
      val showDDL = getShowCreateDDL(s"SHOW CREATE TABLE $t")
      assert(showDDL === Array(
        s"CREATE TABLE $t (",
        "`a` INT,",
        "`b` STRING)",
        defaultUsing,
        "PARTITIONED BY (a)",
        "COMMENT 'This is a comment'",
        "TBLPROPERTIES(",
        "'a' = '1')"
      ))
    }
  }

  test("SPARK-33898: SHOW CREATE TABLE") {
    val db = "ns1"
    val table = "tbl"
    withNamespaceAndTable(db, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint NOT NULL,
           |  b bigint,
           |  c bigint,
           |  `extraCol` ARRAY<INT>,
           |  `<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>
           |)
           |$defaultUsing
           |OPTIONS (
           |  from = 0,
           |  to = 1,
           |  via = 2)
           |COMMENT 'This is a comment'
           |TBLPROPERTIES ('prop1' = '1', 'prop2' = '2', 'prop3' = 3, 'prop4' = 4)
           |PARTITIONED BY (a)
           |LOCATION '/tmp'
        """.stripMargin)
      val showDDL = getShowCreateDDL(s"SHOW CREATE TABLE $t")
      assert(showDDL === Array(
        s"CREATE TABLE $t (",
        "`a` BIGINT NOT NULL,",
        "`b` BIGINT,",
        "`c` BIGINT,",
        "`extraCol` ARRAY<INT>,",
        "`<another>` STRUCT<`x`: INT, `y`: ARRAY<BOOLEAN>>)",
        defaultUsing,
        "OPTIONS(",
        "'from' = '0',",
        "'to' = '1',",
        "'via' = '2')",
        "PARTITIONED BY (a)",
        "COMMENT 'This is a comment'",
        "LOCATION '/tmp'",
        "TBLPROPERTIES(",
        "'prop1' = '1',",
        "'prop2' = '2',",
        "'prop3' = '3',",
        "'prop4' = '4')"
      ))
    }
  }

  test("SPARK-33898: SHOW CREATE TABLE PARTITIONED BY TRANSFORMS") {
    val db = "ns1"
    val table = "tbl"
    withNamespaceAndTable(db, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (a INT, b STRING, ts TIMESTAMP) $defaultUsing
           |PARTITIONED BY (
           |    a,
           |    bucket(16, b),
           |    years(ts),
           |    months(ts),
           |    days(ts),
           |    hours(ts))
         """.stripMargin)
      // V1 transforms cannot be converted to partition columns: bucket,year,...)
      val showDDL = getShowCreateDDL(s"SHOW CREATE TABLE $t")
      assert(showDDL === Array(
        s"CREATE TABLE $t (",
        "`a` INT,",
        "`b` STRING,",
        "`ts` TIMESTAMP)",
        defaultUsing,
        "PARTITIONED BY (a, bucket(16, b), years(ts), months(ts), days(ts), hours(ts))"
      ))
    }
  }
}
