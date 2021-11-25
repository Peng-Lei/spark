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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `SHOW CREATE TABLE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowCreateTableSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.ShowCreateTableSuite`
 */
trait ShowCreateTableSuiteBase extends command.ShowCreateTableSuiteBase
    with command.TestsV1AndV2Commands {
  override def fullName: String = s"`$ns`.`$table`"

  test("show create table[simple]") {
    // todo After SPARK-37517 unify the testcase both v1 and v2
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint NOT NULL,
           |  b bigint,
           |  c bigint,
           |  `extraCol` ARRAY<INT>,
           |  `<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>
           |)
           |using parquet
           |OPTIONS (
           |  from = 0,
           |  to = 1,
           |  via = 2)
           |COMMENT 'This is a comment'
           |TBLPROPERTIES ('prop1' = '1', 'prop2' = '2', 'prop3' = 3, 'prop4' = 4)
           |PARTITIONED BY (a)
           |LOCATION '/tmp'
        """.stripMargin)
      val showDDL = getShowCreateDDL(t)
      assert(showDDL === Array(
        s"CREATE TABLE $fullName (",
        "`b` BIGINT,",
        "`c` BIGINT,",
        "`extraCol` ARRAY<INT>,",
        "`<another>` STRUCT<`x`: INT, `y`: ARRAY<BOOLEAN>>,",
        "`a` BIGINT NOT NULL)",
        "USING parquet",
        "OPTIONS (",
        "'from' = '0',",
        "'to' = '1',",
        "'via' = '2')",
        "PARTITIONED BY (a)",
        "COMMENT 'This is a comment'",
        "LOCATION 'file:/tmp'",
        "TBLPROPERTIES (",
        "'prop1' = '1',",
        "'prop2' = '2',",
        "'prop3' = '3',",
        "'prop4' = '4')"
      ))
    }
  }

  test("bucketed data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( `a` INT, `b` STRING) USING json" +
        s" CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("partitioned bucketed data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""CREATE TABLE $t
           |USING json
           |PARTITIONED BY (c)
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )
      val expected = s"CREATE TABLE $fullName ( `a` INT, `b` STRING, `c` DECIMAL(2,1)) USING json" +
        s" PARTITIONED BY (c) CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS"
      assert(getShowCreateDDL(t).mkString(" ") == expected)
    }
  }

  test("temp view") {
    val viewName = "spark_28383"
    withTempView(viewName) {
      sql(s"CREATE TEMPORARY VIEW $viewName AS SELECT 1 AS a")
      val ex = intercept[AnalysisException] {
        sql(s"SHOW CREATE TABLE $viewName")
      }
      assert(ex.getMessage.contains(
        s"$viewName is a temp view. 'SHOW CREATE TABLE' expects a table or permanent view."))
    }

    withGlobalTempView(viewName) {
      sql(s"CREATE GLOBAL TEMPORARY VIEW $viewName AS SELECT 1 AS a")
      val globalTempViewDb = spark.sessionState.catalog.globalTempViewManager.database
      val ex = intercept[AnalysisException] {
        sql(s"SHOW CREATE TABLE $globalTempViewDb.$viewName")
      }
      assert(ex.getMessage.contains(
        s"$globalTempViewDb.$viewName is a temp view. " +
          "'SHOW CREATE TABLE' expects a table or permanent view."))
    }
  }

  test("view") {
    Seq(true, false).foreach { serde =>
      withView("v1") {
        sql("CREATE VIEW v1 AS SELECT 1 AS a")
        val showDDL = getShowCreateDDL("default.v1", serde)
        assert(showDDL(0) == "CREATE VIEW `default`.`v1` (")
        assert(showDDL(1) == "`a`)")
        assert(showDDL.last == "AS SELECT 1 AS a")
      }
    }
  }

  test("view with output columns") {
    Seq(true, false).foreach { serde =>
      withView("v1") {
        sql("CREATE VIEW v1 (a, b COMMENT 'b column') AS SELECT 1 AS a, 2 AS b")
        val showDDL = getShowCreateDDL("default.v1", serde)
        assert(showDDL(0) == "CREATE VIEW `default`.`v1` (")
        assert(showDDL(1) == "`a`,")
        assert(showDDL(2) == "`b` COMMENT 'b column')")
        assert(showDDL.last == "AS SELECT 1 AS a, 2 AS b")
      }
    }
  }

  test("view with table comment and properties") {
    Seq(true, false).foreach { serde =>
      withView("v1") {
        sql(
          s"""
             |CREATE VIEW v1 (
             |  c1 COMMENT 'bla',
             |  c2
             |)
             |COMMENT 'table comment'
             |TBLPROPERTIES (
             |  'prop1' = 'value1',
             |  'prop2' = 'value2'
             |)
             |AS SELECT 1 AS c1, '2' AS c2
         """.stripMargin
        )
        val expected = "CREATE VIEW `default`.`v1` ( `c1` COMMENT 'bla', `c2`)" +
          " COMMENT 'table comment'" +
          " TBLPROPERTIES ( 'prop1' = 'value1', 'prop2' = 'value2')" +
          " AS SELECT 1 AS c1, '2' AS c2"
        assert(getShowCreateDDL("default.v1", serde).mkString(" ") == expected)
      }
    }
  }

  test("show create table as serde can't work on data source table") {
    withNamespaceAndTable(ns, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  c1 STRING COMMENT 'bla',
           |  c2 STRING
           |)
           |USING orc
         """.stripMargin
      )

      val cause = intercept[AnalysisException] {
        getShowCreateDDL(t, true)
      }

      assert(cause.getMessage.contains("Use `SHOW CREATE TABLE` without `AS SERDE` instead"))
    }
  }
}

/**
 * The class contains tests for the `SHOW CREATE TABLE` command to check V1 In-Memory
 * table catalog.
 */
class ShowCreateTableSuite extends ShowCreateTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowCreateTableSuiteBase].commandVersion
}
