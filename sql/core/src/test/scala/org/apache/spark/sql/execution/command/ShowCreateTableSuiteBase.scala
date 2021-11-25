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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.sources.SimpleInsertSource
import org.apache.spark.util.Utils

/**
 * This base suite contains unified tests for the `SHOW CREATE TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.ShowCreateTableSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.ShowCreateTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowCreateTableSuite`
 *     - V1 Hive External catalog:
*        `org.apache.spark.sql.hive.execution.command.ShowCreateTableSuite`
 */
trait ShowCreateTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW CREATE TABLE"

  test("SPARK-33892: SHOW CREATE TABLE w/ char/varchar") {
    val db = "ns1"
    val table = "tbl"
    withNamespaceAndTable(db, table) { t =>
      sql(s"CREATE TABLE $t(v VARCHAR(3), c CHAR(5)) $defaultUsing")
      val rest = sql(s"SHOW CREATE TABLE $t").head().getString(0)
      assert(rest.contains("VARCHAR(3)"))
      assert(rest.contains("CHAR(5)"))
    }
  }

  test("DO NOT SUPPORT TEMP VIEW") {
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

  test("SPARK-36012: ADD NULL FLAG WHEN SHOW CREATE TABLE") {
    val db = "ns1"
    val table = "tbl"
    withNamespaceAndTable(db, table) { t =>
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint NOT NULL,
           |  b bigint
           |)
           |USING ${classOf[SimpleInsertSource].getName}
        """.stripMargin)
      val showDDL = getShowCreateDDL(s"SHOW CREATE TABLE $t")
      // if v2 showDDL(0) == s"CREATE TABLE $t ("
      // if v1 showDDL(0) == s"CREATE TABLE `$db`.`$table` ("
      assert(showDDL(1) == "`a` BIGINT NOT NULL,")
      assert(showDDL(2) == "`b` BIGINT)")
      assert(showDDL(3) == s"USING ${classOf[SimpleInsertSource].getName}")
    }
  }

  test("SPARK-24911: KEEP QUOTES FOR NESTED FIELDS") {
    val db = "ns1"
    val table = "tbl"
    withNamespaceAndTable(db, table) { t =>
      sql(s"CREATE TABLE $t (`a` STRUCT<`b`: STRING>) $defaultUsing")
      val showDDL = getShowCreateDDL(s"SHOW CREATE TABLE $t")
      // if v2 showDDL(0) == s"CREATE TABLE $t ("
      // if v1 showDDL(0) == s"CREATE TABLE `$db`.`$table` ("
      assert(showDDL(1) == "`a` STRUCT<`b`: STRING>)")
      if (catalogVersion == "Hive V1") {
        assert(showDDL(2) == "USING text")
      } else {
        assert(showDDL(2) == defaultUsing)
      }
    }
  }

  test("DATA SOURCE TABLE WITH USER SPECIFIED SCHEMA") {
    val db = "ns1"
    val table = "ddl_test"
    withNamespaceAndTable(db, table) { t =>
      val jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
      sql(
        s"""CREATE TABLE $t (
           |  a STRING,
           |  b STRING,
           |  `extra col` ARRAY<INT>,
           |  `<another>` STRUCT<x: INT, y: ARRAY<BOOLEAN>>
           |)
           |USING json
           |OPTIONS (
           | PATH '$jsonFilePath'
           |)
         """.stripMargin
      )
      val showDDL = getShowCreateDDL(s"SHOW CREATE TABLE $t")
      // if v2 showDDL(0) == s"CREATE TABLE $t ("
      // if v1 showDDL(0) == s"CREATE TABLE `$db`.`$table` ("
      assert(showDDL(1) == "`a` STRING,")
      assert(showDDL(2) == "`b` STRING,")
      assert(showDDL(3) == "`extra col` ARRAY<INT>,")
      assert(showDDL(4) == "`<another>` STRUCT<`x`: INT, `y`: ARRAY<BOOLEAN>>)")
      assert(showDDL(5) == "USING json")
      // V2 showDDL(6) == LOCATION 'jsonFilePath'
      // V1 showDDL(6) == LOCATION 'file:jsonFilePath'
    }
  }

  def getShowCreateDDL(showCreateTableSql: String): Array[String] = {
    sql(showCreateTableSql).head().getString(0).split("\n").map(_.trim)
  }
}
