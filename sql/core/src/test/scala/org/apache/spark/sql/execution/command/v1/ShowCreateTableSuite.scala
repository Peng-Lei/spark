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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
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

  test("CATS") {
    val table = "ddl_test"
    withTable(table) {
      sql(
        s"""CREATE TABLE $table
           |USING json
           |PARTITIONED BY (c)
           |CLUSTERED BY (a) SORTED BY (b) INTO 2 BUCKETS
           |COMMENT 'This is a comment'
           |TBLPROPERTIES ('a' = '1')
           |AS SELECT 1 AS a, "foo" AS b, 2.5 AS c
         """.stripMargin
      )
      checkCreateTable(table)
    }
  }

  test("PERSISTED VIEW") {
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
        checkCreateView("v1", serde)
      }
    }
  }

  protected def checkCreateTable(table: String, serde: Boolean = false): Unit = {
    checkCreateTableOrView(TableIdentifier(table, Some("default")), "TABLE", serde)
  }

  protected def checkCreateView(table: String, serde: Boolean = false): Unit = {
    checkCreateTableOrView(TableIdentifier(table, Some("default")), "VIEW", serde)
  }

  protected def checkCreateTableOrView(
      table: TableIdentifier,
      checkType: String,
      serde: Boolean): Unit = {
    val db = table.database.getOrElse("default")
    val expected = spark.sharedState.externalCatalog.getTable(db, table.table)
    val shownDDL = if (serde) {
      sql(s"SHOW CREATE TABLE ${table.quotedString} AS SERDE").head().getString(0)
    } else {
      sql(s"SHOW CREATE TABLE ${table.quotedString}").head().getString(0)
    }
    sql(s"DROP $checkType ${table.quotedString}")
    try {
      sql(shownDDL)
      val actual = spark.sharedState.externalCatalog.getTable(db, table.table)
      checkCatalogTables(expected, actual)
    } finally {
      sql(s"DROP $checkType IF EXISTS ${table.table}")
    }
  }

  protected def checkCatalogTables(expected: CatalogTable, actual: CatalogTable): Unit = {
    assert(CatalogTable.normalize(actual) == CatalogTable.normalize(expected))
  }
}

/**
 * The class contains tests for the `SHOW CREATE TABLE` command to check V1 In-Memory
 * table catalog.
 */
class ShowCreateTableSuite extends ShowCreateTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowCreateTableSuiteBase].commandVersion

  test("SHOW CREATE TABLE") {
    val t = "tbl"
    withTable(t) {
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
        s"CREATE TABLE `default`.`$t` (",
        "`b` BIGINT,",
        "`c` BIGINT,",
        "`extraCol` ARRAY<INT>,",
        "`<another>` STRUCT<`x`: INT, `y`: ARRAY<BOOLEAN>>,",
        "`a` BIGINT NOT NULL)",
        defaultUsing,
        "OPTIONS (",
        "`from` '0',",
        "`to` '1',",
        "`via` '2')",
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
}
