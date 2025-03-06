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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.util.SchemaUtils.restoreOriginalOutputNames

object V2PruneColumnAfterRewriteSubquery extends Rule[LogicalPlan] with Logging {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case ScanOperation(project, filtersStayUp, filtersPushDown, dssr: DataSourceV2ScanRelation)
        if needPrune(project, filtersStayUp, filtersPushDown, dssr) =>
        val r = dssr.relation;
        val sHolder = ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
        val normalizedProjects = DataSourceStrategy
          .normalizeExprs(project, sHolder.output)
          .asInstanceOf[Seq[NamedExpression]]
        val allFilters = filtersPushDown.reduceOption(And).toSeq ++ filtersStayUp

        val normalizedFilters = DataSourceStrategy.normalizeExprs(allFilters, sHolder.output)
        val (scan, output) = PushDownUtils.pruneColumns(
          sHolder.builder, sHolder.relation, normalizedProjects, normalizedFilters)

        val scanRelation = DataSourceV2ScanRelation(sHolder.relation, scan, output)

        val projectionOverSchema =
          ProjectionOverSchema(output.toStructType, AttributeSet(output))
        val projectionFunc = (expr: Expression) => expr transformDown {
          case projectionOverSchema(newExpr) => newExpr
        }

        val finalFilters = normalizedFilters.map(projectionFunc)
        val withFilter = finalFilters.foldLeft[LogicalPlan](scanRelation)((plan, cond) => {
          Filter(cond, plan)
        })

        if (withFilter.output != project) {
          val newProjects = normalizedProjects
            .map(projectionFunc)
            .asInstanceOf[Seq[NamedExpression]]
          Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
        } else {
          withFilter
        }
    }
  }

  private def needPrune(
      projects: Seq[NamedExpression],
      filtersStayUp: Seq[Expression],
      filtersPushDown: Seq[Expression],
      dssr: DataSourceV2ScanRelation): Boolean = {
    val allFilters = filtersPushDown.reduceOption(And).toSeq ++ filtersStayUp
    val exprs = projects ++ allFilters
    val requiredColumns = AttributeSet(exprs.flatMap(_.references))
    !AttributeSet(dssr.output).equals(requiredColumns)
  }
}
