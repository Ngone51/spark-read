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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/**
 * 一个用于将LogicalPlan（逻辑计划）转化为physical plan（物理计划）的抽象类。
 * Abstract class for transforming [[LogicalPlan]]s into physical plans.
 * Child classes are responsible for specifying a list of [[GenericStrategy]] objects that
 * each of which can return a list of possible physical plan options.
 * If a given strategy is unable to plan all of the remaining operators in the tree,
 * it can call [[GenericStrategy#planLater planLater]], which returns a placeholder
 * object that will be [[collectPlaceholders collected]] and filled in
 * using other available strategies.
 *
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // Collect physical plan candidates.
    val candidates = strategies.iterator.flatMap(_(plan))

    // candidates可能包含标记为planLater的占位符，所以需要将其替换成他们的child plans
    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      // 类型：Seq[(SparkPlan, LogicalPlan)]
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // 虽然candidate用Iterator包裹了，但是此处应该只有一个candidate。之所以这样做，是因为该plan函数
        // 最终需要返回Iterator的格式。
        // 注意: placeholders是该candidate对应的所有planLater，即一个candidate可能有多个placeholders
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          // 注意：这个case其实是foldLeft里op函数的两个参数。第一个参数就是Iterator(candidate)，
          // 第二个参数是（placeholder, logicalPlan）（即placeholders foreach之后的每个元素）
          // 但是注意：真正传进op函数的第一个参数是result，而第一次传进来的result确实是Iterator(candidate)。
          // 但是由于foldLeft的迭代计算，result是会持续变化的！！！
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // 为该placeholder的logicalPlan上physical plan
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            // 注意这两个长得很像的变量在'W'之前差了一个‘s’
            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  // 即使childPlans有多个，在该替换第一次发生之后，也不会再有相同的替换发生了。因为placeholder已经
                  // 被替换掉了呀。所以，这里的map相当于只执行了一次next。（理解的对吗？？？）
                  case p if p == placeholder => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
