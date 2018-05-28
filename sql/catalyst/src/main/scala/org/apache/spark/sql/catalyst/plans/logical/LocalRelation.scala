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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.types.{StructField, StructType}

object LocalRelation {
  def apply(output: Attribute*): LocalRelation = new LocalRelation(output)

  def apply(output1: StructField, output: StructField*): LocalRelation = {
    new LocalRelation(StructType(output1 +: output).toAttributes)
  }

  def fromExternalRows(output: Seq[Attribute], data: Seq[Row]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }

  def fromProduct(output: Seq[Attribute], data: Seq[Product]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }
}

// 相当于一张本地的关系表，output是该表的schema，data即表中存储的多行数据
case class LocalRelation(output: Seq[Attribute],
                         data: Seq[InternalRow] = Nil,
                         // Indicates whether this relation has data from a streaming source.
                         override val isStreaming: Boolean = false)
  extends LeafNode with analysis.MultiInstanceRelation {

  // QUESTION：既然output是Attribute类型，Attribute又是LeafExpression类型，所以，output是没有children的。
  // 然后我们看resolved方法，需保证checkInputDataTypes().isSuccess == true即可。然后再看，Attribute也没有
  // 实现自己的checkInputDataTypes()方法。而Expression默认的实现是checkInputDataTypes().isSuccess = true。
  // 难道，这里的require检查是多余的？
  // ANSWER：不，并不是多余的！如果output是AttributeReference类型，则肯定resolved = true（AttributeReference已经
  // 指明了告诉你它引用的是哪一个字段啊，当然是resolved = true啦）。而如果output是UnresolvedAttribute，则resolved
  // 默认为false，需要先resolve它，我们才能使用它。所以对于UnresolvedAttribute，我们需要先resolve它，才能再用来构建
  // Relation（不然Relation不知道去哪里读写哪些字段啊）。output还有一种可能的类型是PrettyAttribute。（这种类型目前
  // 我还太了解，不过看起来它的resolved默认也是为true。）
  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance(): this.type = {
    LocalRelation(output.map(_.newInstance()), data, isStreaming).asInstanceOf[this.type]
  }

  override protected def stringArgs: Iterator[Any] = {
    if (data.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  // 计算该LocalRelation的可写出的字节总数
  override def computeStats(): Statistics =
    Statistics(sizeInBytes = output.map(n => BigInt(n.dataType.defaultSize)).sum * data.length)

  def toSQL(inlineTableName: String): String = {
    require(data.nonEmpty)
    val types = output.map(_.dataType)
    val rows = data.map { row =>
      val cells = row.toSeq(types).zip(types).map { case (v, tpe) => Literal(v, tpe).sql }
      cells.mkString("(", ", ", ")")
    }
    "VALUES " + rows.mkString(", ") +
      " AS " + inlineTableName +
      output.map(_.name).mkString("(", ", ", ")")
  }
}
