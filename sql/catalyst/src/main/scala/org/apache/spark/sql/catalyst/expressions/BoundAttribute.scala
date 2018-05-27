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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"input[$ordinal, ${dataType.simpleString}, $nullable]"

  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(ordinal)) {
      null
    } else {
      dataType match {
        case BooleanType => input.getBoolean(ordinal)
        case ByteType => input.getByte(ordinal)
        case ShortType => input.getShort(ordinal)
        case IntegerType | DateType => input.getInt(ordinal)
        case LongType | TimestampType => input.getLong(ordinal)
        case FloatType => input.getFloat(ordinal)
        case DoubleType => input.getDouble(ordinal)
        case StringType => input.getUTF8String(ordinal)
        case BinaryType => input.getBinary(ordinal)
        case CalendarIntervalType => input.getInterval(ordinal)
        case t: DecimalType => input.getDecimal(ordinal, t.precision, t.scale)
        case t: StructType => input.getStruct(ordinal, t.size)
        case _: ArrayType => input.getArray(ordinal)
        case _: MapType => input.getMap(ordinal)
        case _ => input.get(ordinal, dataType)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (ctx.currentVars != null && ctx.currentVars(ordinal) != null) {
      val oev = ctx.currentVars(ordinal)
      ev.isNull = oev.isNull
      ev.value = oev.value
      ev.copy(code = oev.code)
    } else {
      // 在currentVars为null的情况下，选择INPUT_ROW
      assert(ctx.INPUT_ROW != null, "INPUT_ROW and currentVars cannot both be null.")
      val javaType = ctx.javaType(dataType)
      // 返回获取input row中ordinal对应位置的value的*代码片段*
      val value = ctx.getValue(ctx.INPUT_ROW, dataType, ordinal.toString)
      if (nullable) {
        // 如果可以为null，则如果input row的ordinal处为null，则返回dataType对应的默认值。
        // 例如，boolean默认false，byte默认-1，int默认-1等等；
        ev.copy(code =
          s"""
             |boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinal);
             |$javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : ($value);
           """.stripMargin)
      } else {
        ev.copy(code = s"$javaType ${ev.value} = $value;", isNull = "false")
      }
    }
  }
}

object BindReferences extends Logging {

  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          if (allowFailures) {
            a
          } else {
            sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
          }
        } else {
          BoundReference(ordinal, a.dataType, input(ordinal).nullable)
        }
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }
}
