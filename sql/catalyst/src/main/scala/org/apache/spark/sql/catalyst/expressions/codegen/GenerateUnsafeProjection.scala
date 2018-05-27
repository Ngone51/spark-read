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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Generates a [[Projection]] that returns an [[UnsafeRow]].
 *
 * It generates the code for all the expressions, computes the total length for all the columns
 * (can be accessed via variables), and then copies the data into a scratch buffer space in the
 * form of UnsafeRow (the scratch buffer will grow as needed).
 *
 * @note The returned UnsafeRow will be pointed to a scratch buffer inside the projection.
 */
object GenerateUnsafeProjection extends CodeGenerator[Seq[Expression], UnsafeProjection] {

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case t: AtomicType => true
    case _: CalendarIntervalType => true
    case t: StructType => t.forall(field => canSupport(field.dataType))
    case t: ArrayType if canSupport(t.elementType) => true
    case MapType(kt, vt, _) if canSupport(kt) && canSupport(vt) => true
    case udt: UserDefinedType[_] => canSupport(udt.sqlType)
    case _ => false
  }

  // TODO: if the nullability of field is correct, we can use it to save null check.
  private def writeStructToBuffer(
      ctx: CodegenContext,
      input: String,
      fieldTypes: Seq[DataType],
      bufferHolder: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val fieldEvals = fieldTypes.zipWithIndex.map { case (dt, i) =>
      ExprCode("", s"$tmpInput.isNullAt($i)", ctx.getValue(tmpInput, dt, i.toString))
    }

    s"""
      final InternalRow $tmpInput = $input;
      if ($tmpInput instanceof UnsafeRow) {
        ${writeUnsafeData(ctx, s"((UnsafeRow) $tmpInput)", bufferHolder)}
      } else {
        ${writeExpressionsToBuffer(ctx, tmpInput, fieldEvals, fieldTypes, bufferHolder)}
      }
    """
  }

  private def writeExpressionsToBuffer(
      ctx: CodegenContext,
      row: String,
      inputs: Seq[ExprCode],
      inputTypes: Seq[DataType],
      bufferHolder: String,
      isTopLevel: Boolean = false): String = {
    // 生成创建UnsafeRowWriter的代码片段，可通过rowWriter来访问。
    // UnsafeRowWriter可以以UnsafeRow的格式来将数据写入BufferHolder中。
    val rowWriterClass = classOf[UnsafeRowWriter].getName
    val rowWriter = ctx.addMutableState(rowWriterClass, "rowWriter",
      v => s"$v = new $rowWriterClass($bufferHolder, ${inputs.length});")

    val resetWriter = if (isTopLevel) {
      // For top level row writer, it always writes to the beginning of the global buffer holder,
      // which means its fixed-size region always in the same position, so we don't need to call
      // `reset` to set up its fixed-size region every time.
      if (inputs.map(_.isNull).forall(_ == "false")) {
        // If all fields are not nullable, which means the null bits never changes, then we don't
        // need to clear it out every time.
        ""
      } else {
        s"$rowWriter.zeroOutNullBytes();"
      }
    } else {
      s"$rowWriter.reset();"
    }

    // 生成用于将fields（值）写入row的代码片段
    val writeFields = inputs.zip(inputTypes).zipWithIndex.map {
      case ((input, dataType), index) =>
        val dt = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType
          case other => other
        }
        // 在生成的代码片段中，声明一个tmpCursor变量，用于在写完数据之后统计写入的数据的size。
        val tmpCursor = ctx.freshName("tmpCursor")

        val setNull = dt match {
          case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
            // Can't call setNullAt() for DecimalType with precision larger than 18.
            s"$rowWriter.write($index, (Decimal) null, ${t.precision}, ${t.scale});"
          case _ => s"$rowWriter.setNullAt($index);"
        }

        val writeField = dt match {
          case t: StructType =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeStructToBuffer(ctx, input.value, t.map(_.dataType), bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case a @ ArrayType(et, _) =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeArrayToBuffer(ctx, input.value, et, bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case m @ MapType(kt, vt, _) =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeMapToBuffer(ctx, input.value, kt, vt, bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case t: DecimalType =>
            s"$rowWriter.write($index, ${input.value}, ${t.precision}, ${t.scale});"

          case NullType => ""

            // 我们先看个最简单的吧。。。
            // 注意看这里！！！在这里调用了input.value，将该filed的对应值通过writer写入了BufferHolder中。
            // 而BufferHolder又会将该value写入row中。
            // 假如我们有个case class Person(age: Int, name: String)类型，则input就对应了age字段的expression
            // 或name字段的expression通过genCode()生成的ExprCode，然后(input)调用value，则相当于访问了Person对象
            // 的age字段或name字段的值。然后，rowWriter将获取到的值写入buffer，再写入row中。最终实现了从一个Object
            // 对象（类型T）到Spark SQL内部row的序列化。
          case _ => s"$rowWriter.write($index, ${input.value});"
        }

        if (input.isNull == "false") {
          s"""
            ${input.code}
            ${writeField.trim}
          """
        } else {
          // 如果input.code的执行结果可以是为null的，则在执行完input.code之后，判断input.code的执行结果
          // 是否为null。如果执行结果为null，则我们将row的index对应的值设为null，反之，则设为该执行结果。
          // 也就是说，（继续应用上面Person的例子，假设此时的input对应name）, 如果调用person.name == null,
          // 则我们将row的index对应值设置为null；如果调用person.name == "wuyi", 则我们将对应值设置为"wuyi"。
          // （当然，string类型在spark中可能不会返回null，应该它有默认值""。我们在这里只是打个比方。）
          s"""
            ${input.code}
            if (${input.isNull}) {
              ${setNull.trim}
            } else {
              ${writeField.trim}
            }
          """
        }
    }

    val writeFieldsCode = if (isTopLevel && (row == null || ctx.currentVars != null)) {
      // TODO: support whole stage codegen
      writeFields.mkString("\n")
    } else {
      assert(row != null, "the input row name cannot be null when generating code to write it.")
      // 为了避免所有expression生成的代码片段作为单独的一个function的size超过jvm对单个function的代码
      // 片段64kb的限制，将该代码片段拆封成多个functions。
      ctx.splitExpressions(
        expressions = writeFields,
        funcName = "writeFields",
        arguments = Seq("InternalRow" -> row))
    }

    s"""
      $resetWriter
      $writeFieldsCode
    """.trim
  }

  // TODO: if the nullability of array element is correct, we can use it to save null check.
  private def writeArrayToBuffer(
      ctx: CodegenContext,
      input: String,
      elementType: DataType,
      bufferHolder: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val arrayWriterClass = classOf[UnsafeArrayWriter].getName
    val arrayWriter = ctx.addMutableState(arrayWriterClass, "arrayWriter",
      v => s"$v = new $arrayWriterClass();")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")

    val et = elementType match {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }

    val jt = ctx.javaType(et)

    val elementOrOffsetSize = et match {
      case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS => 8
      case _ if ctx.isPrimitiveType(jt) => et.defaultSize
      case _ => 8  // we need 8 bytes to store offset and length
    }

    val tmpCursor = ctx.freshName("tmpCursor")
    val element = ctx.getValue(tmpInput, et, index)
    val writeElement = et match {
      case t: StructType =>
        s"""
          final int $tmpCursor = $bufferHolder.cursor;
          ${writeStructToBuffer(ctx, element, t.map(_.dataType), bufferHolder)}
          $arrayWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
        """

      case a @ ArrayType(et, _) =>
        s"""
          final int $tmpCursor = $bufferHolder.cursor;
          ${writeArrayToBuffer(ctx, element, et, bufferHolder)}
          $arrayWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
        """

      case m @ MapType(kt, vt, _) =>
        s"""
          final int $tmpCursor = $bufferHolder.cursor;
          ${writeMapToBuffer(ctx, element, kt, vt, bufferHolder)}
          $arrayWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
        """

      case t: DecimalType =>
        s"$arrayWriter.write($index, $element, ${t.precision}, ${t.scale});"

      case NullType => ""

      case _ => s"$arrayWriter.write($index, $element);"
    }

    val primitiveTypeName = if (ctx.isPrimitiveType(jt)) ctx.primitiveTypeName(et) else ""
    s"""
      final ArrayData $tmpInput = $input;
      if ($tmpInput instanceof UnsafeArrayData) {
        ${writeUnsafeData(ctx, s"((UnsafeArrayData) $tmpInput)", bufferHolder)}
      } else {
        final int $numElements = $tmpInput.numElements();
        $arrayWriter.initialize($bufferHolder, $numElements, $elementOrOffsetSize);

        for (int $index = 0; $index < $numElements; $index++) {
          if ($tmpInput.isNullAt($index)) {
            $arrayWriter.setNull$primitiveTypeName($index);
          } else {
            $writeElement
          }
        }
      }
    """
  }

  // TODO: if the nullability of value element is correct, we can use it to save null check.
  private def writeMapToBuffer(
      ctx: CodegenContext,
      input: String,
      keyType: DataType,
      valueType: DataType,
      bufferHolder: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val tmpCursor = ctx.freshName("tmpCursor")

    // Writes out unsafe map according to the format described in `UnsafeMapData`.
    s"""
      final MapData $tmpInput = $input;
      if ($tmpInput instanceof UnsafeMapData) {
        ${writeUnsafeData(ctx, s"((UnsafeMapData) $tmpInput)", bufferHolder)}
      } else {
        // preserve 8 bytes to write the key array numBytes later.
        $bufferHolder.grow(8);
        $bufferHolder.cursor += 8;

        // Remember the current cursor so that we can write numBytes of key array later.
        final int $tmpCursor = $bufferHolder.cursor;

        ${writeArrayToBuffer(ctx, s"$tmpInput.keyArray()", keyType, bufferHolder)}
        // Write the numBytes of key array into the first 8 bytes.
        Platform.putLong($bufferHolder.buffer, $tmpCursor - 8, $bufferHolder.cursor - $tmpCursor);

        ${writeArrayToBuffer(ctx, s"$tmpInput.valueArray()", valueType, bufferHolder)}
      }
    """
  }

  /**
   * If the input is already in unsafe format, we don't need to go through all elements/fields,
   * we can directly write it.
   */
  private def writeUnsafeData(ctx: CodegenContext, input: String, bufferHolder: String) = {
    val sizeInBytes = ctx.freshName("sizeInBytes")
    s"""
      final int $sizeInBytes = $input.getSizeInBytes();
      // grow the global buffer before writing data.
      $bufferHolder.grow($sizeInBytes);
      $input.writeToMemory($bufferHolder.buffer, $bufferHolder.cursor);
      $bufferHolder.cursor += $sizeInBytes;
    """
  }

  def createCode(
      ctx: CodegenContext,
      expressions: Seq[Expression],
      useSubexprElimination: Boolean = false): ExprCode = {
    // 生成所有Expressions的ExprCode
    val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
    // 获取各个Expression的eval的返回值的数据类型
    val exprTypes = expressions.map(_.dataType)

    // 统计在所有expressions的返回结果的数据类型中可变长的filed类型的个数
    val numVarLenFields = exprTypes.count {
      case dt if UnsafeRow.isFixedLength(dt) => false
      // TODO: consider large decimal and interval type
      case _ => true
    }

    // 注意：这里返回的不是变量名“result”，而是（类似）“array[i]”（数组访问的代码片段）
    // 该代码片段用于创建UnsafeRow对象，并通过result来访问它。
    val result = ctx.addMutableState("UnsafeRow", "result",
      v => s"$v = new UnsafeRow(${expressions.length});")

    // 一个fully-qualified class name
    val holderClass = classOf[BufferHolder].getName
    // $result的值就是array[i]引用的在上面addMutableState第三个参数initFunc创建的UnsafeRow对象；
    // （不过一定要认识到，现在这里还都只是代码片段。）
    // 该代码片段用于创建BufferHolder对象，并通过holder来访问它。
    val holder = ctx.addMutableState(holderClass, "holder",
      v => s"$v = new $holderClass($result, ${numVarLenFields * 32});")

    // 如果有变长的fields的，则需要生成BufferHolder.reset的代码段
    // （现在我们当然还不知道为什么啦。。。先放在这里。。。有缘再见）
    val resetBufferHolder = if (numVarLenFields == 0) {
      ""
    } else {
      s"$holder.reset();"
    }
    // 如果有变长的fields，则需要生成"result.setTotalSize(holder.totalSize())"的代码段，
    // 其中，result即为UnsafeRow对象，holder为BufferHolder对象。
    val updateRowSize = if (numVarLenFields == 0) {
      ""
    } else {
      s"$result.setTotalSize($holder.totalSize());"
    }

    // TODO read 可能和subExprEliminationExprs有关
    // Evaluate all the subexpression.
    val evalSubexpr = ctx.subexprFunctions.mkString("\n")

    // 生成将expression写入buffer的代码片段
    val writeExpressions =
      writeExpressionsToBuffer(ctx, ctx.INPUT_ROW, exprEvals, exprTypes, holder, isTopLevel = true)

    val code =
      s"""
        $resetBufferHolder
        $evalSubexpr
        $writeExpressions
        $updateRowSize
      """
    // 这一整块代码的执行结果，通过result来访问。result是对UnsafeRow的访问。
    // 在该代码片段执行完成之后，result对应的UnsafeRow已经写入了所有fields的值。
    ExprCode(code, "false", result)
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    create(canonicalize(expressions), subexpressionEliminationEnabled)
  }

  protected def create(references: Seq[Expression]): UnsafeProjection = {
    create(references, subexpressionEliminationEnabled = false)
  }

  private def create(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    // 为该UnsafeProjection新建一个GodegenContext
    val ctx = newCodeGenContext()
    // 返回一个ExprCode，该code的*执行结果*value是一个UnsafeRow对象，且已经写入
    // 了object的所有fields的值。即完成对了一个object的序列化。
    val eval = createCode(ctx, expressions, subexpressionEliminationEnabled)

    // 注意：虽然ctx.INPUT_ROW = "i"。但它并不是一个row的index，而是它本身就是一个InternalRow！
    // SpecificUnsafeProjection会读取该INPUT_ROW中的对象，然后将该对象序列化为InternalRow
    // 1. ctx.declareMutableStates(): 获取所有需要在该类中声明的全局变量（代码片段）
    // 2. ctx.initMutableStates(): 获取初始化所有在该类中声明的全局变量（代码片段）。
    // 3. ctx.initPartition(): 暂时还没看到...
    // 4. eval.code: 执行eval.code，会完成一个object(type of T)到Spark SQL UnsafeRow的序列化过程。
    // 本质上就是将一个object的各个field的值写入到一个UnsafeRow中。
    // 5. eval.value: 用于访问eval.code的执行结果，即生成UnsafeRow对象（此时的UnsafeRow已经写入了所有fields的值）
    // 6. ctx.declareAddedFunctions()：获取所有在该类中声明的方法（代码片段）。在ctx.initMutableStates()
    // 和eval.code执行时被调用。
    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificUnsafeProjection(references);
      }

      class SpecificUnsafeProjection extends ${classOf[UnsafeProjection].getName} {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificUnsafeProjection(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        // Scala.Function1 need this
        public java.lang.Object apply(java.lang.Object row) {
          return apply((InternalRow) row);
        }

        public UnsafeRow apply(InternalRow ${ctx.INPUT_ROW}) {
          ${eval.code.trim}
          return ${eval.value};
        }

        ${ctx.declareAddedFunctions()}
      }
      """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
  }
}
