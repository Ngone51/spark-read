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

import java.io.ByteArrayInputStream
import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.{ByteArrayClassLoader, ClassBodyEvaluator, InternalCompilerException, SimpleCompiler}
import org.codehaus.janino.util.ClassFile

import org.apache.spark.{SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types._
import org.apache.spark.util.{ParentClassLoader, Utils}

/**
 * Java source for evaluating an [[Expression]] given a [[InternalRow]] of input.
 *
 * @param code The sequence of statements required to evaluate the expression.
 *             It should be empty string, if `isNull` and `value` are already existed, or no code
 *             needed to evaluate them (literals).
 * @param isNull A term that holds a boolean value representing whether the expression evaluated
 *                 to null.
 * @param value A term for a (possibly primitive) value of the result of the evaluation. Not
 *              valid if `isNull` is set to `true`.
 */
case class ExprCode(var code: String, var isNull: String, var value: String)

/**
 * State used for subexpression elimination.
 *
 * @param isNull A term that holds a boolean value representing whether the expression evaluated
 *               to null.
 * @param value A term for a value of a common sub-expression. Not valid if `isNull`
 *              is set to `true`.
 */
case class SubExprEliminationState(isNull: String, value: String)

/**
 * Codes and common subexpressions mapping used for subexpression elimination.
 *
 * @param codes Strings representing the codes that evaluate common subexpressions.
 * @param states Foreach expression that is participating in subexpression elimination,
 *               the state to use.
 */
case class SubExprCodes(codes: Seq[String], states: Map[Expression, SubExprEliminationState])

/**
 * The main information about a new added function.
 *
 * @param functionName String representing the name of the function
 * @param innerClassName Optional value which is empty if the function is added to
 *                       the outer class, otherwise it contains the name of the
 *                       inner class in which the function has been added.
 * @param innerClassInstance Optional value which is empty if the function is added to
 *                           the outer class, otherwise it contains the name of the
 *                           instance of the inner class in the outer class.
 */
private[codegen] case class NewFunctionSpec(
    functionName: String,
    innerClassName: Option[String],
    innerClassInstance: Option[String])

/**
 * A context for codegen, tracking a list of objects that could be passed into generated Java
 * function.
 */
class CodegenContext {

  /**
   * Holding a list of objects that could be used passed into generated class.
   */
  val references: mutable.ArrayBuffer[Any] = new mutable.ArrayBuffer[Any]()

  /**
   * Add an object to `references`.
   *
   * Returns the code to access it.
   *
   * This does not to store the object into field but refer it from the references field at the
   * time of use because number of fields in class is limited so we should reduce it.
   */
  def addReferenceObj(objName: String, obj: Any, className: String = null): String = {
    val idx = references.length
    references += obj
    val clsName = Option(className).getOrElse(obj.getClass.getName)
    // 返回一个代码片段
    // ($clsName) 相当于是对reference[$idx]的类型强制转化。比如，obj为int类型，
    // 则生成的代码片段为：(int) references[idx] /* int */
    s"(($clsName) references[$idx] /* $objName */)"
  }

  /**
   * Holding the variable name of the input row of the current operator, will be used by
   * `BoundReference` to generate code.
   *
   * Note that if `currentVars` is not null, `BoundReference` prefers `currentVars` over `INPUT_ROW`
   * to generate code. If you want to make sure the generated code use `INPUT_ROW`, you need to set
   * `currentVars` to null, or set `currentVars(i)` to null for certain columns, before calling
   * `Expression.genCode`.
   */
  var INPUT_ROW = "i"

  /**
   * Holding a list of generated columns as input of current operator, will be used by
   * BoundReference to generate code.
   */
  var currentVars: Seq[ExprCode] = null

  /**
   * Holding expressions' inlined mutable states like `MonotonicallyIncreasingID.count` as a
   * 2-tuple: java type, variable name.
   * As an example, ("int", "count") will produce code:
   * {{{
   *   private int count;
   * }}}
   * as a member variable
   *
   * They will be kept as member variables in generated classes like `SpecificProjection`.
   *
   * Exposed for tests only.
   */
  private[catalyst] val inlinedMutableStates: mutable.ArrayBuffer[(String, String)] =
    mutable.ArrayBuffer.empty[(String, String)]

  /**
   * The mapping between mutable state types and corrseponding compacted arrays.
   * The keys are java type string. The values are [[MutableStateArrays]] which encapsulates
   * the compacted arrays for the mutable states with the same java type.
   *
   * Exposed for tests only.
   */
  private[catalyst] val arrayCompactedMutableStates: mutable.Map[String, MutableStateArrays] =
    mutable.Map.empty[String, MutableStateArrays]

  // An array holds the code that will initialize each state
  // Exposed for tests only.
  private[catalyst] val mutableStateInitCode: mutable.ArrayBuffer[String] =
    mutable.ArrayBuffer.empty[String]

  // Tracks the names of all the mutable states.
  private val mutableStateNames: mutable.HashSet[String] = mutable.HashSet.empty

  /**
   * This class holds a set of names of mutableStateArrays that is used for compacting mutable
   * states for a certain type, and holds the next available slot of the current compacted array.
   */
  class MutableStateArrays {
    val arrayNames = mutable.ListBuffer.empty[String]
    createNewArray()

    private[this] var currentIndex = 0

    private def createNewArray() = {
      val newArrayName = freshName("mutableStateArray")
      mutableStateNames += newArrayName
      arrayNames.append(newArrayName)
    }

    def getCurrentIndex: Int = currentIndex

    /**
     * Returns the reference of next available slot in current compacted array. The size of each
     * compacted array is controlled by the constant `CodeGenerator.MUTABLESTATEARRAY_SIZE_LIMIT`.
     * Once reaching the threshold, new compacted array is created.
     */
    def getNextSlot(): String = {
      if (currentIndex < CodeGenerator.MUTABLESTATEARRAY_SIZE_LIMIT) {
        // 返回一个（在java中）获取数组对应下标的值的代码片段，即arrayName[index]
        val res = s"${arrayNames.last}[$currentIndex]"
        currentIndex += 1
        res
      } else {
        createNewArray()
        currentIndex = 1
        s"${arrayNames.last}[0]"
      }
    }

  }

  /**
   * A map containing the mutable states which have been defined so far using
   * `addImmutableStateIfNotExists`. Each entry contains the name of the mutable state as key and
   * its Java type and init code as value.
   */
  private val immutableStates: mutable.Map[String, (String, String)] =
    mutable.Map.empty[String, (String, String)]

  /**
   * Add a mutable state as a field to the generated class. c.f. the comments above.
   *
   * @param javaType Java type of the field. Note that short names can be used for some types,
   *                 e.g. InternalRow, UnsafeRow, UnsafeArrayData, etc. Other types will have to
   *                 specify the fully-qualified Java type name. See the code in doCompile() for
   *                 the list of default imports available.
   *                 Also, generic type arguments are accepted but ignored.
   * @param variableName Name of the field.
   * @param initFunc Function includes statement(s) to put into the init() method to initialize
   *                 this field. The argument is the name of the mutable state variable.
   *                 If left blank, the field will be default-initialized.
   * @param forceInline whether the declaration and initialization code may be inlined rather than
   *                    compacted. Please set `true` into forceInline for one of the followings:
   *                    1. use the original name of the status
   *                    2. expect to non-frequently generate the status
   *                       (e.g. not much sort operators in one stage)
   * @param useFreshName If this is false and the mutable state ends up inlining in the outer
   *                     class, the name is not changed
   * @return the name of the mutable state variable, which is the original name or fresh name if
   *         the variable is inlined to the outer class, or an array access if the variable is to
   *         be stored in an array of variables of the same type.
   *         A variable will be inlined into the outer class when one of the following conditions
   *         are satisfied:
   *         1. forceInline is true
   *         2. its type is primitive type and the total number of the inlined mutable variables
   *            is less than `CodeGenerator.OUTER_CLASS_VARIABLES_THRESHOLD`
   *         3. its type is multi-dimensional array
   *         When a variable is compacted into an array, the max size of the array for compaction
   *         is given by `CodeGenerator.MUTABLESTATEARRAY_SIZE_LIMIT`.
   */
  def addMutableState(
      javaType: String,
      variableName: String,
      initFunc: String => String = _ => "",
      forceInline: Boolean = false,
      useFreshName: Boolean = true): String = {

    // want to put a primitive type variable at outerClass for performance
    val canInlinePrimitive = isPrimitiveType(javaType) &&
      (inlinedMutableStates.length < CodeGenerator.OUTER_CLASS_VARIABLES_THRESHOLD)
    // 如果是forceInline（true）或者是可以inline的基础类型或者是多维（大于1）的数组类型
    if (forceInline || canInlinePrimitive || javaType.contains("[][]")) {
      val varName = if (useFreshName) freshName(variableName) else variableName
      val initCode = initFunc(varName)
      inlinedMutableStates += ((javaType, varName))
      mutableStateInitCode += initCode
      mutableStateNames += varName
      varName
    } else {
      // 不能inline，则使用compact mutable states
      val arrays = arrayCompactedMutableStates.getOrElseUpdate(javaType, new MutableStateArrays)
      // element：array[i]，数组的访问方式
      val element = arrays.getNextSlot()

      // 把initFunc的结果（代码片段）存储在element中
      val initCode = initFunc(element)
      mutableStateInitCode += initCode
      element
    }
  }

  /**
   * Add an immutable state as a field to the generated class only if it does not exist yet a field
   * with that name. This helps reducing the number of the generated class' fields, since the same
   * variable can be reused by many functions.
   *
   * Even though the added variables are not declared as final, they should never be reassigned in
   * the generated code to prevent errors and unexpected behaviors.
   *
   * Internally, this method calls `addMutableState`.
   *
   * @param javaType Java type of the field.
   * @param variableName Name of the field.
   * @param initFunc Function includes statement(s) to put into the init() method to initialize
   *                 this field. The argument is the name of the mutable state variable.
   */
  def addImmutableStateIfNotExists(
      javaType: String,
      variableName: String,
      initFunc: String => String = _ => ""): Unit = {
    val existingImmutableState = immutableStates.get(variableName)
    if (existingImmutableState.isEmpty) {
      // 注意：useFreshName = false，forceInline = true
      addMutableState(javaType, variableName, initFunc, useFreshName = false, forceInline = true)
      immutableStates(variableName) = (javaType, initFunc(variableName))
    } else {
      // 该变量已经存在
      val (prevJavaType, prevInitCode) = existingImmutableState.get
      assert(prevJavaType == javaType, s"$variableName has already been defined with type " +
        s"$prevJavaType and now it is tried to define again with type $javaType.")
      assert(prevInitCode == initFunc(variableName), s"$variableName has already been defined " +
        s"with different initialization statements.")
    }
  }

  /**
   * Add buffer variable which stores data coming from an [[InternalRow]]. This methods guarantees
   * that the variable is safely stored, which is important for (potentially) byte array backed
   * data types like: UTF8String, ArrayData, MapData & InternalRow.
   */
  def addBufferedState(dataType: DataType, variableName: String, initCode: String): ExprCode = {
    // value是返回的变量名称
    val value = addMutableState(javaType(dataType), variableName)
    val code = dataType match {
      case StringType => s"$value = $initCode.clone();"
      case _: StructType | _: ArrayType | _: MapType => s"$value = $initCode.copy();"
      case _ => s"$value = $initCode;"
    }
    ExprCode(code, "false", value)
  }

  def declareMutableStates(): String = {
    // It's possible that we add same mutable state twice, e.g. the `mergeExpressions` in
    // `TypedAggregateExpression`, we should call `distinct` here to remove the duplicated ones.
    val inlinedStates = inlinedMutableStates.distinct.map { case (javaType, variableName) =>
      s"private $javaType $variableName;"
    }

    val arrayStates = arrayCompactedMutableStates.flatMap { case (javaType, mutableStateArrays) =>
      val numArrays = mutableStateArrays.arrayNames.size
      mutableStateArrays.arrayNames.zipWithIndex.map { case (arrayName, index) =>
        val length = if (index + 1 == numArrays) {
          // 说明mutableStateArrays中只有一个array，那么，currentIndex就代表了该array的length。
          mutableStateArrays.getCurrentIndex
        } else {
          // 反之，说明mutableStateArrays中有多个arrays，则每个array（除了最后一个新申请的array）的
          // length为MUTABLESTATEARRAY_SIZE_LIMIT
          CodeGenerator.MUTABLESTATEARRAY_SIZE_LIMIT
        }
        // 既然我们已经在arrayCompactedMutableStates找states，则javaType不可能是多维数组（"[][]..."），
        // 因为多维数组肯定是在inlinedMutableStates中的。
        if (javaType.contains("[]")) {
          // initializer had an one-dimensional array variable
          // 例如，javaType = "int[]"，则baseType = "int"
          val baseType = javaType.substring(0, javaType.length - 2)
          // 注意，这里创建的是二维（"[][]"）数组。以javaType = "int[]"为例，相当于在
          // mutableStateArrays.array中存储了length个"int[]"。所以，如果我们在一个类中声明
          // 该length个"int[]"变量的话，直接声明一个二维数组:
          // private int[] arrayName = new int[length][] 即可。
          s"private $javaType[] $arrayName = new $baseType[$length][];"
        } else {
          // 和上面一样，在这里也需要声明该变量的一个一维数组即可。
          // initializer had a scalar variable
          s"private $javaType[] $arrayName = new $javaType[$length];"
        }
      }
    }

    // 返回所有需要在该class（outer ？inner ？）中声明的全局（？）变量。
    (inlinedStates ++ arrayStates).mkString("\n")
  }

  def initMutableStates(): String = {
    // It's possible that we add same mutable state twice, e.g. the `mergeExpressions` in
    // `TypedAggregateExpression`, we should call `distinct` here to remove the duplicated ones.
    val initCodes = mutableStateInitCode.distinct.map(_ + "\n")

    // The generated initialization code may exceed 64kb function size limit in JVM if there are too
    // many mutable states, so split it into multiple functions.
    splitExpressions(expressions = initCodes, funcName = "init", arguments = Nil)
  }

  /**
   * Code statements to initialize states that depend on the partition index.
   * An integer `partitionIndex` will be made available within the scope.
   */
  val partitionInitializationStatements: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty

  def addPartitionInitializationStatement(statement: String): Unit = {
    partitionInitializationStatements += statement
  }

  def initPartition(): String = {
    partitionInitializationStatements.mkString("\n")
  }

  /**
   * Holds expressions that are equivalent. Used to perform subexpression elimination
   * during codegen.
   *
   * For expressions that appear more than once, generate additional code to prevent
   * recomputing the value.
   *
   * For example, consider two expression generated from this SQL statement:
   *  SELECT (col1 + col2), (col1 + col2) / col3.
   *
   *  equivalentExpressions will match the tree containing `col1 + col2` and it will only
   *  be evaluated once.
   */
  val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

  // Foreach expression that is participating in subexpression elimination, the state to use.
  val subExprEliminationExprs = mutable.HashMap.empty[Expression, SubExprEliminationState]

  // The collection of sub-expression result resetting methods that need to be called on each row.
  val subexprFunctions = mutable.ArrayBuffer.empty[String]

  val outerClassName = "OuterClass"

  /**
   * (String, String) -> (类类型名，类实例名) e.g. (NestedClass, nestedClassInstance)
   * Holds the class and instance names to be generated, where `OuterClass` is a placeholder
   * standing for whichever class is generated as the outermost class and which will contain any
   * inner sub-classes. All other classes and instance names in this list will represent private,
   * inner sub-classes.
   */
  private val classes: mutable.ListBuffer[(String, String)] =
    mutable.ListBuffer[(String, String)](outerClassName -> null)

  // A map holding the current size in bytes of each class to be generated.
  private val classSize: mutable.Map[String, Int] =
    mutable.Map[String, Int](outerClassName -> 0)

  // Nested maps holding function names and their code belonging to each class.
  private val classFunctions: mutable.Map[String, mutable.Map[String, String]] =
    mutable.Map(outerClassName -> mutable.Map.empty[String, String])

  // Verbatim extra code to be added to the OuterClass.
  private val extraClasses: mutable.ListBuffer[String] = mutable.ListBuffer[String]()

  // Returns the size of the most recently added class.
  private def currClassSize(): Int = classSize(classes.head._1)

  // Returns the class name and instance name for the most recently added class.
  private def currClass(): (String, String) = classes.head

  // Adds a new class. Requires the class' name, and its instance name.
  private def addClass(className: String, classInstance: String): Unit = {
    // 插入到队首
    classes.prepend(className -> classInstance)
    classSize += className -> 0
    classFunctions += className -> mutable.Map.empty[String, String]
  }

  /**
   * Adds a function to the generated class. If the code for the `OuterClass` grows too large, the
   * function will be inlined into a new private, inner class, and a class-qualified name for the
   * function will be returned. Otherwise, the function will be inlined to the `OuterClass` the
   * simple `funcName` will be returned.
   *
   * @param funcName the class-unqualified name of the function
   * @param funcCode the body of the function
   * @param inlineToOuterClass whether the given code must be inlined to the `OuterClass`. This
   *                           can be necessary when a function is declared outside of the context
   *                           it is eventually referenced and a returned qualified function name
   *                           cannot otherwise be accessed.
   * @return the name of the function, qualified by class if it will be inlined to a private,
   *         inner class
   */
  def addNewFunction(
      funcName: String,
      funcCode: String,
      inlineToOuterClass: Boolean = false): String = {
    val newFunction = addNewFunctionInternal(funcName, funcCode, inlineToOuterClass)
    newFunction match {
      case NewFunctionSpec(functionName, None, None) => functionName
      case NewFunctionSpec(functionName, Some(_), Some(innerClassInstance)) =>
        innerClassInstance + "." + functionName
    }
  }

  private[this] def addNewFunctionInternal(
      funcName: String,
      funcCode: String,
      inlineToOuterClass: Boolean): NewFunctionSpec = {
    val (className, classInstance) = if (inlineToOuterClass) {
      // 如果inlineToOuterClass = true，则将该func添加到outerClass中
      outerClassName -> ""
    } else if (currClassSize > CodeGenerator.GENERATED_CLASS_SIZE_THRESHOLD) {
      // 如果当前class（最近一个在outerClass中创建的私有内部类）的size超过了threshold，
      // 则新创建一个内部类（以避免超过jvm的常量池的限制）
      val className = freshName("NestedClass")
      val classInstance = freshName("nestedClassInstance")

      // 添加该新增内部类，更新相关数据结构
      addClass(className, classInstance)

      className -> classInstance
    } else {
      // 如果没有超过阈值，说明还能继续向该class（最近一个在outerClass中创建的私有内部类）中添加func
      currClass()
    }

    // 向className对应的class，添加该func（更新相关数据结构）
    addNewFunctionToClass(funcName, funcCode, className)

    if (className == outerClassName) {
      // 说明该func添加到了outerClass中，即作为outerClass的inline function
      NewFunctionSpec(funcName, None, None)
    } else {
      // 说明该func添加到了outerClass的某个私有内部类中
      NewFunctionSpec(funcName, Some(className), Some(classInstance))
    }
  }

  private[this] def addNewFunctionToClass(
      funcName: String,
      funcCode: String,
      className: String) = {
    classSize(className) += funcCode.length
    classFunctions(className) += funcName -> funcCode
  }

  /**
   * Declares all function code. If the added functions are too many, split them into nested
   * sub-classes to avoid hitting Java compiler constant pool limitation.
   */
  def declareAddedFunctions(): String = {
    val inlinedFunctions = classFunctions(outerClassName).values

    // 私有的内部类没有mutable state（虽然它们引用了在outer class中声明的mutable state（声明的全局变量）），
    // 因此，我们在outer class里以inline的形式声明并初始化这些内部类。
    // Nested, private sub-classes have no mutable state (though they do reference the outer class'
    // mutable state), so we declare and initialize them inline to the OuterClass.
    val initNestedClasses = classes.filter(_._1 != outerClassName).map {
      case (className, classInstance) =>
        s"private $className $classInstance = new $className();"
    }

    // 生成各个内部类的定义或声明（只有成员方法，没有成员变量）的代码片段
    val declareNestedClasses = classFunctions.filterKeys(_ != outerClassName).map {
      case (className, functions) =>
        s"""
           |private class $className {
           |  ${functions.values.mkString("\n")}
           |}
           """.stripMargin
    }

    (inlinedFunctions ++ initNestedClasses ++ declareNestedClasses).mkString("\n")
  }

  /**
   * Emits extra inner classes added with addExtraCode
   */
  def emitExtraCode(): String = {
    extraClasses.mkString("\n")
  }

  /**
   * Add extra source code to the outermost generated class.
   * @param code verbatim source code of the inner class to be added.
   */
  def addInnerClass(code: String): Unit = {
    extraClasses.append(code)
  }

  final val JAVA_BOOLEAN = "boolean"
  final val JAVA_BYTE = "byte"
  final val JAVA_SHORT = "short"
  final val JAVA_INT = "int"
  final val JAVA_LONG = "long"
  final val JAVA_FLOAT = "float"
  final val JAVA_DOUBLE = "double"

  /**
   * The map from a variable name to it's next ID.
   */
  private val freshNameIds = new mutable.HashMap[String, Int]
  freshNameIds += INPUT_ROW -> 1

  /**
   * A prefix used to generate fresh name.
   */
  var freshNamePrefix = ""

  /**
   * The map from a place holder to a corresponding comment
   */
  private val placeHolderToComments = new mutable.HashMap[String, String]

  /**
   * Returns a term name that is unique within this instance of a `CodegenContext`.
   */
  def freshName(name: String): String = synchronized {
    val fullName = if (freshNamePrefix == "") {
      name
    } else {
      s"${freshNamePrefix}_$name"
    }
    if (freshNameIds.contains(fullName)) {
      val id = freshNameIds(fullName)
      freshNameIds(fullName) = id + 1
      s"$fullName$id"
    } else {
      freshNameIds += fullName -> 1
      fullName
    }
  }

  /**
   * Returns the specialized code to access a value from `inputRow` at `ordinal`.
   */
  def getValue(input: String, dataType: DataType, ordinal: String): String = {
    val jt = javaType(dataType)
    dataType match {
      // 在InternalRow中有很多的getInt()、getBoolean()、getByte()等方法，在这里input就是
      // 一个InternalRow. 假设jt是Int类型，ordinal是0，则得到的代码片段是：
      // input.getInt(0)
      case _ if isPrimitiveType(jt) => s"$input.get${primitiveTypeName(jt)}($ordinal)"
      // 其余的（非基础类型），作针对性处理
      case t: DecimalType => s"$input.getDecimal($ordinal, ${t.precision}, ${t.scale})"
      case StringType => s"$input.getUTF8String($ordinal)"
      case BinaryType => s"$input.getBinary($ordinal)"
      case CalendarIntervalType => s"$input.getInterval($ordinal)"
      case t: StructType => s"$input.getStruct($ordinal, ${t.size})"
      case _: ArrayType => s"$input.getArray($ordinal)"
      case _: MapType => s"$input.getMap($ordinal)"
      case NullType => "null"
      case udt: UserDefinedType[_] => getValue(input, udt.sqlType, ordinal)
      // 对get到的结果用(jt)作强制的类型转化
      case _ => s"($jt)$input.get($ordinal, null)"
    }
  }

  /**
   * Returns the code to update a column in Row for a given DataType.
   */
  def setColumn(row: String, dataType: DataType, ordinal: Int, value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) => s"$row.set${primitiveTypeName(jt)}($ordinal, $value)"
      case t: DecimalType => s"$row.setDecimal($ordinal, $value, ${t.precision})"
      case udt: UserDefinedType[_] => setColumn(row, udt.sqlType, ordinal, value)
      // The UTF8String, InternalRow, ArrayData and MapData may came from UnsafeRow, we should copy
      // it to avoid keeping a "pointer" to a memory region which may get updated afterwards.
      case StringType | _: StructType | _: ArrayType | _: MapType =>
        s"$row.update($ordinal, $value.copy())"
      case _ => s"$row.update($ordinal, $value)"
    }
  }

  /**
   * Update a column in MutableRow from ExprCode.
   *
   * @param isVectorized True if the underlying row is of type `ColumnarBatch.Row`, false otherwise
   */
  def updateColumn(
      row: String,
      dataType: DataType,
      ordinal: Int,
      ev: ExprCode,
      nullable: Boolean,
      isVectorized: Boolean = false): String = {
    if (nullable) {
      // Can't call setNullAt on DecimalType, because we need to keep the offset
      if (!isVectorized && dataType.isInstanceOf[DecimalType]) {
        s"""
           if (!${ev.isNull}) {
             ${setColumn(row, dataType, ordinal, ev.value)};
           } else {
             ${setColumn(row, dataType, ordinal, "null")};
           }
         """
      } else {
        s"""
           if (!${ev.isNull}) {
             ${setColumn(row, dataType, ordinal, ev.value)};
           } else {
             $row.setNullAt($ordinal);
           }
         """
      }
    } else {
      s"""${setColumn(row, dataType, ordinal, ev.value)};"""
    }
  }

  /**
   * Returns the specialized code to set a given value in a column vector for a given `DataType`.
   */
  def setValue(vector: String, rowId: String, dataType: DataType, value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) =>
        s"$vector.put${primitiveTypeName(jt)}($rowId, $value);"
      case t: DecimalType => s"$vector.putDecimal($rowId, $value, ${t.precision});"
      case t: StringType => s"$vector.putByteArray($rowId, $value.getBytes());"
      case _ =>
        throw new IllegalArgumentException(s"cannot generate code for unsupported type: $dataType")
    }
  }

  /**
   * Returns the specialized code to set a given value in a column vector for a given `DataType`
   * that could potentially be nullable.
   */
  def updateColumn(
      vector: String,
      rowId: String,
      dataType: DataType,
      ev: ExprCode,
      nullable: Boolean): String = {
    if (nullable) {
      s"""
         if (!${ev.isNull}) {
           ${setValue(vector, rowId, dataType, ev.value)}
         } else {
           $vector.putNull($rowId);
         }
       """
    } else {
      s"""${setValue(vector, rowId, dataType, ev.value)};"""
    }
  }

  /**
   * Returns the specialized code to access a value from a column vector for a given `DataType`.
   */
  def getValueFromVector(vector: String, dataType: DataType, rowId: String): String = {
    if (dataType.isInstanceOf[StructType]) {
      // `ColumnVector.getStruct` is different from `InternalRow.getStruct`, it only takes an
      // `ordinal` parameter.
      s"$vector.getStruct($rowId)"
    } else {
      getValue(vector, dataType, rowId)
    }
  }

  /**
   * Returns the name used in accessor and setter for a Java primitive type.
   */
  def primitiveTypeName(jt: String): String = jt match {
    case JAVA_INT => "Int"
    case _ => boxedType(jt)
  }

  def primitiveTypeName(dt: DataType): String = primitiveTypeName(javaType(dt))

  /**
   * Returns the Java type for a DataType.
   */
  def javaType(dt: DataType): String = dt match {
    case BooleanType => JAVA_BOOLEAN
    case ByteType => JAVA_BYTE
    case ShortType => JAVA_SHORT
    case IntegerType | DateType => JAVA_INT
    case LongType | TimestampType => JAVA_LONG
    case FloatType => JAVA_FLOAT
    case DoubleType => JAVA_DOUBLE
    case dt: DecimalType => "Decimal"
    case BinaryType => "byte[]"
    case StringType => "UTF8String"
    case CalendarIntervalType => "CalendarInterval"
    case _: StructType => "InternalRow"
    case _: ArrayType => "ArrayData"
    case _: MapType => "MapData"
    case udt: UserDefinedType[_] => javaType(udt.sqlType)
    case ObjectType(cls) if cls.isArray => s"${javaType(ObjectType(cls.getComponentType))}[]"
    case ObjectType(cls) => cls.getName
    case _ => "Object"
  }

  /**
   * Returns the boxed type in Java.
   */
  def boxedType(jt: String): String = jt match {
    case JAVA_BOOLEAN => "Boolean"
    case JAVA_BYTE => "Byte"
    case JAVA_SHORT => "Short"
    case JAVA_INT => "Integer"
    case JAVA_LONG => "Long"
    case JAVA_FLOAT => "Float"
    case JAVA_DOUBLE => "Double"
    case other => other
  }

  def boxedType(dt: DataType): String = boxedType(javaType(dt))

  /**
   * Returns the representation of default value for a given Java Type.
   */
  def defaultValue(jt: String): String = jt match {
    case JAVA_BOOLEAN => "false"
    case JAVA_BYTE => "(byte)-1"
    case JAVA_SHORT => "(short)-1"
    case JAVA_INT => "-1"
    case JAVA_LONG => "-1L"
    case JAVA_FLOAT => "-1.0f"
    case JAVA_DOUBLE => "-1.0"
    case _ => "null"
  }

  def defaultValue(dt: DataType): String = defaultValue(javaType(dt))

  /**
   * Generates code for equal expression in Java.
   */
  def genEqual(dataType: DataType, c1: String, c2: String): String = dataType match {
    case BinaryType => s"java.util.Arrays.equals($c1, $c2)"
    case FloatType => s"(java.lang.Float.isNaN($c1) && java.lang.Float.isNaN($c2)) || $c1 == $c2"
    case DoubleType => s"(java.lang.Double.isNaN($c1) && java.lang.Double.isNaN($c2)) || $c1 == $c2"
    case dt: DataType if isPrimitiveType(dt) => s"$c1 == $c2"
    case dt: DataType if dt.isInstanceOf[AtomicType] => s"$c1.equals($c2)"
    case array: ArrayType => genComp(array, c1, c2) + " == 0"
    case struct: StructType => genComp(struct, c1, c2) + " == 0"
    case udt: UserDefinedType[_] => genEqual(udt.sqlType, c1, c2)
    case NullType => "false"
    case _ =>
      throw new IllegalArgumentException(
        "cannot generate equality code for un-comparable type: " + dataType.simpleString)
  }

  /**
   * Generates code for comparing two expressions.
   *
   * @param dataType data type of the expressions
   * @param c1 name of the variable of expression 1's output
   * @param c2 name of the variable of expression 2's output
   */
  def genComp(dataType: DataType, c1: String, c2: String): String = dataType match {
    // java boolean doesn't support > or < operator
    case BooleanType => s"($c1 == $c2 ? 0 : ($c1 ? 1 : -1))"
    case DoubleType => s"org.apache.spark.util.Utils.nanSafeCompareDoubles($c1, $c2)"
    case FloatType => s"org.apache.spark.util.Utils.nanSafeCompareFloats($c1, $c2)"
    // use c1 - c2 may overflow
    case dt: DataType if isPrimitiveType(dt) => s"($c1 > $c2 ? 1 : $c1 < $c2 ? -1 : 0)"
    case BinaryType => s"org.apache.spark.sql.catalyst.util.TypeUtils.compareBinary($c1, $c2)"
    case NullType => "0"
    case array: ArrayType =>
      val elementType = array.elementType
      val elementA = freshName("elementA")
      val isNullA = freshName("isNullA")
      val elementB = freshName("elementB")
      val isNullB = freshName("isNullB")
      val compareFunc = freshName("compareArray")
      val minLength = freshName("minLength")
      val funcCode: String =
        s"""
          public int $compareFunc(ArrayData a, ArrayData b) {
            // when comparing unsafe arrays, try equals first as it compares the binary directly
            // which is very fast.
            if (a instanceof UnsafeArrayData && b instanceof UnsafeArrayData && a.equals(b)) {
              return 0;
            }
            int lengthA = a.numElements();
            int lengthB = b.numElements();
            int $minLength = (lengthA > lengthB) ? lengthB : lengthA;
            for (int i = 0; i < $minLength; i++) {
              boolean $isNullA = a.isNullAt(i);
              boolean $isNullB = b.isNullAt(i);
              if ($isNullA && $isNullB) {
                // Nothing
              } else if ($isNullA) {
                return -1;
              } else if ($isNullB) {
                return 1;
              } else {
                ${javaType(elementType)} $elementA = ${getValue("a", elementType, "i")};
                ${javaType(elementType)} $elementB = ${getValue("b", elementType, "i")};
                int comp = ${genComp(elementType, elementA, elementB)};
                if (comp != 0) {
                  return comp;
                }
              }
            }

            if (lengthA < lengthB) {
              return -1;
            } else if (lengthA > lengthB) {
              return 1;
            }
            return 0;
          }
        """
      s"${addNewFunction(compareFunc, funcCode)}($c1, $c2)"
    case schema: StructType =>
      val comparisons = GenerateOrdering.genComparisons(this, schema)
      val compareFunc = freshName("compareStruct")
      val funcCode: String =
        s"""
          public int $compareFunc(InternalRow a, InternalRow b) {
            // when comparing unsafe rows, try equals first as it compares the binary directly
            // which is very fast.
            if (a instanceof UnsafeRow && b instanceof UnsafeRow && a.equals(b)) {
              return 0;
            }
            $comparisons
            return 0;
          }
        """
      s"${addNewFunction(compareFunc, funcCode)}($c1, $c2)"
    case other if other.isInstanceOf[AtomicType] => s"$c1.compare($c2)"
    case udt: UserDefinedType[_] => genComp(udt.sqlType, c1, c2)
    case _ =>
      throw new IllegalArgumentException(
        "cannot generate compare code for un-comparable type: " + dataType.simpleString)
  }

  /**
   * Generates code for greater of two expressions.
   *
   * @param dataType data type of the expressions
   * @param c1 name of the variable of expression 1's output
   * @param c2 name of the variable of expression 2's output
   */
  def genGreater(dataType: DataType, c1: String, c2: String): String = javaType(dataType) match {
    case JAVA_BYTE | JAVA_SHORT | JAVA_INT | JAVA_LONG => s"$c1 > $c2"
    case _ => s"(${genComp(dataType, c1, c2)}) > 0"
  }

  /**
   * Generates code to do null safe execution, i.e. only execute the code when the input is not
   * null by adding null check if necessary.
   *
   * @param nullable used to decide whether we should add null check or not.
   * @param isNull the code to check if the input is null.
   * @param execute the code that should only be executed when the input is not null.
   */
  def nullSafeExec(nullable: Boolean, isNull: String)(execute: String): String = {
    if (nullable) {
      s"""
        if (!$isNull) {
          $execute
        }
      """
    } else {
      "\n" + execute
    }
  }

  /**
   * List of java data types that have special accessors and setters in [[InternalRow]].
   */
  val primitiveTypes =
    Seq(JAVA_BOOLEAN, JAVA_BYTE, JAVA_SHORT, JAVA_INT, JAVA_LONG, JAVA_FLOAT, JAVA_DOUBLE)

  /**
   * Returns true if the Java type has a special accessor and setter in [[InternalRow]].
   */
  def isPrimitiveType(jt: String): Boolean = primitiveTypes.contains(jt)

  def isPrimitiveType(dt: DataType): Boolean = isPrimitiveType(javaType(dt))

  /**
   * Splits the generated code of expressions into multiple functions, because function has
   * 64kb code size limit in JVM. If the class to which the function would be inlined would grow
   * beyond 1000kb, we declare a private, inner sub-class, and the function is inlined to it
   * instead, because classes have a constant pool limit of 65,536 named values.
   *
   * Note that different from `splitExpressions`, we will extract the current inputs of this
   * context and pass them to the generated functions. The input is `INPUT_ROW` for normal codegen
   * path, and `currentVars` for whole stage codegen path. Whole stage codegen path is not
   * supported yet.
   *
   * @param expressions the codes to evaluate expressions.
   * @param funcName the split function name base.
   * @param extraArguments the list of (type, name) of the arguments of the split function,
   *                       except for the current inputs like `ctx.INPUT_ROW`.
   * @param returnType the return type of the split function.
   * @param makeSplitFunction makes split function body, e.g. add preparation or cleanup.
   * @param foldFunctions folds the split function calls.
   */
  def splitExpressionsWithCurrentInputs(
      expressions: Seq[String],
      funcName: String = "apply",
      extraArguments: Seq[(String, String)] = Nil,
      returnType: String = "void",
      makeSplitFunction: String => String = identity,
      foldFunctions: Seq[String] => String = _.mkString("", ";\n", ";")): String = {
    // TODO: support whole stage codegen
    if (INPUT_ROW == null || currentVars != null) {
      expressions.mkString("\n")
    } else {
      splitExpressions(
        expressions,
        funcName,
        ("InternalRow", INPUT_ROW) +: extraArguments,
        returnType,
        makeSplitFunction,
        foldFunctions)
    }
  }

  /**
   * 将生成的expressions的代码（可能会超过64kb）拆散成多个functions，因为在JVM中单独的一个function的
   * 代码size不能超过64kb。如果把一个function作为该该class的内联函数将会超过1000kb，那么，我们新定义一个
   * 私有的内部类，然后将该function作为该内部类的内联函数。因为一个calss的常量池只能包含65536个声明变量（
   * 估计包含了成员变量和成员方法）。
   * Splits the generated code of expressions into multiple functions, because function has
   * 64kb code size limit in JVM. If the class to which the function would be inlined would grow
   * beyond 1000kb, we declare a private, inner sub-class, and the function is inlined to it
   * instead, because classes have a constant pool limit of 65,536 named values.
   *
   * @param expressions the codes to evaluate expressions.
   * @param funcName the split function name base.
   * @param arguments the list of (type, name) of the arguments of the split function.
   * @param returnType the return type of the split function.
   * @param makeSplitFunction makes split function body, e.g. add preparation or cleanup.
   * @param foldFunctions folds the split function calls.
   */
  def splitExpressions(
      expressions: Seq[String],
      funcName: String,
      arguments: Seq[(String, String)],
      returnType: String = "void",
      makeSplitFunction: String => String = identity,
      foldFunctions: Seq[String] => String = _.mkString("", ";\n", ";")): String = {
    // 将expressions拆分成多个代码块，一个代码块可能包含一个或多个expressin的code。
    // 每个代码块构建出一个function。
    val blocks = buildCodeBlocks(expressions)

    if (blocks.length == 1) {
      // QUESTION：如果所有的expression只生成了一个block，就直接可以生成inline function？
      // 考虑这样一种情况，如果只有一个expression，而该expression的size超过了inline function的阈值，
      // 那么，它还能作为该class的inline function吗？
      // inline execution if only one block
      blocks.head
    } else {
      if (Utils.isTesting) {
        // Passing global variables to the split method is dangerous, as any mutating to it is
        // ignored and may lead to unexpected behavior.
        arguments.foreach { case (_, name) =>
          assert(!mutableStateNames.contains(name),
            s"split function argument $name cannot be a global variable.")
        }
      }

      val func = freshName(funcName)
      // 生成func的参数列表：(形参类型1 形参名字1, 形参类型2 形参名字2, ...) （应该是java的）
      // 例如：func(int age, String name)
      val argString = arguments.map { case (t, name) => s"$t $name" }.mkString(", ")
      // 为每个block，构建一个func。也就是生成了一组相同功能的func(func_1, func_2,....)
      // （本来可以调用一个func完成的事情，现在要调用这一组func_i才能完成这一件相同的事情。）
      val functions = blocks.zipWithIndex.map { case (body, i) =>
        val name = s"${func}_$i"
        val code = s"""
           |private $returnType $name($argString) {
           |  ${makeSplitFunction(body)}
           |}
         """.stripMargin
        // 在内部添加该方法（作为某个私有内部类的inline function）
        addNewFunctionInternal(name, code, inlineToOuterClass = false)
      }

      // function的类型为NewFunctionSpec(nfs)，如果nfs.innerClassName为Empty，说明该function被添加到了
      // outerClass中，反之，被添加到了innerClass（即outerClass的内部类）中
      val (outerClassFunctions, innerClassFunctions) = functions.partition(_.innerClassName.isEmpty)

      val argsString = arguments.map(_._2).mkString(", ")
      // 生成调用outerClass的functions的代码片段（注意上面的functions是生func定义或声明的的代码片段，这里是调用！）
      val outerClassFunctionCalls = outerClassFunctions.map(f => s"${f.functionName}($argsString)")

      // 生成调用innerClass的functions的代码片段
      val innerClassFunctionCalls = generateInnerClassesFunctionCalls(
        innerClassFunctions,
        func,
        arguments,
        returnType,
        makeSplitFunction,
        foldFunctions)

      foldFunctions(outerClassFunctionCalls ++ innerClassFunctionCalls)
    }
  }

  /**
   * 基于String的长度阈值来将生成的expressions的一个单独的代码片段拆封成多个代码片段
   * Splits the generated code of expressions into multiple sequences of String
   * based on a threshold of length of a String
   *
   * @param expressions the codes to evaluate expressions.
   */
  private def buildCodeBlocks(expressions: Seq[String]): Seq[String] = {
    val blocks = new ArrayBuffer[String]()
    val blockBuilder = new StringBuilder()
    var length = 0
    // 每个code对应一个单独的expression生成的代码片段
    for (code <- expressions) {
      // 我们无法知道究竟会有多少字节的代码生成（因为这得等到代码编译完成之后才能知道），所以，我们采用代码
      // 片段（源代码）的字符长度来作为衡量标准。一个方法不能超过8k，否则它将无法使用JIT功能，也不能太小，
      // 否则（对于宽表），就会有很多的方法调用（应该比较影响性能）。
      // We can't know how many bytecode will be generated, so use the length of source code
      // as metric. A method should not go beyond 8K, otherwise it will not be JITted, should
      // also not be too small, or it will have many function calls (for wide table), see the
      // results in BenchmarkWideTable.
      if (length > 1024) {
        blocks += blockBuilder.toString()
        blockBuilder.clear()
        length = 0
      }
      // 注意：一个完成的expression的code是不可能拆分开来的！！！我们只会在一个function中放入
      // 多个expressions的code，🈯️直到该方法超过限制的1024大小。
      // 假如，有个expressions = [code1, code2, code3], 则我们可能拆出两个function：function1 = {code1, code2}
      // function2 = {code3}, 且code1 + code2 > 1024.
      blockBuilder.append(code)
      // 计算在code去除注释和多余空行后的length
      length += CodeFormatter.stripExtraNewLinesAndComments(code).length
    }
    blocks += blockBuilder.toString()
  }

  /**
   * 在这里，我们处理所有被添加到内部类而不是外部类中的方法。由于这些方法可能会很多，所以在outer class中
   * 直接一个个调用这些方法会在outer class的常量池中加入很多entries(应该是用于记录这些调用信息)。而这就
   * 可能会导致常量池的大小超过jvm的限制。另外，这也可能造成outer class的方法调用会超过64kb的限制。为了
   * 该问题，我们将一个内部类中的方法组织成一个group添加一个新的方法中，而outer class只需要调用该新方法即可。
   * 由此，就减少了outer class直接调用内部类方法的次数。
   * Here we handle all the methods which have been added to the inner classes and
   * not to the outer class.
   * Since they can be many, their direct invocation in the outer class adds many entries
   * to the outer class' constant pool. This can cause the constant pool to past JVM limit.
   * Moreover, this can cause also the outer class method where all the invocations are
   * performed to grow beyond the 64k limit.
   * To avoid these problems, we group them and we call only the grouping methods in the
   * outer class.
   *
   * @param functions a [[Seq]] of [[NewFunctionSpec]] defined in the inner classes
   * @param funcName the split function name base.
   * @param arguments the list of (type, name) of the arguments of the split function.
   * @param returnType the return type of the split function.
   * @param makeSplitFunction makes split function body, e.g. add preparation or cleanup.
   * @param foldFunctions folds the split function calls.
   * @return an [[Iterable]] containing the methods' invocations
   */
  private def generateInnerClassesFunctionCalls(
      functions: Seq[NewFunctionSpec],
      funcName: String,
      arguments: Seq[(String, String)],
      returnType: String,
      makeSplitFunction: String => String,
      foldFunctions: Seq[String] => String): Iterable[String] = {
    val innerClassToFunctions = mutable.LinkedHashMap.empty[(String, String), Seq[String]]
    functions.foreach(f => {
      val key = (f.innerClassName.get, f.innerClassInstance.get)
      // 从队列的头部插入该function（性能更优？？？why？？？）
      val value = f.functionName +: innerClassToFunctions.getOrElse(key, Seq.empty[String])
      innerClassToFunctions.put(key, value)
    })

    // 生成定义func时的参数列表
    val argDefinitionString = arguments.map { case (t, name) => s"$t $name" }.mkString(", ")
    // 生成调用func时的参数列表
    val argInvocationString = arguments.map(_._2).mkString(", ")

    innerClassToFunctions.flatMap {
      case ((innerClassName, innerClassInstance), innerClassFunctions) =>
        // 出于性能的考虑，functions在添加的时候是从队首插入的，而不是队尾。
        // 因此，在这里我们将顺序反过来。即现在functions的顺序，和该function加入到队列中的先后顺序是一样的。
        // for performance reasons, the functions are prepended, instead of appended,
        // thus here they are in reversed order
        val orderedFunctions = innerClassFunctions.reverse
        // 如果该内部类的function的个数超过了设定的阈值（默认为3），则我们在该内部类中新建一个方法，例如apply(),
        // 然后在该apply()方法中，调用这些functions。这样，就可以达到在outerClass中，减少调用内部类的functions
        // 的个数的目的。详见下面注释中的例子。
        if (orderedFunctions.size > CodeGenerator.MERGE_SPLIT_METHODS_THRESHOLD) {
          // Adding a new function to each inner class which contains the invocation of all the
          // ones which have been added to that inner class. For example,
          //   private class NestedClass {
          //     private void apply_862(InternalRow i) { ... }
          //     private void apply_863(InternalRow i) { ... }
          //       ...
          //     private void apply(InternalRow i) {
          //       apply_862(i);
          //       apply_863(i);
          //       ...
          //     }
          //   }
          // 在内部类中新生成一个方法，用于调用其它的方法。以减少在outerClass中调用内部类方法的次数。
          val body = foldFunctions(orderedFunctions.map(name => s"$name($argInvocationString)"))
          val code = s"""
              |private $returnType $funcName($argDefinitionString) {
              |  ${makeSplitFunction(body)}
              |}
            """.stripMargin
          addNewFunctionToClass(funcName, code, innerClassName)
          Seq(s"$innerClassInstance.$funcName($argInvocationString)")
        } else {
          orderedFunctions.map(f => s"$innerClassInstance.$f($argInvocationString)")
        }
    }
  }

  /**
   * Perform a function which generates a sequence of ExprCodes with a given mapping between
   * expressions and common expressions, instead of using the mapping in current context.
   */
  def withSubExprEliminationExprs(
      newSubExprEliminationExprs: Map[Expression, SubExprEliminationState])(
      f: => Seq[ExprCode]): Seq[ExprCode] = {
    val oldsubExprEliminationExprs = subExprEliminationExprs
    subExprEliminationExprs.clear
    newSubExprEliminationExprs.foreach(subExprEliminationExprs += _)

    val genCodes = f

    // Restore previous subExprEliminationExprs
    subExprEliminationExprs.clear
    oldsubExprEliminationExprs.foreach(subExprEliminationExprs += _)
    genCodes
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination. This finds the
   * common subexpressions, generates the code snippets that evaluate those expressions and
   * populates the mapping of common subexpressions to the generated code snippets. The generated
   * code snippets will be returned and should be inserted into generated codes before these
   * common subexpressions actually are used first time.
   */
  def subexpressionEliminationForWholeStageCodegen(expressions: Seq[Expression]): SubExprCodes = {
    // Create a clear EquivalentExpressions and SubExprEliminationState mapping
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions
    val subExprEliminationExprs = mutable.HashMap.empty[Expression, SubExprEliminationState]

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree)

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    val codes = commonExprs.map { e =>
      val expr = e.head
      // Generate the code for this expression tree.
      val eval = expr.genCode(this)
      val state = SubExprEliminationState(eval.isNull, eval.value)
      e.foreach(subExprEliminationExprs.put(_, state))
      eval.code.trim
    }
    SubExprCodes(codes, subExprEliminationExprs.toMap)
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination. This finds the
   * common subexpressions, generates the functions that evaluate those expressions and populates
   * the mapping of common subexpressions to the generated functions.
   */
  private def subexpressionElimination(expressions: Seq[Expression]): Unit = {
    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    commonExprs.foreach { e =>
      val expr = e.head
      val fnName = freshName("subExpr")
      val isNull = addMutableState(JAVA_BOOLEAN, "subExprIsNull")
      val value = addMutableState(javaType(expr.dataType), "subExprValue")

      // Generate the code for this expression tree and wrap it in a function.
      val eval = expr.genCode(this)
      val fn =
        s"""
           |private void $fnName(InternalRow $INPUT_ROW) {
           |  ${eval.code.trim}
           |  $isNull = ${eval.isNull};
           |  $value = ${eval.value};
           |}
           """.stripMargin

      // Add a state and a mapping of the common subexpressions that are associate with this
      // state. Adding this expression to subExprEliminationExprMap means it will call `fn`
      // when it is code generated. This decision should be a cost based one.
      //
      // The cost of doing subexpression elimination is:
      //   1. Extra function call, although this is probably *good* as the JIT can decide to
      //      inline or not.
      // The benefit doing subexpression elimination is:
      //   1. Running the expression logic. Even for a simple expression, it is likely more than 3
      //      above.
      //   2. Less code.
      // Currently, we will do this for all non-leaf only expression trees (i.e. expr trees with
      // at least two nodes) as the cost of doing it is expected to be low.

      subexprFunctions += s"${addNewFunction(fnName, fn)}($INPUT_ROW);"
      val state = SubExprEliminationState(isNull, value)
      e.foreach(subExprEliminationExprs.put(_, state))
    }
  }

  /**
   * Generates code for expressions. If doSubexpressionElimination is true, subexpression
   * elimination will be performed. Subexpression elimination assumes that the code for each
   * expression will be combined in the `expressions` order.
   */
  def generateExpressions(
      expressions: Seq[Expression],
      doSubexpressionElimination: Boolean = false): Seq[ExprCode] = {
    // TODO read when doSubexpressionElimination = true
    if (doSubexpressionElimination) subexpressionElimination(expressions)
    expressions.map(e => e.genCode(this))
  }

  /**
   * get a map of the pair of a place holder and a corresponding comment
   */
  def getPlaceHolderToComments(): collection.Map[String, String] = placeHolderToComments

  /**
   * Register a comment and return the corresponding place holder
   */
  def registerComment(text: => String): String = {
    // By default, disable comments in generated code because computing the comments themselves can
    // be extremely expensive in certain cases, such as deeply-nested expressions which operate over
    // inputs with wide schemas. For more details on the performance issues that motivated this
    // flat, see SPARK-15680.
    if (SparkEnv.get != null && SparkEnv.get.conf.getBoolean("spark.sql.codegen.comments", false)) {
      val name = freshName("c")
      // 生成注释：如果text有多行，则用'/** */'多行注释；反之，使用'//'单行注释；
      val comment = if (text.contains("\n") || text.contains("\r")) {
        text.split("(\r\n)|\r|\n").mkString("/**\n * ", "\n * ", "\n */")
      } else {
        s"// $text"
      }
      // 添加注释变量名name(c$id)到注释comment之间的映射
      placeHolderToComments += (name -> comment)
      // 返回注释变量名
      s"/*$name*/"
    } else {
      ""
    }
  }

  /**
   * Returns the length of parameters for a Java method descriptor. `this` contributes one unit
   * and a parameter of type long or double contributes two units. Besides, for nullable parameter,
   * we also need to pass a boolean parameter for the null status.
   */
  def calculateParamLength(params: Seq[Expression]): Int = {
    def paramLengthForExpr(input: Expression): Int = {
      // For a nullable expression, we need to pass in an extra boolean parameter.
      (if (input.nullable) 1 else 0) + javaType(input.dataType) match {
        case JAVA_LONG | JAVA_DOUBLE => 2
        case _ => 1
      }
    }
    // Initial value is 1 for `this`.
    1 + params.map(paramLengthForExpr(_)).sum
  }

  /**
   * In Java, a method descriptor is valid only if it represents method parameters with a total
   * length less than a pre-defined constant.
   */
  def isValidParamLength(paramLength: Int): Boolean = {
    paramLength <= CodeGenerator.MAX_JVM_METHOD_PARAMS_LENGTH
  }
}

/**
 * A wrapper for generated class, defines a `generate` method so that we can pass extra objects
 * into generated class.
 */
abstract class GeneratedClass {
  def generate(references: Array[Any]): Any
}

/**
 * A wrapper for the source code to be compiled by [[CodeGenerator]].
 */
class CodeAndComment(val body: String, val comment: collection.Map[String, String])
  extends Serializable {
  override def equals(that: Any): Boolean = that match {
    case t: CodeAndComment if t.body == body => true
    case _ => false
  }

  override def hashCode(): Int = body.hashCode
}

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {

  protected val genericMutableRowType: String = classOf[GenericInternalRow].getName

  /**
   * Generates a class for a given input expression.  Called when there is not cached code
   * already available.
   */
  protected def create(in: InType): OutType

  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  protected def canonicalize(in: InType): InType

  /** Binds an input expression to a given input schema */
  protected def bind(in: InType, inputSchema: Seq[Attribute]): InType

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expressions: InType): OutType = create(canonicalize(expressions))

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodegenContext = {
    new CodegenContext
  }
}

object CodeGenerator extends Logging {

  // This is the value of HugeMethodLimit in the OpenJDK JVM settings
  final val DEFAULT_JVM_HUGE_METHOD_LIMIT = 8000

  // The max valid length of method parameters in JVM.
  final val MAX_JVM_METHOD_PARAMS_LENGTH = 255

  // This is the threshold over which the methods in an inner class are grouped in a single
  // method which is going to be called by the outer class instead of the many small ones
  final val MERGE_SPLIT_METHODS_THRESHOLD = 3

  // 一个class中的命名常量的个数被常量池限制为65536。我们无法知道一个class中插入了多少个常量，所以，我们用
  // 1000k bytes这样一个阈值来决定一个func是否应该作为一个私有的内部类的内联函数。如果超过了该阈值，我们就要
  // 为新建一个私有的内部类，然后将该方法作为该新建内部类的内联函数。
  // The number of named constants that can exist in the class is limited by the Constant Pool
  // limit, 65,536. We cannot know how many constants will be inserted for a class, so we use a
  // threshold of 1000k bytes to determine when a function should be inlined to a private, inner
  // class.
  final val GENERATED_CLASS_SIZE_THRESHOLD = 1000000

  // 在outer class中，全局变量（java基础数据类型或复杂类型（例如多为的数组））的总数不能超过该值。
  // 如果超过该值，则需要在该outer class中构建一个inner class，来放置其它变量。
  // This is the threshold for the number of global variables, whose types are primitive type or
  // complex type (e.g. more than one-dimensional array), that will be placed at the outer class
  final val OUTER_CLASS_VARIABLES_THRESHOLD = 10000

  // This is the maximum number of array elements to keep global variables in one Java array
  // 32767 is the maximum integer value that does not require a constant pool entry in a Java
  // bytecode instruction
  final val MUTABLESTATEARRAY_SIZE_LIMIT = 32768

  /**
   * Compile the Java source code into a Java class, using Janino.
   *
   * @return a pair of a generated class and the max bytecode size of generated functions.
   */
  def compile(code: CodeAndComment): (GeneratedClass, Int) = try {
    cache.get(code)
  } catch {
    // Cache.get() may wrap the original exception. See the following URL
    // http://google.github.io/guava/releases/14.0/api/docs/com/google/common/cache/
    //   Cache.html#get(K,%20java.util.concurrent.Callable)
    case e @ (_: UncheckedExecutionException | _: ExecutionError) =>
      throw e.getCause
  }

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: CodeAndComment): (GeneratedClass, Int) = {
    val evaluator = new ClassBodyEvaluator()

    // A special classloader used to wrap the actual parent classloader of
    // [[org.codehaus.janino.ClassBodyEvaluator]] (see CodeGenerator.doCompile). This classloader
    // does not throw a ClassNotFoundException with a cause set (i.e. exception.getCause returns
    // a null). This classloader is needed because janino will throw the exception directly if
    // the parent classloader throws a ClassNotFoundException with cause set instead of trying to
    // find other possible classes (see org.codehaus.janinoClassLoaderIClassLoader's
    // findIClass method). Please also see https://issues.apache.org/jira/browse/SPARK-15622 and
    // https://issues.apache.org/jira/browse/SPARK-11636.
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("org.apache.spark.sql.catalyst.expressions.GeneratedClass")
    evaluator.setDefaultImports(Array(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName
    ))
    evaluator.setExtendedClass(classOf[GeneratedClass])

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      s"\n${CodeFormatter.format(code)}"
    })

    val maxCodeSize = try {
      evaluator.cook("generated.java", code.body)
      updateAndGetCompilationStats(evaluator)
    } catch {
      case e: InternalCompilerException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        val maxLines = SQLConf.get.loggingMaxLinesForCodegen
        logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
        throw new InternalCompilerException(msg, e)
      case e: CompileException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        val maxLines = SQLConf.get.loggingMaxLinesForCodegen
        logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
        throw new CompileException(msg, e.getLocation)
    }

    (evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass], maxCodeSize)
  }

  /**
   * Returns the max bytecode size of the generated functions by inspecting janino private fields.
   * Also, this method updates the metrics information.
   */
  private def updateAndGetCompilationStats(evaluator: ClassBodyEvaluator): Int = {
    // First retrieve the generated classes.
    val classes = {
      val resultField = classOf[SimpleCompiler].getDeclaredField("result")
      resultField.setAccessible(true)
      val loader = resultField.get(evaluator).asInstanceOf[ByteArrayClassLoader]
      val classesField = loader.getClass.getDeclaredField("classes")
      classesField.setAccessible(true)
      classesField.get(loader).asInstanceOf[JavaMap[String, Array[Byte]]].asScala
    }

    // Then walk the classes to get at the method bytecode.
    val codeAttr = Utils.classForName("org.codehaus.janino.util.ClassFile$CodeAttribute")
    val codeAttrField = codeAttr.getDeclaredField("code")
    codeAttrField.setAccessible(true)
    val codeSizes = classes.flatMap { case (_, classBytes) =>
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(classBytes.length)
      try {
        val cf = new ClassFile(new ByteArrayInputStream(classBytes))
        val stats = cf.methodInfos.asScala.flatMap { method =>
          method.getAttributes().filter(_.getClass.getName == codeAttr.getName).map { a =>
            val byteCodeSize = codeAttrField.get(a).asInstanceOf[Array[Byte]].length
            CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.update(byteCodeSize)
            byteCodeSize
          }
        }
        Some(stats)
      } catch {
        case NonFatal(e) =>
          logWarning("Error calculating stats of compiled class.", e)
          None
      }
    }.flatten

    codeSizes.max
  }

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint.  Note that this cache does not use
   * weak keys/values and thus does not respond to memory pressure.
   */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[CodeAndComment, (GeneratedClass, Int)]() {
        override def load(code: CodeAndComment): (GeneratedClass, Int) = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
          CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })
}
