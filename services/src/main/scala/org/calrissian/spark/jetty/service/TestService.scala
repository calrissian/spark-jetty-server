/*
 * Copyright (C) 2014 Corey J. Nolet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.spark.jetty.service


import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.{AccumulatorParam, SparkContext}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.StructField
import scala.Some

/**
 * A stateless service class that interacts with a spark context.
 */

object TestService {



  implicit object SchemaAP extends AccumulatorParam[Set[(String, DataType)]] {
    def zero(v: Set[(String,DataType)]): Set[(String,DataType)] = Set()
    def addInPlace(s1: Set[(String,DataType)], s2: Set[(String,DataType)]): Set[(String, DataType)] = s1 ++ s2
  }

  def allKeysWithValueTypes(m: Map[String, Any]): Set[(String, DataType)] = {
    val keyValuePairs = m.map {
      // Quote the key with backticks to handle cases which have dots
      // in the field name.
      case (key, value) => (s"`$key`", value)
    }.toSet
    keyValuePairs.flatMap {
      case (key: String, struct: Map[_, _]) => {
        // The value associated with the key is an JSON object.
        allKeysWithValueTypes(struct.asInstanceOf[Map[String, Any]]).map {
          case (k, dataType) => (s"$key.$k", dataType)
        } ++ Set((key, StructType(Nil)))
      }
      case (key: String, array: Seq[_]) => {
        // The value associated with the key is an array.
        // Handle inner structs of an array.
        def buildKeyPathForInnerStructs(v: Any, t: DataType): Seq[(String, DataType)] = t match {
          case ArrayType(e: StructType, containsNull) => {
            // The elements of this arrays are structs.
            v.asInstanceOf[Seq[Map[String, Any]]].flatMap(Option(_)).flatMap {
              element => allKeysWithValueTypes(element)
            }.map {
              case (k, t) => (s"$key.$k", t)
            }
          }
          case ArrayType(t1, containsNull) =>
            v.asInstanceOf[Seq[Any]].flatMap(Option(_)).flatMap {
              element => buildKeyPathForInnerStructs(element, t1)
            }
          case other => Nil
        }
        val elementType = typeOfArray(array)
        buildKeyPathForInnerStructs(array, elementType) :+ (key, elementType)
      }
      // we couldn't tell what the type is if the value is null or empty string
      case (key: String, value) if value == "" || value == null => (key, NullType) :: Nil
      case (key: String, value) => (key, typeOfPrimitiveValue(value)) :: Nil
    }
  }


  /**
   * Returns the most general data type for two given data types.
   */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonType(t1, t2) match {
      case Some(commonType) => commonType
      case None =>
        // t1 or t2 is a StructType, ArrayType, or an unexpected type.
        (t1, t2) match {
          case (other: DataType, NullType) => other
          case (NullType, other: DataType) => other
          case (StructType(fields1), StructType(fields2)) => {
            val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
              case (name, fieldTypes) => {
                val dataType = fieldTypes.map(field => field.dataType).reduce(
                  (type1: DataType, type2: DataType) => compatibleType(type1, type2))
                StructField(name, dataType, true)
              }
            }
            StructType(newFields.toSeq.sortBy(_.name))
          }
          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
          // TODO: We should use JsonObjectStringType to mark that values of field will be
          // strings and every string is a Json object.
          case (_, _) => StringType
        }
    }
  }

  private def typeOfPrimitiveValue: PartialFunction[Any, DataType] = {
    ScalaReflection.typeOfObject orElse {
      // Since we do not have a data type backed by BigInteger,
      // when we see a Java BigInteger, we use DecimalType.
      case value: java.math.BigInteger => DecimalType.Unlimited
      // DecimalType's JVMType is scala BigDecimal.
      case value: java.math.BigDecimal => DecimalType.Unlimited
      // Unexpected data type.
      case _ => StringType
    }
  }

  /**
   * Returns the element type of an JSON array. We go through all elements of this array
   * to detect any possible type conflict. We use [[compatibleType]] to resolve
   * type conflicts.
   */
  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.exists(v => v == null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements.map {
        e => e match {
          case map: Map[_, _] => StructType(Nil)
          // We have an array of arrays. If those element arrays do not have the same
          // element types, we will return ArrayType[StringType].
          case seq: Seq[_] =>  typeOfArray(seq)
          case value => typeOfPrimitiveValue(value)
        }
      }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

      ArrayType(elementType, containsNull)
    }
  }

  def createTestMaps = {
    Seq[Map[String,Any]](

      Map(
        ("age" -> 35),
        ("name" -> "John Smith")
      ),

      Map(
        ("age" -> 50),
        ("name" -> "Jane Smith"),
        ("sex" -> "female"),
        ("location" -> Map[String,Any](
          ("houseNumber" -> 2345),
          ("street" -> "myStreet")
        ))

      )
    )
  }
}

class TestService(sparkContext: SparkContext) extends Serializable {

  import TestService._


  def testSchemaInference = {

    val accumulator = sparkContext.accumulator(Set[(String,DataType)]())

    sparkContext.parallelize(createTestMaps).map(it => accumulator.add(allKeysWithValueTypes(it))).foreach(println)

    val json = StructType(accumulator.value.map(it => StructField(it._1, it._2)).toSeq).json

    DataType.fromJson(json)
  }

  /**
   * A simple count of items in a sequence which is able to be processed in parallel
   */
  def test: String = sparkContext.parallelize(0 to 500000, 25).count.toString


}
