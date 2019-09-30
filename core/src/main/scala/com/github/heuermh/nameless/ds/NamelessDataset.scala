package com.github.heuermh.nameless.ds

import grizzled.slf4j.Logging

import java.util.Objects.requireNonNull

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{ Function, Function2 }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset, SQLContext }

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/**
 * Nameless dataset conversion function.
 *
 * @tparam T type of data in the source dataset wrapped in an RDD or JavaRDD
 * @tparam U type of data in the source dataset wrapped in a Dataset
 * @tparam V type of the source dataset
 * @tparam X type of data in the target dataset wrapped in an RDD or JavaRDD
 * @tparam Y type of data in the target dataset wrapped in a Dataset
 * @tparam Z type of the target dataset
 */
trait NamelessDatasetConversion[T, U <: Product, V <: NamelessDataset[T, U, V], X, Y <: Product, Z <: NamelessDataset[X, Y, Z]] extends Function2[V, Dataset[Y], Z] {
  val yTag: TypeTag[Y]

  /**
   * Convert a source dataset to a target dataset, possibly of different types.
   *
   * @param v1 source dataset
   * @param v2 target dataset wrapped in a Dataset
   * @return the source dataset converted to a target dataset
   */
  def call(v1: V, v2: Dataset[Y]): Z
}

/**
 * Nameless dataset.
 *
 * @tparam T type of data wrapped in an RDD or JavaRDD
 * @tparam U type of data wrapped in a Dataset
 * @tparam V type of this nameless dataset
 */
trait NamelessDataset[T, U <: Product, V <: NamelessDataset[T, U, V]] extends Logging {
  val uTag: TypeTag[U]
  val rdd: RDD[T]
  val dataset: Dataset[U]

  lazy val jrdd: JavaRDD[T] = {
    rdd.toJavaRDD()
  }

  /**
   * Convert this dataset to a DataFrame.
   *
   * @return this dataset converted to a DataFrame
   */
  def toDataFrame(): DataFrame = {
    toDF()
  }

  /**
   * Convert this dataset to a Dataset.
   *
   * @return this dataset converted to a Dataset
   */
  def toDataset(): Dataset[U] = {
    dataset
  }

  /**
   * Convert this dataset to a DataFrame.
   *
   * @return this dataset converted to a DataFrame
   */
  def toDF(): DataFrame = {
    dataset.toDF()
  }

  /**
   * Convert this dataset to a JavaRDD.
   *
   * @return this dataset converted to a JavaRDD
   */
  def toJavaRDD(): JavaRDD[T] = {
    jrdd
  }

  /**
   * Convert this dataset to an RDD.
   *
   * @return this dataset converted to an RDD
   */
  def toRDD(): RDD[T] = {
    rdd
  }

  /**
   * Transform this dataset by the specified function over DataFrames.
   *
   * @param tFn transform function over DataFrames
   * @return this dataset transformed by the specified function over DataFrames
   */
  def transformDataFrame(tFn: DataFrame => DataFrame)(
    implicit uTag: TypeTag[U]): V = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transformDataset((ds: Dataset[U]) => {
      tFn(ds.toDF()).as[U]
    })
  }

  /**
   * Transform this dataset by the specified function over Datasets.
   *
   * @param tFn transform function over Datasets
   * @return this dataset transformed by the specified function over Datasets
   */
  def transformDataset(tFn: Dataset[U] => Dataset[U]): V

  /**
   * Transform this dataset by the specified function over RDDs.
   *
   * @param tFn transform function over RDDs
   * @return this dataset transformed by the specified function over RDDs
   */
  def transformRdd(tFn: RDD[T] => RDD[T]): V

  /**
   * (Scala-specific) Transmute this dataset by the specified function over DataFrames.
   * The conversion function is implicit.
   *
   * @param tFn transmute function over DataFrames
   * @return this dataset transmuted by the specific function over DataFrames
   */
  def transmuteDataFrame[X, Y <: Product, Z <: NamelessDataset[X, Y, Z]](
    tFn: DataFrame => DataFrame)(
      implicit yTag: TypeTag[Y],
      convFn: (V, Dataset[Y]) => Z): Z = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transmuteDataset[X, Y, Z]((ds: Dataset[U]) => {
      tFn(ds.toDF()).as[Y]
    })
  }

  /**
   * (Java-specific) Transmute this dataset by the specified function over DataFrames.
   *
   * @param tFn transmute function over DataFrames, must not be null
   * @param convFn conversion function, must not be null
   * @return this dataset transmuted by the specified function over DataFrames
   */
  def transmuteDataFrame[X, Y <: Product, Z <: NamelessDataset[X, Y, Z]](
    tFn: Function[DataFrame, DataFrame],
    convFn: NamelessDatasetConversion[T, U, V, X, Y, Z]): Z = {
    requireNonNull(tFn)
    requireNonNull(convFn)
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    transmuteDataFrame[X, Y, Z](tFn.call(_))(convFn.yTag,
      (v: V, dsY: Dataset[Y]) => {
        convFn.call(v, dsY)
      })
  }

  /**
   * (Scala-specific) Transmute this dataset by the specified function over Datasets.
   * The conversion function is implicit.
   *
   * @param tFn transmute function over Datasets
   * @return this dataset transmuted by the specific function over Datasets
   */
  def transmuteDataset[X, Y <: Product, Z <: NamelessDataset[X, Y, Z]](
    tFn: Dataset[U] => Dataset[Y])(
      implicit yTag: TypeTag[Y],
      convFn: (V, Dataset[Y]) => Z): Z = {
    convFn(this.asInstanceOf[V], tFn(dataset))
  }

  /**
   * (Java-specific) Transmute this dataset by the specified function over Datasets.
   *
   * @param tFn transmute function over Datasets, must not be null
   * @param convFn conversion function, must not be null
   * @return this dataset transmuted by the specified function over Datasets
   */
  def transmuteDataset[X, Y <: Product, Z <: NamelessDataset[X, Y, Z]](
    tFn: Function[Dataset[U], Dataset[Y]],
    convFn: NamelessDatasetConversion[T, U, V, X, Y, Z]): Z = {
    requireNonNull(tFn)
    requireNonNull(convFn)
    val tfn: Dataset[U] => Dataset[Y] = tFn.call(_)
    val cfn: (V, Dataset[Y]) => Z = convFn.call(_, _)
    transmuteDataset[X, Y, Z](tfn)(convFn.yTag, cfn)
  }

  /**
   * (Scala-specific) Transmute this dataset by the specified function over RDDs.
   * The conversion function is implicit.
   *
   * @param tFn transmute function over RDDs
   * @return this dataset transmuted by the specific function over RDDs
   */
  def transmuteRdd[X, Y <: Product, Z <: NamelessDataset[X, Y, Z]](tFn: RDD[T] => RDD[X])(
    implicit convFn: (V, RDD[X]) => Z): Z = {
    convFn(this.asInstanceOf[V], tFn(rdd))
  }

  /**
   * (Java-specific) Transmute this dataset by the specified function over JavaRDDs.
   *
   * @param tFn transmute function over JavaRDDs, must not be null
   * @param convFn conversion function, must not be null
   * @return this dataset transmuted by the specified function over JavaRDDs
   */
  def transmuteRdd[X, Y <: Product, Z <: NamelessDataset[X, Y, Z]](
    tFn: Function[JavaRDD[T], JavaRDD[X]],
    convFn: Function2[V, RDD[X], Z]): Z = {
    requireNonNull(tFn)
    requireNonNull(convFn)
    convFn.call(this.asInstanceOf[V], tFn.call(jrdd).rdd)
  }

  /**
   * (Scala-specific) Union together multiple datasets.
   *
   * @param datasets datasets to union with this dataset
   * @return return the union of this dataset and the specified datasets
   */
  def union(datasets: V*): V

  /**
   * (Java-specific) Union together multiple datasets.
   *
   * @param datasets list of datasets to union with this dataset, must not be null
   * @return the union of this dataset and the specified list of datasets
   */
  def union(datasets: java.util.List[V]): V = {
    requireNonNull(datasets)
    val datasetSeq: Seq[V] = datasets.toSeq
    union(datasetSeq: _*)
  }
}
