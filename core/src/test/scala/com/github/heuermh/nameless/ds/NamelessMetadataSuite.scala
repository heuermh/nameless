package com.github.heuermh.nameless.ds

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Dataset

import org.scalatest.FunSuite

import scala.reflect.runtime.universe._

private class Foo {
  // empty
}

private case class FooProduct() {
  // empty
}

private class ReferenceSet {
  // empty
}

private trait WithReferences[T, U <: Product, V <: NamelessDataset[T, U, V]]
    extends WithMetadata {

  val references: ReferenceSet

  override protected def saveMetadata(pathName: String): Unit = {
    saveReferences(pathName)
  }

  def replaceReferences(references: ReferenceSet): V

  protected def saveReferences(pathName: String): Unit
}

private case class FooDataset(references: ReferenceSet)
    extends NamelessDataset[Foo, FooProduct, FooDataset]
    with WithReferences[Foo, FooProduct, FooDataset] {

  @transient val uTag: TypeTag[FooProduct] = typeTag[FooProduct]
  val dataset: Dataset[FooProduct] = null
  val rdd: RDD[Foo] = null

  override def transform(tFn: RDD[Foo] => RDD[Foo]): FooDataset = {
    this
  }

  override def transformDataset(tFn: Dataset[FooProduct] => Dataset[FooProduct]): FooDataset = {
    this
  }

  override def union(datasets: FooDataset*): FooDataset = {
    this
  }

  override def replaceReferences(references: ReferenceSet): FooDataset = {
    this
  }

  override def saveReferences(pathName: String): Unit = {
    // empty
  }
}

class NamelessMetadataSuite extends FunSuite {
  test("demonstrate example with metadata trait") {
    val references = new ReferenceSet()
    val foos = new FooDataset(references)
    assert(foos.references === references)
  }
}
