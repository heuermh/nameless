package com.github.heuermh.nameless

import grizzled.slf4j.Logging
import org.apache.spark.SparkContext

object NamelessContext {
  implicit def sparkContextToNamelessContext(sc: SparkContext): NamelessContext = new NamelessContext(sc)
}

class NamelessContext(@transient val sc: SparkContext) extends Serializable with Logging {
  // empty
}
