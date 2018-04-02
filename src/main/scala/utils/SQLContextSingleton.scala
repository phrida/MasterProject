package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SQLContextSingleton {

  private var instance: SQLContext = _

  def getInstance(sc: SparkContext): SQLContext = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = SQLContext.getOrCreate(sc)
        }
      }
    }
    instance
  }

}
