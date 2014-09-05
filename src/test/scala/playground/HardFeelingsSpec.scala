package playground

import org.specs2.mutable._
import org.apache.spark._

class HardFeelingsSpec extends Specification {
  "HardFeelings" should {
    "identify harshest words" in {
      val sc = new SparkContext(DefaultConf("harsh words").setMaster("local[2]"))
      try {
        {
          val (harshWords, niceWords) = HardFeelings.harshestAndNicest(sc)
          harshWords must contain("bastard")
          niceWords must contain("superb")
        } must not(throwAn[Exception])
      } finally {
        sc.stop
        System.clearProperty("spark.master.port")
      }
    }
  }
}
