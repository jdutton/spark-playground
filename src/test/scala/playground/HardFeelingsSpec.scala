package playground

import org.apache.spark._
import org.scalatest._

class HardFeelingsSpec extends WordSpec with Matchers {
  "HardFeelings" should {
    "identify harshest words" in {
      val sc = new SparkContext(DefaultConf("harsh words").setMaster("local[2]"))
      try {
        noException should be thrownBy {
          val (harshWords, niceWords) = HardFeelings.harshestAndNicest(sc)
          harshWords should contain("bastard")
          niceWords should contain("superb")
        }
      } finally {
        sc.stop
        System.clearProperty("spark.master.port")
      }
    }
  }
}
