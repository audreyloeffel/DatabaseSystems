
package relation
import relation.shallow._


object GenApp {
  def main(args: Array[String]): Unit = Timing.time({
    val x27 = new RelationScanner("data/En.csv", '|')
    val x28 = RelationScanner.getNumLinesInFile("data/En.csv")
    val x29 = new Array[String](x28)
    val x30 = new Array[String](x28)
    var x31: Int = 0
    val x47 = while({
      val x32 = x27.hasNext()
      x32
    })
    {
      val x39 = x31
      val x40 = x27.next_string()
      val x41 = x29.update(x39, x40)
      val x42 = x27.next_string()
      val x43 = x30.update(x39, x42)
      val x44 = x31
      val x46 = x31 = (x44.+(1))
      x46
    }
    val x50 = new RelationScanner("data/Fr.csv", '|')
    val x51 = RelationScanner.getNumLinesInFile("data/Fr.csv")
    val x52 = new Array[String](x51)
    val x53 = new Array[String](x51)
    var x54: Int = 0
    val x70 = while({
      val x55 = x50.hasNext()
      x55
    })
    {
      val x62 = x54
      val x63 = x50.next_string()
      val x64 = x52.update(x62, x63)
      val x65 = x50.next_string()
      val x66 = x53.update(x62, x65)
      val x67 = x54
      val x69 = x54 = (x67.+(1))
      x69
    }
    var x71: Int = 0
    val x72 = new Range(0, x28, 1)
    val x114 = for(x94 <- 0 until x28) {
      val x95 = new Range(0, x51, 1)
      val x113 = for(x105 <- 0 until x51) {
        val x106 = x30.apply(x94)
        val x107 = x52.apply(x105)
        val x112 = if((x106.==(x107))) 
        {
          val x109 = x71
          val x111 = x71 = (x109.+(1))
          x111
        }
        else
        {
          ()
        }
        
        x112
      }
      x113
    }
    val x115 = x71
    val x116 = new Array[String](x115)
    val x117 = new Array[String](x115)
    val x118 = new Array[String](x115)
    var x119: Int = 0
    val x120 = new Range(0, x28, 1)
    val x230 = for(x176 <- 0 until x28) {
      val x177 = new Range(0, x51, 1)
      val x229 = for(x204 <- 0 until x51) {
        val x205 = x30.apply(x176)
        val x206 = x52.apply(x204)
        val x228 = if((x205.==(x206))) 
        {
          val x218 = x119
          val x219 = x29.apply(x176)
          val x220 = x116.update(x218, x219)
          val x221 = x30.apply(x176)
          val x222 = x117.update(x218, x221)
          val x223 = x53.apply(x204)
          val x224 = x118.update(x218, x223)
          val x225 = x119
          val x227 = x119 = (x225.+(1))
          x227
        }
        else
        {
          ()
        }
        
        x228
      }
      x229
    }
    val x231 = new Range(0, x115, 1)
    val x273 = for(x253 <- 0 until x115) {
      val x264 = x116.apply(x253)
      val x265 = "".+(x264)
      val x266 = x265.+("|")
      val x267 = x117.apply(x253)
      val x268 = x266.+(x267)
      val x269 = x268.+("|")
      val x270 = x118.apply(x253)
      val x271 = x269.+(x270)
      val x272 = println(x271)
      x272
    }
    ()
  }

, "Query Execution")}
