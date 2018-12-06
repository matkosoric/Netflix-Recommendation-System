import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object QualifyingParser {

  def main(args: Array[String]): Unit = {

    val lines = readFileToStringArray ("netflix-data/qualifying.txt")
    val bw = new BufferedWriter(new FileWriter("src/main/resources/qualifying.csv"))

    for (i <- 0 to lines.length -1 ) {
      if (lines(i).contains(":")) {
        var i2 = i+1
        breakable {
          while ((i2 < lines.length)) {
            if (!lines(i2).contains(":")) {
              val newRow = lines(i).substring(0, (lines(i).length() - 1)) + "," + lines(i2)
              bw.write(newRow + "\r\n")
            } else break
            i2 += 1
          }
        }
//        println("Processing line: " + i)
      }
    }
    bw.close()
  }

  @throws(classOf[IOException])
  def readFileToStringArray(canonFilename: String): Array[String] = {
    val bufferedReader = new BufferedReader(new FileReader(canonFilename))
    val lines = new ArrayBuffer[String]()
    var line: String = null
    while ({line = bufferedReader.readLine; line != null}) {
      lines += line
    }
    bufferedReader.close
    lines.toArray
  }

}
