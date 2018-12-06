import java.io._

import scala.io.Source
import scala.util.control.Breaks.{break, breakable}

object QualifyingParser {


  def main(args: Array[String]): Unit = {


    val lines = Source.fromFile("netflix-data/qualifying.txt")
    .getLines()
      .buffered
      .take(2834601)
      .toList

    val bw = new BufferedWriter(new FileWriter("src/main/resources/qualifying.csv"))


    for (i <- 0 to lines.length -1 ) {

      if (lines(i).contains(":")) {


        var i2 = i+1
        breakable {
          while ((i2 < lines.length)) {
            if (!lines(i2).contains(":")) {

              val newRow = lines(i).substring(0, (lines(i).length() - 1)) + ", " + lines(i2)

              bw.write(newRow + "\r\n")


//              bw.print(newRow)
//              bw.print("\r\n")

            } else break
            i2 += 1
          }
        }

        println("Processing line: " + i)

//        batch.foreach(fw.write)

//        fw.write(batch) ;
      }
    }

    bw.close()

  }

}
