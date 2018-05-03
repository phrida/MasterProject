import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.TimerTask

class Statistics extends TimerTask {

  var count: Long = 0L
  var total: Long = _
  var startTime: Long = System.currentTimeMillis()
  var file: Path = Paths.get("/home/spnorrha/PlotResults/tps.csv")
  Files.write(file, "TIME,\tTPS\n".getBytes(), StandardOpenOption.APPEND)

  def log(count: Long): Unit = {
    //println("Start time: " + startTime)
    var timeDelta = (System.currentTimeMillis() - startTime) / 1000
    //println("Time delta: " + timeDelta)
    val logEntry = timeDelta.toString + ",\t" + count.toString + "\n"
    //println("LogEntry: " + logEntry)

    try {
      Files.write(file, logEntry.getBytes(), StandardOpenOption.APPEND)
    } catch {
      case e: IOException => e.printStackTrace()
    }

  }

  def addToCount(i: Long): Unit = {
    count += i
  }



  override def run(): Unit = {

    //For batchsize = 3
    /*if (count > 0) {
      total = count/3
      log(total)
      count = 0L
    } else if (count == 0)  {
      log(total)
    }*/

    //For batchsize = 1
    log(count)
    count=0L

  }
}
