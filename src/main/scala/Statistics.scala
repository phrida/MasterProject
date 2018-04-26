import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.TimerTask

class Statistics extends TimerTask {

  var count: Long = 0L
  var startTime: Long = System.currentTimeMillis()
  var file: Path = Paths.get("/Users/Phrida/PlotTest/tps.csv")
  Files.write(file, "TIME,\tTPS".getBytes(), StandardOpenOption.APPEND)

  def log(count: Long): Unit = {
    println("Start time: " + startTime)
    var timeDelta = (System.currentTimeMillis() - startTime) / 1000
    println("Time delta: " + timeDelta)
    val logEntry = timeDelta.toString + ",\t" + count.toString + "\n"

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
    log(count);
    count = 0L;
  }
}
