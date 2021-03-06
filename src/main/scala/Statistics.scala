import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.TimerTask

class Statistics extends TimerTask {

  var count: Long = 0L
  var time: Double = 0L
  var total: Long = 0L
  var startTime: Long = System.currentTimeMillis()

  var file1: Path = Paths.get("/home/spnorrha/PlotResults/tps.csv")
  var file2: Path = Paths.get("/home/spnorrha/PlotResults/keywords.csv")

  Files.write(file1, "TIME,\tTOTAL,\tCOUNT\n".getBytes(), StandardOpenOption.APPEND)
  Files.write(file2, "TIME,\tCOUNT,\tMATCH\n".getBytes(), StandardOpenOption.APPEND)

  def log1(count: Long, total: Long): Unit = {
    var timeDelta = (System.currentTimeMillis() - startTime) / 1000
    val logEntry = timeDelta.toString + ",\t" + total.toString + ",\t" + count.toString + "\n"

    try {
      Files.write(file1, logEntry.getBytes(), StandardOpenOption.APPEND)
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }

  def log2(deltaTime: Double, count: Long): Unit = {
    val keywords = TweetGenerator.readFromFile().size
    val logEntry = deltaTime.toString + ",\t" + keywords.toString + ",\t" + count.toString + "\n"
    try {
      Files.write(file2, logEntry.getBytes(), StandardOpenOption.APPEND)
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }


  def addToCount(i: Long): Unit = {
    count += i
  }

  def getTime(time: Double): Unit = {
    this.time = time
  }

  def addTotal(i: Long): Unit = {
    total += i
  }


  override def run(): Unit = {

    //For load test
    log1(count, total)
    count=0L
    total=0L

    //For keyword test
    log2(time, count)
    time = 0L
    count = 0L





  }
}
