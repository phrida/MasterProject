import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.TimerTask

class Statistics extends TimerTask {

  var count: Long = 0L
  var time: Double = 0L
  var total: Long = 0L
  var startTime: Long = System.currentTimeMillis()
  var file1: Path = Paths.get("/Users/Phrida/PlotResults/tps.csv")
  var file2: Path = Paths.get("/Users/Phrida/PlotResults/keywords.csv")
  var file3: Path = Paths.get("/Users/Phrida/PlotResults/keywords_tps.csv")
  //Files.write(file1, "TIME,\tCOUNT\n".getBytes(), StandardOpenOption.APPEND)
  //Files.write(file2, "TIME,\tCOUNT\n".getBytes(), StandardOpenOption.APPEND)
  Files.write(file3, "#TPS,\t#KEYWORDS\n".getBytes(), StandardOpenOption.APPEND)

  def log1(count: Long): Unit = {
    //println("Start time: " + startTime)
    var timeDelta = (System.currentTimeMillis() - startTime) / 1000
    //println("Time delta: " + timeDelta)
    val logEntry = timeDelta.toString + count.toString + "\n"
    //println("LogEntry: " + logEntry)

    try {
      Files.write(file1, logEntry.getBytes(), StandardOpenOption.APPEND)
    } catch {
      case e: IOException => e.printStackTrace()
    }

  }

  def log2(deltaTime: Double): Unit = {
    val keywords = TweetGenerator.readFromFile().size
    println("Number of keywords: " + keywords)
    val logEntry = deltaTime.toString + ",\t" + keywords.toString + "\n"
    try {
      Files.write(file2, logEntry.getBytes(), StandardOpenOption.APPEND)
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }

  def log3(count: Long): Unit = {
    val keywords = TweetGenerator.readFromFile().size
    println("Number of keywords: " + keywords)
    val logEntry = count.toString + ",\t" + keywords.toString + "\n"
    try {
      Files.write(file3, logEntry.getBytes(), StandardOpenOption.APPEND)
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

  def getTotalTPS(j: Long): Unit = {
    total += j
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

    /*For batchsize = 1
    log1(count)
    count=0L*/

    /*For keyword test
    log2(time)
    time = 0L*/

    //For keyword_tps test
    log3(count)
    count=0L


  }
}
