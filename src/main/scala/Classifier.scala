import java.util.Timer

class Classifier {

  var statistics: Statistics = _

  def initialize(): Unit = {
    this.statistics = new Statistics
    var timer: Timer = new Timer()
    timer.schedule(this.statistics, 0, 1000)
  }

  def classify(count: Long): Unit = {
    statistics.addToCount(count)
  }

  def setTime(time: Double): Unit = {
    statistics.getTime(time)
  }

  def totalTPS(count: Long): Unit = {
    statistics.addTotal(count)
  }



}
