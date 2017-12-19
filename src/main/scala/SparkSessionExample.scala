import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex


object SparkSessionExample {

  case class Wiki(project: String, title: String, accesses: Int, data: Int)

  val excludedWords = Array("Media:", "Special:", "Talk:", "User:", "User_talk:", "Project:",
    "Project_talk:", "File:", "File_talk:", "MediaWiki:", "MediaWiki_talk:", "Template:",
    "Template_talk:", "Help:", "Help_talk:", "Category:", "Category_talk:", "Portal:", "Wikipedia:", "Wikipedia_talk:")

  val lowerCasePattern: Regex = "([a-z]+)".r

  val excludedExtensions = Array(".jpg", ".gif", ".png", ".JPG", ".GIF", ".PNG", ".txt", ".ico")

  val excludedMediaWiki = Array("404_error/", "Main_Page", "Hypertext_Transfer_Protocol", "Search")

  def startWithLowerCase(s: String): Boolean = s match {
    case lowerCasePattern(_) => true
    case _ => false
  }

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder
      .master("local")
      .appName("spark wiki")
      .getOrCreate()

    import session.implicits._

    val dataFrame = session
      .read
      .text("src/main/resources/pageviews*")
      .map(d => {
        val row = d.getString(0).split(" ")
        Wiki(row(0), row(1), row(2).toInt, row(3).toInt)
      })
      .toDF()

    val filtered = dataFrame
      .filter(
        row => row.getString(0).equals("en")
          && !row.getString(1).eq("")
          && !excludedWords.exists(row.getString(1).startsWith)
          && !startWithLowerCase(row.getString(1))
          && !excludedExtensions.exists(row.getString(1).endsWith)
          && !excludedMediaWiki.contains(row.getString(1))
      )
      .groupBy($"title")
      .sum()
      .sort(
        $"sum(accesses)".desc
      )
      .select($"title", $"sum(accesses)")

    filtered.write.csv("src/main/resources/output")

  }

}
