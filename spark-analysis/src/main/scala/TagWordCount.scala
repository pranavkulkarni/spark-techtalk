import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import play.api.libs.json._
import java.io._

object TagWordCount {
  def main(args: Array[String]) {

    // initialise spark context
    val sparkConf = new SparkConf().setAppName(TagWordCount.getClass.getName)
    val spark = new SparkContext(sparkConf)

    val counts = spark.textFile("datasets.json")
      .map(Json.parse)
      .map(extract)
      .filter(_.length > 0)
      .flatMap(_.map(_.toLowerCase()))
      .map(clean)
      .map((_, 1))
      .countByKey()
      .toSeq
      .sortBy(-_._2)

    save("tags.json", Json.stringify(Json.toJson(counts)))
  }

  def extract (json: JsValue): List[String] = {
    val tags = (json\ "item" \ "tags")

    val filtered = tags.as[List[JsValue]] flatMap {
      case s: JsString => Some(s)
      case _ => None
    }
    filtered.map(_.as[String])
  }

  def clean (input: String): String = {
    input
      .trim()
  }

  def writeError (d: JsDefined): Unit = {
    save("error.txt", d.toString())
  }

  def save (filePath: String, contents: String): Unit = {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(contents)
    bw.close()
  }
}