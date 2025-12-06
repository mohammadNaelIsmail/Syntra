import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkToES")
      .config("spark.master", "local[*]")
      .config("es.nodes", "192.168.88.14")                // IP أو hostname الخاص بـ ES
      .config("es.port", "9200")                          // Port
      .config("es.net.http.auth.user", "elastic")         // اسم المستخدم
      .config("es.net.http.auth.pass", "15u7bMLAf=srLFvjJCeg")      // كلمة السر
      .config("es.net.ssl", "true")                       // إذا تستخدم HTTPS
      .config("es.nodes.wan.only", "true")               // مهم إذا الاتصال من خارج cluster
      .config("es.index.auto.create", "true")            // لإنشاء index تلقائياً إذا غير موجود
      .getOrCreate()
    val df = spark.read.json(
      "/home/mohammad/IdeaProjects/LinkHarvester/src/main/resources/people_1000_development.json"
    )

    df.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", "192.168.88.14")                   // نفس الـ IP الموجود في SparkSession
      .option("es.port", "9200")
      .option("es.net.ssl", "true")
      .option("es.net.ssl.cert.allow.self.signed", "true")   // لأنك تستخدم cert self-signed
      .option("es.net.http.auth.user", "elastic")
      .option("es.net.http.auth.pass", "15u7bMLAf=srLFvjJCeg")
      .option("es.resource", "people/_doc")
      .option("es.index.auto.create", "true")
      .save()



    spark.stop()
  }
}
