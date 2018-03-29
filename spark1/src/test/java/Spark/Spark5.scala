package Spark


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}

object Spark5 {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("appname").setMaster("local[4]")
      val sc = new SparkContext(conf)
      val file = sc.textFile(this.getClass.getClassLoader.getResource("file.txt").getFile)

    val result = file.map(JSON.parseArray).flatMap(_.toArray).map(_.asInstanceOf[JSONObject])
  .map(x => (x.getString("dst_as"),x.getString("dst_ip"),x.getString("sampling_algorithm"), x.getString("src_ip"),x.getString("protocol"),x.getString("time_received")))
      result.foreach(x=>println(x))
    }

}
