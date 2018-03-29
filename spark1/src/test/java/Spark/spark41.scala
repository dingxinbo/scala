package Spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


object spark41 {
      def main(args: Array[String]): Unit = {
          val conf = new SparkConf().setAppName("appname").setMaster("local[4]")
        //配置sparkconf  传入到上下文context
          val sc = new SparkContext(conf)
        // 读取本类 本目录resources下的文件 4(1).txt 同时创建rdd file
          val file = sc.textFile(this.getClass.getClassLoader.getResource("4(1).txt").getFile)
        // rdd file 通过filter算子 形成新的rdd warn
          var warn = file.filter(line=>{
              line.contains("3")
          })
        // Action的count算子 最后输出结果
          val count = warn.count()
          println(count)
      }
}