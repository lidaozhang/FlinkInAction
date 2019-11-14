package batch

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    val inputPath = "data/text4wordcount.txt";
    val outPut = "data/result4scala.txt";                         //结果文件由代码创建，无需提前创建


    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    //引入隐式转换,
    import org.apache.flink.api.scala._

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(outPut,"\n"," ").setParallelism(1)
    env.execute("batch word count")

    print("词频统计执行结束")
  }

}
