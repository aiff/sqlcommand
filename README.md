# sqlcommand   极客时间练习
实现 Compact table command
要求：
添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB。
语法：
COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES]；
说明：
基本要求是完成以下功能：COMPACT TABLE test1 INTO 500 FILES；
如果添加 partitionSpec，则只合并指定的 partition 目录的文件；
如果不加 into fileNum files，则把表中的文件合并成 128MB 大小。



 
## 1.修改g4文件
## 2.运行 Maven -> Spark Project Catalyst -> antlr4 -> antlr4:antlr4
## 3.SparkSqlParser.scala 添加代码   
```
#  D:\spark-3.2.0\spark-3.2.0\sql\catalyst\target\generated-sources\antlr4\org\apache\spark\sql\catalyst\parser\SqlBaseParser.java
/
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val table: TableIdentifier = visitTableIdentifier(ctx.tableIdentifier())
    val fileNum: Option[Int] = ctx.INTEGER_VALUE().getText.toInt
    CompactTableCommand(table, fileNum)
  }
```
## 4.放在src/main/scala/org/apache/spark/sql/execution/command
```
case class CompactTableCommand(table: TableIdentifier,fileNum: Option[Int]) extends LeafRunnableCommand {
override def output: Seq[Attribute] = Seq(AttributeReference("no_return", StringType, false)())
override def run(spark: SparkSession): Seq[Row] = {

 
val dataDF: DataFrame = spark.table(table)
val num: Int = fileNum match {
  case Some(i) => i
  case _ =>
    (spark
      .sessionState
      .executePlan(dataDF.queryExecution.logical)
      .optimizedPlan
      .stats.sizeInBytes / (1024L * 1024L * 128L)
      ).toInt
}
log.warn(s"fileNum is $num")
val tmpTableName = table.identifier+"_tmp"
dataDF.write.mode(SaveMode.Overwrite).saveAsTable(tmpTableName)
spark.table(tmpTableName).repartition(num).write.mode(SaveMode.Overwrite).saveAsTable(table.identifier)
spark.sql(s"drop table if exists $tmpTableName")
log.warn("Compacte Table Completed.")
Seq()

```
