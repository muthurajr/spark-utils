spark.conf.set("spark.sql.parquet.binaryAsString","true")
import org.apache.spark.sql.types._
def checkDataType(schema: StructType): StructType = {
  StructType(schema.fields.map { field =>
    field.dataType match {
      case struct: StructType =>
        field.copy(name = field.name, dataType = checkDataType(struct))
      case struct: BinaryType =>
        field.copy(name = field.name, dataType = StringType)
      case _ =>
        field.copy(name = field.name)
    }
  })
}

val df = spark.read.parquet("****Parquet File Location")

val df2 = spark.createDataFrame(df.rdd, checkDataType(df.schema))

df2.write.csv("*****Destination Location");
