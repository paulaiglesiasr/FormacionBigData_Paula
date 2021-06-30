package cap3

import org.apache.spark.sql.SparkSession

object ejercicios_extra {
  def leer_csv(): Unit ={

    val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .getOrCreate()

    //val schema = "CallNumber int,UnitID int,IncidentNumber int,CallType string,CallDate date,WatchDate date,CallFinalDisposition string,AvailableDtTm date,Address string,City string,Zipcode int,Battalion string,StationArea,Box,OriginalPriority,Priority,FinalPriority,ALSUnit,CallTypeGroup,NumAlarms,UnitType,UnitSequenceInCallDispatch,FirePreventionDistrict,SupervisorDistrict,Neighborhood,Location,RowID,Delay"

  val df = spark.read.format("csv")
//    .schema(schema)
    .option("header","true")
    .load("Datos/cap3/sf-fire-calls.csv")
    .show()
  }
}
