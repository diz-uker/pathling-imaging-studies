import datetime
import math
import os
import shutil
import time

import pyspark
from pathling import PathlingContext, Expression as exp
from pathling.etc import find_jar
from pydantic import BaseSettings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, first, regexp_replace, to_date, udf
from pyspark.sql.types import StringType


class Settings(BaseSettings):
    output_folder: str = "~/opal-output"
    output_filename: str = "analysis-results.csv"
    kafka_topic_year_suffix: str = ""
    kafka_imaging_study_topic: str = "fhir.pacs.imagingStudy"
    partition_a: str = "0"
    partition_b: str = "1"
    # ⚠️ make sure these are consistent with the ones downloaded inside the Dockerfile
    jar_list: list = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
        "au.csiro.pathling:library-api:6.2.1",
        "ch.cern.sparkmeasure:spark-measure_2.13:0.21",
        "io.delta:delta-core_2.12:2.3.0",
    ]
    spark_app_name: str = "imaging_study_analysis"
    master: str = "local[*]"
    kafka_bootstrap_server: str = "kafka:9092"

    spark_worker_memory: str = "24g"
    spark_executor_memory: str = "18g"
    spark_driver_memory: str = "16g"
    spark_executor_cores: str = "6"

    spark_jars_ivy: str = "/home/spark/.ivy2"


settings = Settings()


def setup_spark_session(appName: str, master: str):
    spark = (
        SparkSession.builder.appName(appName)
        .master(master)
        .config("spark.ui.port", "4040")
        .config("spark.rpc.message.maxSize", "1000")
        .config("spark.worker.memory", settings.spark_worker_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.cores", settings.spark_executor_cores)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages", ",".join(settings.jar_list))
        .config("spark.jars.ivy", settings.spark_jars_ivy)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.network.timeout", "1000000")
        .config("spark.driver.maxResultSize", "8g")
        .config("spark.sql.broadcastTimeout", "1200s")
        .config("spark.executor.heartbeatInterval", "1200s")
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark "
            + "-XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails "
            + "-XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
        )
        .getOrCreate()
    )
    spark.sparkContext.addFile(find_jar())
    return spark


def create_list_of_kafka_topics():
    return (
        settings.kafka_imaging_study_topic
        + settings.kafka_topic_year_suffix
    )


def read_data_from_kafka_save_delta(spark: SparkSession):
    kafka_imaging_study_topic = settings.kafka_imaging_study_topic
    partition_a = settings.partition_a
    partition_b = settings.partition_b
    assign_string = '{"'+kafka_imaging_study_topic+ '":[' + partition_a + "," + partition_b +"]}"
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_server)
        .option("assign", assign_string)
        .load()
    )
    kafka_data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    return kafka_data


def save_final_df(final_df, df_name):
    output_folder = settings.output_folder
    partition_a = settings.partition_a
    partition_b = settings.partition_b
    output_file = str(df_name + "_partition_" + partition_a + "-" +
                      partition_b + ".csv")
    print("output_file = ", output_file)

    final_df_pandas = final_df.toPandas()

    output_path_filename = os.path.join(
        output_folder, output_file
    )
    print(output_path_filename)
    print("###### current dir: ", os.getcwd())
    print("###### output_path_filename : ", output_path_filename)

    final_df_pandas.to_csv(output_path_filename)


def encode_imaging_studies(ptl, kafka_data):

    imaging_data_kafka = ptl.encode_bundle(kafka_data
                                           .select("value"), 'ImagingStudy')
    imaging_data_kafka_dataset = ptl.read.datasets({
        "ImagingStudy": imaging_data_kafka})

    imaging_studies = imaging_data_kafka_dataset.extract(
        "ImagingStudy", columns=[
            exp("numberOfSeries", "no_series"),
            exp("numberOfInstances", "no_instances")
        ]
    )

    imaging_series = imaging_data_kafka_dataset.extract("ImagingStudy", columns=[
        exp("series.bodySite.code", "bodysite"),
        exp("series.modality.code", "modality"),
        exp("series.laterality.code", "laterality"),
        exp("series.numberOfInstances", "s_no_instances")
        ]
    )

    studies_no = imaging_studies.count()
    series_no = imaging_series.count()
    instance_no = imaging_series.agg({"s_no_instances": "sum"}).collect()[0][0]
    print("Total number of studies: "+str(studies_no))
    print("Total number of series: "+str(series_no))
    print("Total number of instances: "+str(instance_no))

    result_bodysite = imaging_series.groupBy("bodysite").count().orderBy("count", ascending=False)
    save_final_df(result_bodysite, "result_bodysite")
    result_bodysite_with_modality = imaging_series.filter(col("modality") != "SR").filter(col("modality") != "DOC").filter(col("modality") != "SEG").filter(col("modality") != "PR").filter(col("modality") != "KO").filter(col("modality") != "OT").filter(col("modality") != "REG").filter(col("modality") != "SC").filter(col("modality") != "RTSTRUCT").groupBy("bodysite").count().orderBy("count", ascending=False)
    save_final_df(result_bodysite_with_modality, "result_bodysite_with_modality")
    result_laterality = imaging_series.groupBy("laterality").count()
    save_final_df(result_laterality, "result_laterality")
    result_laterality_L_bodySite = imaging_series.filter(col("laterality") == "L").groupBy("bodySite").count().orderBy("count", ascending=False)
    save_final_df(result_laterality_L_bodySite, "result_laterality_L_bodySite")
    result_laterality_R_bodySite = imaging_series.filter(col("laterality") == "R").groupBy("bodySite").count().orderBy("count", ascending=False)
    save_final_df(result_laterality_R_bodySite, "result_laterality_R_bodySite")
    result_modality = imaging_series.groupBy("modality").count().orderBy("count", ascending=False)
    save_final_df(result_modality, "result_modality")
    result_instances_modality = imaging_series.groupBy("modality").agg({"s_no_instances": "avg"}).orderBy("avg(s_no_instances)", ascending=False)
    save_final_df(result_instances_modality, "result_instances_modality")
    result_instances_bodysite = imaging_series.filter(col("bodysite") == "10200004").agg({"s_no_instances": "avg"})
    save_final_df(result_instances_bodysite, "result_instances_bodysite")
    result_modality_bodysite = imaging_series.filter(col("modality") == "XA").groupBy("bodysite").count().orderBy("count", ascending=False)
    save_final_df(result_modality_bodysite, "result_modality_bodysite")
    result_series = imaging_studies.agg({"no_series": "avg"})
    save_final_df(result_series, "result_series")
    result_instances = imaging_studies.agg({"no_instances": "avg"})
    save_final_df(result_instances, "result_instances")


def main():
    start = time.monotonic()

    spark = setup_spark_session(settings.spark_app_name, settings.master)
    ptl = PathlingContext.create(spark=spark, enable_extensions=True)

    kafka_data = read_data_from_kafka_save_delta(spark)

    encode_imaging_studies(ptl, kafka_data)
    print("done")

    end = time.monotonic()
    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
