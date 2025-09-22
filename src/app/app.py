import os

from pyspark.sql.types import *
import config
import queries, io_cluster
import udfs
import patterns

schema = StructType([
    StructField("tên công việc", StringType(), True),
    StructField("tên công ty", StringType(), True),
    StructField("Địa điểm công việc", StringType(), True),
    StructField("Mức lương", StringType(), True),
    StructField("Kinh nghiệm", StringType(), True),
    StructField("mô tả công việc", StringType(), True),
    StructField("kĩ năng yêu cầu", StringType(), True),
    StructField("thông tin liên hệ", StringType(), True),
    StructField("loại công việc", StringType(), True),
    StructField("cấp bậc", StringType(), True),
    StructField("học vấn", StringType(), True),
    StructField("giới tính", StringType(), True),
    StructField("tuổi", StringType(), True),
    StructField("ngành nghề", StringType(), True),
])


if __name__ == "__main__":
    APP_NAME = "PreprocessData"

    app_config = config.Config(elasticsearch_host="elasticsearch",
                               elasticsearch_port="9200",
                               elasticsearch_input_json="yes",
                               hdfs_namenode="hdfs://node01:8020"
                               )
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext
    sc.addPyFile(os.path.dirname(__file__) + "/patterns.py")
    # print()

    raw_recruit_df = spark.read.schema(schema).option("multiline", "true").json(
        "hdfs://node01:8020/datasource/*.json")
    extracted_recruit_df = raw_recruit_df.select(raw_recruit_df["tên công việc"].alias("JobName"),
                                                 raw_recruit_df['tên công ty'].alias("CompanyName"),
                                                 udfs.extract_location("Địa điểm công việc").alias("Location"),
                                                 udfs.extract_exp_pattern('Kinh nghiệm').alias("Experience"),
                                                 raw_recruit_df['loại công việc'].alias("JobType"),
                                                 raw_recruit_df["cấp bậc"].alias('Level'),
                                                 udfs.extract_education("học vấn", "kĩ năng yêu cầu").alias('Education'),
                                                 raw_recruit_df["giới tính"].alias('Sex'),
                                                 udfs.extract_old_pattern("tuổi").alias("Old"),
                                                 udfs.extract_framework_plattform("mô tả công việc",
                                                                                  "kĩ năng yêu cầu").alias(
                                                     "FrameworkPlattforms"),
                                                 udfs.extract_IT_language("mô tả công việc", "kĩ năng yêu cầu").alias(
                                                     "JobLanguages"),
                                                 udfs.extract_language("kĩ năng yêu cầu").alias("Languages"),
                                                 udfs.extract_design_pattern("mô tả công việc",
                                                                             "kĩ năng yêu cầu").alias("DesignPatterns"),
                                                 udfs.extract_knowledge("mô tả công việc", "kĩ năng yêu cầu").alias(
                                                     "Knowledges"),
                                                 udfs.normalize_salary("Mức lương").alias("Salaries"),
                                                 raw_recruit_df['thông tin liên hệ'].alias("Contact"),
                                                udfs.extract_job_type("ngành nghề").alias("JobSummary"),
                                                 ).withColumn('Knowledge', udfs.get_grouped_knowledge("Knowledges"))
    print('extract successuly!!!!')
    extracted_recruit_df.cache()
    extracted_recruit_df.show(5)

    salaries_not_null= queries.get_not_null_salary(extracted_recruit_df)
    salaries_not_null.show(5)

    # ##========save extracted_recruit_df to hdfs========================
    df_to_hdfs = (extracted_recruit_df,)
    df_hdfs_name = ("extracted_recruit",)
    io_cluster.save_dataframes_to_hdfs("/extracted_data", app_config, df_to_hdfs, df_hdfs_name)




    # ##========make some query==========================================
    # knowledge_df = queries.get_counted_knowledge(extracted_recruit_df)
    # knowledge_df.cache()
    # knowledge_df.show(5)

    # udfs.broadcast_labeled_knowledges(sc, patterns.labeled_knowledges)
    # grouped_knowledge_df =
    # grouped_knowledge_df.show(10)

    # extracted_recruit_df = extracted_recruit_df.drop("Knowledges")
    # extracted_recruit_df.cache()

    ##========save some df to elasticsearch========================
    df_to_elasticsearch = (
        extracted_recruit_df,
        salaries_not_null
        # grouped_knowledge_df
    )

    df_es_indices = (
        "recruit",
        'salaries'
        # "grouped_knowledges"
    )
    # extracted_recruit_df.show(5)
    io_cluster.save_df_to_elastic(df_to_elasticsearch, df_es_indices, app_config)