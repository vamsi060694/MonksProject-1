from pyspark.sql import SparkSession
import yaml
import os.path
import utils.aws_utils as ut
from pyspark.sql.functions import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read com.test enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    src_list = app_conf['source_list']
    for src in src_list:
        src_config = app_conf[src]
        stg_path = 's3a://' + app_conf['s3_conf']['staging_location'] + src
        if src == 'SB':
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                          "lowerBound": "1",
                          "upperBound": "100",
                          "dbtable": src_config["mysql_conf"]["query"],
                          "numPartitions": "2",
                          "partitionColumn": src_config["mysql_conf"]["partition_column"],
                          "user": app_secret["mysql_conf"]["username"],
                          "password": app_secret["mysql_conf"]["password"]
                           }

            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            txnDF = spark\
                .read.format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .options(**jdbc_params)\
                .load()\
                .withColumn('ins_dt', current_date())

            txnDF.show()

            txnDF.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)
        elif src == 'OL':
            #SFTP source
            ol_txn_df = spark.read \
                .format("com.springml.spark.sftp") \
                .option("host", app_secret["sftp_conf"]["hostname"]) \
                .option("port", app_secret["sftp_conf"]["port"]) \
                .option("username", app_secret["sftp_conf"]["username"]) \
                .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"])) \
                .option("fileType", "csv") \
                .option("delimiter", "|") \
                .load(app_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv") \
                .withColumn('ins_dt', current_date())

            ol_txn_df.show(5, False)

            ol_txn_df.write.partitionBy('ins_dt').mode('overwrite').parquet()

    #MongoDB Source

    students = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", app_conf["mongodb_config"]["database"]) \
        .option("collection", app_conf["mongodb_config"]["collection"]) \
        .load()\
        .withColumn('ins_dt', current_date())


    students.show()

    students.write.partitionBy('ins_dt').mode('overwrite').parquet()

    #S3 Source

    campaigns = spark \
                .read \
                .csv("s3://monksworkspace/data/KC_Extract_1_20171009.csv") \
                .withColumn('ins_dt', current_date())

    campaigns.show(5,False)
    campaigns.write.partitionBy('ins_dt').mode('overwrite').parquet()





# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/com.test/others/systems/mysql_df.py
