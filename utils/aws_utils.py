import yaml
import os.path
import utils.aws_utils as ut
from pyspark.sql.functions import *



def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


#JDBC connection URL for Mysql connection string
def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)



#Source data loading functions:
def mysql_SB_data_load(spark,app_secret,src_config):
    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": src_config["mysql_conf"]["query"],
                   "numPartitions": "2",
                   "partitionColumn": src_config["mysql_conf"]["partition_column"],
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }
    txnDF = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load() \
        .withColumn('ins_dt', current_date())
    return txnDF

def sftp_data_load(spark,file_path,app_secret):
        ol_txn_df = spark.read \
            .format("com.springml.spark.sftp") \
            .option("host", app_secret["sftp_conf"]["hostname"]) \
            .option("port", app_secret["sftp_conf"]["port"]) \
            .option("username", app_secret["sftp_conf"]["username"]) \
            .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"])) \
            .option("fileType", "csv") \
            .option("delimiter", "|") \
            .load(file_path)
        return ol_txn_df

def mongo_data_load(spark,dbName,collName):
        customer_df = spark \
            .read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("database", dbName) \
            .option("collection",collName ) \
            .load() \
            .withColumn('ins_dt', current_date())
        return customer_df

def s3_data_load(spark):
        campaign_df = spark \
                .read \
                .csv("s3://monksworkspace/data/KC_Extract_1_20171009.csv") \
                .withColumn('ins_dt', current_date())
        return campaign_df