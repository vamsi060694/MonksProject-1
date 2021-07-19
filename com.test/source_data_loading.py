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
    app_config_path = os.path.abspath(current_dir  + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)


    src_list = app_conf['source_list']
    for src in src_list:
        src_config = app_conf[src]
        stg_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + src
        if src == 'SB':
            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            mysql_transaction_df = ut.mysql_SB_data_load(spark,app_secret,src_config)
            mysql_transaction_df.show()
            mysql_transaction_df.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)

    # SFTP source
        elif src == 'OL':
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            sftp_loyalty_df = ut.sftp_data_load(spark,
                                                app_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv",
                                                app_secret)\
                .withColumn('ins_dt', current_date())
            sftp_loyalty_df.show(5, False)
            sftp_loyalty_df.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)

    #MongoDB Source
        elif src == 'CP':
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            mongo_customer_df = ut.mongo_data_load(spark,app_conf["mongodb_config"]["database"],
                                                   app_conf["mongodb_config"]["collection"]
                                                   ,app_secret,src_config)
            mongo_customer_df.show(5,False)
            mongo_customer_df.write.partitionBy('ins_dt').mode('overwrite').parquet()

    #S3 Source
        elif src == 'CP':
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            s3_campaigns_df = ut.sftp_data_load(spark,app_conf,app_secret,src_config)
            s3_campaigns_df.show(5,False)
            s3_campaigns_df.write.partitionBy('ins_dt').mode('overwrite').parquet()



# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/com.test/others/systems/mysql_df.py
