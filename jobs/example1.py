from __future__ import print_function
import logging
import boto3
from botocore.exceptions import ClientError
import csv
import pyspark as spark
from pyspark.sql import SparkSession


if __name__ == '__main__':

    # from pyspark.sql import SparkSession

    # spark = SparkSession.builder \
    #     .master("local") \
    #     .appName("Example1") \
    #     .getOrCreate()
    #
    # df = spark.range(5000).where("id > 500").selectExpr("sum(id)")
    # df.write.csv('D:/Kiran_The_Power_of_Practice3/pyspark_example_project/pyspark-example-project/jobs/loaded_data', mode='overwrite', header=True)
    credentials_path = 'D:\Kiran_The_Power_of_Practice3\pyspark_example_project\pyspark-example-project\configs\credentials_sowmya.csv'
    location = 'ap-south-1'
    BUCKET = 'spark-bucket-edc/'
    FILE = 'Baby_Names_Beginning_2007.csv'


    with open(credentials_path) as f:
        credentials_list = [{k: str(v) for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]


    access_key_id = credentials_list[0]["Access key ID"]
    secret_access_key = credentials_list[0]["Secret access key"]

    s3_client = boto3.resource('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=location)


    def delete_bucket(name):
        pass

    def upload_file(bucket_name, filename):

        s3_client.upload_file("tmp.txt", bucket_name, "key-name")
        # s3_client.Object(bucket_name, 'hello.txt').put(Body=open('/tmp/hello.txt', 'rb'))


    def create_bucket(bucket_name, region=None):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                s3_client = boto3.resource('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.resource('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    # create_bucket("my-edc-bckt","ap-south-1")
    # upload_file("my-edc-bckt", 'edc.csv')

    spark = SparkSession.builder.master("local").appName("CsvReader").getOrCreate()
    # spark = SparkSession \
    #     .builder \
    #     .appName("Demo") \
    #     .config('spark.hadoop.fs.s3a.access.key', 'AKIAS7KLPK5TOSEIXB4T') \
    #     .config('spark.hadoop.fs.s3a.secret.key', "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") \
    #     .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    #     .config('spark.hadoop.fs.s3a.endpoint', 's3-ap-south-1.amazonaws.com') \
    #     .config("com.amazonaws.services.s3a.enableV4", "true") \
    #     .enableHiveSupport() \
    #     .getOrCreate()

    sc = spark.sparkContext
    print('s3://' + BUCKET + FILE)

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAS7KLPK5TOSEIXB4T")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("Dcom.amazonaws.services.s3a.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "false")

    ds_csv = spark.read.format("json").option("header", "true").load('s3a://spark-bucket-edc/colors.json')
    # rdd = sc.textFile('s3a://spark-bucket-edc/Baby_Names_Beginning_2007.csv')
    # rdd.saveAsTextFile("s3a://spark-bucket-edc/python_example_baby_names_s3_not_s3n.csv")

    print(ds_csv.show())


    # sql = SparkSession(sc)
    # csv_df = sql.read.csv('s3a://spark-bucket-edc/Baby_Names_Beginning_2007.csv')

    # ds_csv.cache() # Result is sufficiently small to fit in memory, reuse as basis for subsequent transformations

    # sc=spark.sparkContext
    # hadoop_conf=sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    # hadoop_conf.set("fs.s3n.awsAccessKeyId", 'AKIAS7KLPK5TOSEIXB4T')
    # hadoop_conf.set("fs.s3n.awsSecretAccessKey", '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')
    #
    # df=spark.read.csv("s3n://spark-bucket-edc/Baby_Names_Beginning_2007.csv")
    # df.show()