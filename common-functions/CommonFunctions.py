import json
import time
from functools import reduce
from itertools import chain
from urllib.parse import urlparse

import boto3
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import BinaryType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DateType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from pyspark.sql.types import ByteType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import ShortType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import MapType


"""
This class and it's functions are used to get or modify resources from AWS services
"""


class AWSCommonFunctions:

    def get_secret(self, secret_id, region):
        """
        The function takes 2 parameters. The secret id to get the secret from secrets manager
        and the region of the secret
        It returns the secret or null if the secret id doesn't exist
        """
        client = boto3.client("secretsmanager", region_name=region)
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_id)
            secret = get_secret_value_response['SecretString']
        except Exception as error:
            raise error
        return secret

    def upload_files_to_s3(self, bucket, key, file_path):
        """
        The function takes 3 parameters. The bucket, the key where the file will be uploaded in the bucket
        and the file to upload
        It uploads the file to the bucket or raises an error if something goes wrong
        """
        s3 = boto3.resource('s3')
        try:
            s3.Object(bucket, key).put(
                Body=open(file_path, 'rb')
            )
        except Exception as error:
            raise error

    def update_dynamo_values_from_key(self, table, col_key, key, col_value, values_list):
        """
        The function takes 5 parameters. The table, the column name for the key, the key, the column name for the value
        and the values list to update in the dynamo table values
        It updates the dynamo table or raises an error if something goes wrong
        """
        dynamodb = boto3.resource('dynamodb')
        try:
            table = dynamodb.Table(table)
            table.update_item(
                Key={
                    col_key: key
                },
                UpdateExpression='set ' + col_value + ' = list_append(if_not_exists(' + col_value + ', :empty_list), :val1)',
                ExpressionAttributeValues={
                    ':val1': values_list,
                    ':empty_list': []
                }
            )
        except Exception as error:
            raise error

    def copy_parts_and_clean(self, sourceS3path, targetS3path, copyWithKMSEncryption, partition=None):
    	"""
    	This function takes 4 parameters. The source S3 bucket and prefix, the target S3 bucket and prefix,
    	and a boolean flag showing whether the copy operation would preserve the origin KMS key or not,
        and partition string to limit the delete and copy operation.
    	The function copies produced spark part/result files from the source S3 path to the target S3 path.
    	It will overwrite the target path and delete the source path content after the copy operation.
    	"""
    	p_sourcebucket = urlparse(sourceS3path, allow_fragments=False).netloc
    	p_sourcekeyprefix = urlparse(
    		sourceS3path, allow_fragments=False).path.strip('/')
    	p_targetbucket = urlparse(targetS3path, allow_fragments=False).netloc
    	p_targetkeyprefix = urlparse(
    		targetS3path, allow_fragments=False).path.strip('/')
    	# sleeping, just to get over potential S3 consistency issues after write
    	time.sleep(20)
    	s3_client = boto3.client('s3')
    	s3_resource = boto3.resource('s3')
    	kwargs = {'Bucket': p_sourcebucket, 'Prefix': p_sourcekeyprefix + '/'}
    	paginator = s3_client.get_paginator("list_objects_v2")
    	targetbucket = s3_resource.Bucket(p_targetbucket)
    	if(partition is None):
    		targetbucket.objects.filter(Prefix=p_targetkeyprefix+'/').delete()
    	else:
    		for obj in targetbucket.objects.filter(Prefix=p_targetkeyprefix+'/'):
    			if(obj.key.find('/' + str(partition) + '/')>=0):
    				obj.delete()
    	for page in paginator.paginate(**kwargs):
    		try:
    			contents = page["Contents"]
    		except KeyError:
    			break
    		for obj in contents:
    			sourceabsolutekey = obj['Key']
    			if(partition is None):
    			    sourcefilename = sourceabsolutekey.split("/")[-1]
    			else:
    			    sourcefilename = "/".join(sourceabsolutekey.split("/")[-3:])
    			sourceobject = {
                    'Bucket': p_sourcebucket,
    				'Key': sourceabsolutekey
    			}
    			if ('$folder$' not in sourcefilename):
    				if (copyWithKMSEncryption):
    					source_object_summary = s3_client.head_object(
    						Bucket=p_sourcebucket, Key=sourceabsolutekey)
    					kmskey = source_object_summary.get('SSEKMSKeyId')
    					targetbucket.copy(sourceobject, p_targetkeyprefix + "/" + sourcefilename,
    						ExtraArgs={"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kmskey})
    				else:
    					targetbucket.copy(sourceobject, p_targetkeyprefix + "/" + sourcefilename)
    	sourcebucket = s3_resource.Bucket(p_sourcebucket)
    	sourcebucket.objects.filter(Prefix=p_sourcekeyprefix+'/').delete()


"""
This class and it's functions are used to read data from Glue catalogs or from options (s3, jdbc)
Required Inputs is the Glue context
"""


class GlueFunctions:
    """
    Initialize class with Glue context
    """

    def __init__(self, glueContext):
        if glueContext is not None:
            self.__glueContext = glueContext
        else:
            raise Exception('The Glue context is null')

    def get_source_from_s3(self, bucket_name, object_key, format, logFormat="", StrictMode="", with_header="",
                           skip_first="", separator="", compression=""):
        """
        The function takes 10 parameters. The bucket name, object key, the format of the source,
        the log format option (optional), the strict mode option (optional), with_header format option (optional),
        skip_first format option (optional), separator format option (optional), the compression type (optional)
        and the job bookmark name/transformation context (optional)
        It returns the dataframe of the s3 data
        """
        conn_opts_dict = {'paths': ['s3://' + bucket_name + '/' + object_key]}
        if compression:
            conn_opts_dict['compression'] = compression

        fmt_opts_dict = {}
        if format == "grokLog":
            if logFormat:
                fmt_opts_dict['logFormat'] = logFormat
            else:
                raise Exception('LogFormat is required for grokLog format')
            if StrictMode:
                fmt_opts_dict['StrictMode'] = StrictMode
        elif format == "csv" or format == "parquet":
            if with_header:
                fmt_opts_dict['withHeader'] = with_header
            if skip_first:
                fmt_opts_dict['skipFirst'] = skip_first
            if separator:
                fmt_opts_dict['separator'] = separator
        else:
            raise Exception('Format not supported by the common functions library yet')

        return self.__glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options=conn_opts_dict,
            format=format,
            format_options=fmt_opts_dict,
        ).toDF()
                            

"""
This class and it's functions are used to make dataframe operations
"""


class DataframeOperations:

    def get_col_data_type(self, df, col_name):
        """
        The function takes 2 parameters. The df and column to get the data type
        It returns the data type of the column
        """
        return [data_type for name, data_type in df.dtypes if name == col_name][0]

    def trim_df(self, df):
        """
        The function takes 1 parameter. The df to trim all its columns (both right and left)
        It returns the trimmed df
        """
        for c_name in df.columns:
            col_data_type = self.get_col_data_type(df, c_name)
            df = df.withColumn(c_name, trim(df[c_name]).cast(col_data_type))
        return df

    def upper_case_df(self, df):
        """
        The function takes 1 parameter. The df to modify all its columns values and names to upper case
        It returns the df with all the columns and data in upper case
        """
        for c_name in df.columns:
            col_data_type = self.get_col_data_type(df, c_name)
            df = df.withColumn(c_name, upper(df[c_name]).cast(col_data_type))
        df = df.toDF(*[c.upper() for c in df.columns])
        return df

    def empty_string_to_null(self, x):
        """
        The function takes 1 parameter. The column to modify all the empty values or with just spaces
        It returns the column with empty or just spaces values as NULL
        """
        return when(trim(col(x)) != "", col(x)).otherwise(None)

    def empty_string_to_null_df(self, df):
        """
        The function takes 1 parameter. The df to modify all the empty values or with just spaces
        It returns the df with empty or just spaces values as NULL
        """
        for c_name in df.columns:
            col_data_type = self.get_col_data_type(df, c_name)
            df = df.withColumn(c_name, self.empty_string_to_null(c_name).cast(col_data_type))
        return df

    def cast_column(self, df, col, data_type):
        """
        The function takes 3 parameters. The df and column to cast and the data type
        It returns the df with that column casted to the new data type
        """
        df = df.withColumn(col, df[col].cast(data_type))
        return df

    def union_dataframes(self, dfs):
        """
        The function takes 1 parameter. The list of dataframes to union
        It returns the final union dataframe
        """
        return reduce(DataFrame.union, dfs)
