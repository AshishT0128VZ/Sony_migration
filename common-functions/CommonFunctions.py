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


class AWSUtils:

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

    def get_dynamo_values_from_key(self, table, col_key, key, col_value):
        """
        The function takes 4 parameters. The table, the column name for the key, the key
        and the column name for the value to get
        It returns the list of values or raises an error if something goes wrong
        """
        dynamodb = boto3.resource('dynamodb')
        try:
            table = dynamodb.Table(table)
            response = table.get_item(
                Key={
                    col_key: key
                }
            )
        except Exception as error:
            raise error
        item = response['Item'] if ('Item' in response) else []
        return item[col_value] if (col_value in item) else []

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


class GlueReader:
    """
    Initialize class with Glue context
    """

    def __init__(self, glueContext):
        if glueContext is not None:
            self.__glueContext = glueContext
        else:
            raise Exception('The Glue context is null')

    def get_source_from_s3(self, bucket_name, object_key, format, logFormat="", StrictMode="", with_header="",
                           skip_first="", separator="", compression="", bookmark_name=""):
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
            transformation_ctx=bookmark_name
        ).toDF()

    def get_source_from_jdbc(self, conn_type, jdbc_url, username, password, table, temp_dir, jdbc_s3_path="",
                             jdbc_class_name="", hashfield="", hashexpression="", hashpartitions="", bookmark_name=""):
        """
        The function takes 12 parameters. The connection type, the jdbc url, the database username,
        the database password, the database table, the temporary directory in glue S3 bucket for staging data,
        the jdbc s3 path (optional), the jdbc class name (optional), the hash field for partitioning (optional),
        the hash expression for partitioning when controlling the partitions in the where clause (optional),
        the hash partitions for the number of parallel reads (default is 7) (optional)
        and the job bookmark name/transformation context (optional)
        It returns the dataframe of the data fetched from the database
        """
        conn_opts_dict = {
            "url": jdbc_url,
            "user": username,
            "password": password,
            "dbtable": table,
            "redshiftTmpDir": temp_dir
        }
        if jdbc_s3_path and jdbc_class_name:
            conn_opts_dict['customJdbcDriverS3Path'] = jdbc_s3_path
            conn_opts_dict['customJdbcDriverClassName'] = jdbc_class_name
        elif jdbc_s3_path or jdbc_class_name:
            raise Exception('Custom JDBC usage requires JDBC S3 path and JDBC class name')
        if hashfield:
            conn_opts_dict['hashfield'] = hashfield
        if hashexpression:
            conn_opts_dict['hashexpression'] = hashexpression
        if hashpartitions:
            conn_opts_dict['hashpartitions'] = hashpartitions

        return self.__glueContext.create_dynamic_frame_from_options(
            connection_type=conn_type,
            connection_options=conn_opts_dict,
            transformation_ctx=bookmark_name
        ).toDF()


"""
This class and it's functions are used to read data from sources using spark
Required Inputs is the Spark session
"""


class SparkReader:
    """
    Initialize class with Spark session
    """

    def __init__(self, spark):
        if spark is not None:
            self.__spark = spark
        else:
            raise Exception('The Spark session is null')

    def get_source_from_jdbc(self, jdbc_url, username, password, query, jdbc_class_name,
                             fetchsize="", partition_column="", lower_bound="", upper_bound="", num_partitions="", custom_schema="",
                             oracle_map_date_timestamp="", session_init_statement=""):
        """
        The function takes 13 parameters. The connection type, the jdbc url, the database username,
        the database password, the database table/query, the jdbc class name, the fetch size for read (optional),
        the partition column name (optional), the lower bound for partitioning (optional),
        the upper bound for partitioning (optional), the num partitions (optional), the custom schema (optional),
        the oracle map date to timestamp (optional) and the session init statement (optional)
        It returns the dataframe of the data fetched from the database
        """
        properties_dict = {
            "user": username,
            "password": password,
            "driver": jdbc_class_name
        }
        if partition_column and lower_bound and upper_bound and num_partitions:
            properties_dict['partitionColumn'] = partition_column
            properties_dict['lowerBound'] = lower_bound
            properties_dict['upperBound'] = upper_bound
            properties_dict['numPartitions'] = num_partitions
        elif partition_column or lower_bound or upper_bound or num_partitions:
            raise Exception('Partition column, lower bound, upper bound and num partitions must be either all defined '
                            'or none of them defined')
        if fetchsize:
            properties_dict['fetchsize'] = fetchsize
        if custom_schema:
            properties_dict['customSchema'] = custom_schema

        # For oracle it is not possible to use timestamp as partition column without this options
        if "oracle" in jdbc_class_name:
            if oracle_map_date_timestamp and session_init_statement:
                properties_dict['oracle.jdbc.mapDateToTimestamp'] = oracle_map_date_timestamp
                properties_dict['sessionInitStatement'] = session_init_statement
            elif oracle_map_date_timestamp or session_init_statement:
                raise Exception('Oracle map date to timestamp and session init statement must be either all defined '
                                'or none of them defined')

        return self.__spark.read.jdbc(jdbc_url, query, properties=properties_dict)


"""
This class and it's functions are used to read a fixed width file.
Required Inputs are a list of column sizes, list of column names and the dataframe which has the complete record in a single column
"""


class FixedWidthReader:
    """
    Initialize class with list of colNames & colSizes
    """

    def __init__(self, colNames, colSizes):

        if (len(colNames) == len(colSizes)):
            self.__colNames = colNames
            self.__colSizes = colSizes
        else:
            raise Exception('The number of column names are not equal to the number of column sizes')

    def __get_instance_usage_schema(self):
        """
        get instance usage schema.
        """
        columns_struct_fields = [StructField(field_name, StringType(), True) for field_name in self.__colNames]
        schema = StructType(columns_struct_fields)
        return schema

    def __convert_string_to_row(self, row_string):
        """
        split row into schema specific columns
        :param row_string:
        :return:
        """
        list = []
        flist = []
        index = 0
        colSizes = self.__colSizes
        for i in range(0, len(colSizes)):
            list.append((index, colSizes[i] + index))
            index = index + colSizes[i]
        for j in list:
            flist.append(row_string[j[0]:j[1]])
        return flist

    def getfw_dataframe(self, df):
        """
        """

        schema = self.__get_instance_usage_schema()
        df = df.rdd.map(lambda x: x[0]) \
            .filter(lambda x: len(x) > 0) \
            .map(self.__convert_string_to_row).toDF(schema=schema)
        return df


"""
This class and its functions are used to select the right roles inside Glue jobs in order to use Core and Local locations
"""


class RoleSelector:
    """
    Initialize class with Spark context
    """

    def __init__(self, sc):
        if sc is not None:
            self.__sc = sc
        else:
            raise Exception('The Spark context is null')

    """
    The function takes 2 parameters. The role arn to be chosen, the kms key arn to be used for that role (optional)
    It assumes the input role in the caller
    """

    def config_role(self, role_arn, kms_key_arn=None):
        self.__sc._jsc.hadoopConfiguration().set("fs.s3.customAWSCredentialsProvider", "com.bmw.cdh.AssumeRoleCredentialProvider")
        self.__sc._jsc.hadoopConfiguration().set("fs.s3.assumed.role.arn", role_arn)
        self.__sc._jsc.hadoopConfiguration().set("fs.s3.assumed.role.session.name", "GlueSession")
        self.__sc._jsc.hadoopConfiguration().set("fs.s3.enableServerSideEncryption", "true")

        if kms_key_arn is not None:
            self.__sc._jsc.hadoopConfiguration().set("fs.s3.serverSideEncryption.kms.keyId", kms_key_arn)


"""
This class and its functions are used to write dataframe to dynamodb
all column are first zipped into a json
each json attribute is written as attributes
"""


class DynamoWriter:
    """
    Initialize class with parameters provided by the caller
    aws_region=
    ddb_table_name=
    overwrite_by_pkeys=
    """

    def __init__(self, aws_region, ddb_table_name, overwrite_by_pkeys, repartition_count):
        self.__aws_region = aws_region
        self.__ddb_table_name = ddb_table_name
        self.__overwrite_by_pkeys = overwrite_by_pkeys
        self.__repartition_count = repartition_count

    def __batch_ddb_writer(self, iterator):
        # Get DynamoDB table name
        ddb = boto3.resource('dynamodb', region_name=self.__aws_region)
        table = ddb.Table(self.__ddb_table_name)
        for x in iterator:
            # Get the record value from the spark.sql.Row() type
            record_json = json.loads(x.__getitem__('_1'))
            with table.batch_writer(overwrite_by_pkeys=self.__overwrite_by_pkeys) as batch:
                batch.put_item(Item=record_json)
        return iterator

    """
    Takes dataframe as an input.
    If successful then returns 200 or 400
    """

    def ddb_writer(self, df):
        df = df.toJSON().map(lambda x: (x,)).toDF()
        res = df.rdd.mapPartitions(self.__batch_ddb_writer).collect()
        if len(res) == 0:
            return 200
        else:
            return 400


"""
This class and its functions are used to abstract the usage of the  Encryption and decryption API
"""


class Anonymize:
    def __init__(self, aws_region, endpoint_host, action, endpoint_stage, api_key, arn_auth_role, dpu_no):
        """
        Initialize class with parameters provided by the caller
        aws_region=
        endpoint_host=
        endpoint_stage=
        api_key=
        arn_auth_role
        """
        self.__aws_region = aws_region
        self.__endpoint_host = endpoint_host
        self.__endpoint_stage = endpoint_stage
        self.__api_key = api_key
        self.__arn_auth_role = arn_auth_role
        self.__dpu_no = dpu_no
        self.__action = action
        self.__endpoint = 'https://{}/{}/{}'.format(endpoint_host, endpoint_stage, action)

    def __get_prepared_request(self):
        """
        Get Prepared Request
        """
        sts = boto3.client('sts')
        response = sts.assume_role(RoleArn=self.__arn_auth_role, RoleSessionName="API-GW-Invoke")
        auth = AWSRequestsAuth(aws_access_key=response["Credentials"]["AccessKeyId"],
                               aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
                               aws_token=response["Credentials"]["SessionToken"], aws_host=self.__endpoint_host,
                               aws_region=self.__aws_region, aws_service='execute-api')
        pr_request = requests.Request(method='POST', url=self.__endpoint, auth=auth, headers={"x-api-key": self.__api_key})
        return pr_request

    def __call_api(self, anonymize_columns, retry=0):
        """
        Make a call to the API based on the action requested by the caller.
        pseudonymisations or reidentifications
        """
        pr_request = self.__get_prepared_request()
        anonymize_columns = [str(col_val) for col_val in anonymize_columns]
        anonymize_columns_distinct = list(set(anonymize_columns))
        anonymize_columns_distinct = [i for i in anonymize_columns_distinct if i and str(i).strip() != '']
        result = ''
        if self.__action == "pseudonymisations":
            pr_request.json = {"identifiers": anonymize_columns_distinct}
            final_request = pr_request.prepare()
            session = requests.Session()
            result = session.send(final_request).json().get('pseudonyms')
            if result is None or result == "None":
                if retry < 100:
                    return self.__call_api(anonymize_columns, retry + 1)
                else:
                    raise Exception(
                        'Failed inside pseudonymisations lambda call. anonymize_columns_distinct : {}, result : {}, retry : {}'.format(
                            anonymize_columns_distinct, result, retry
                        )
                    )
        else:
            pr_request.json = {"pseudonyms": anonymize_columns_distinct}
            final_request = pr_request.prepare()
            session = requests.Session()
            result = session.send(final_request).json().get('identifiers')
            if result is None or result == "None":
                if retry < 100:
                    return self.__call_api(anonymize_columns, retry + 1)
                else:
                    raise Exception(
                        'Failed inside reidentifications lambda call. anonymize_columns_distinct : {}, result : {}, retry : {}'.format(
                            anonymize_columns_distinct, result, retry
                        )
                    )
        if type(anonymize_columns_distinct) != type([]) or type(result) != type([]):
            raise Exception(
                'Failed inside lambda call. anonymize_columns_distinct : {}, result : {}, retry : {}'.format(anonymize_columns_distinct,
                                                                                                             result, retry))
        mapping_dict = dict(zip(anonymize_columns_distinct, result))
        final_anonymize_columns = [mapping_dict[i_d] if i_d in mapping_dict.keys() else '' for i_d in anonymize_columns]
        return final_anonymize_columns

    def __do_anonymous(self, iterator, batch_size, column_names, original_columns_order):
        def fun_row_change_column_value(row, column_name, column_value):
            d = row.asDict()
            OrderedRow = Row(*original_columns_order)
            d[column_name] = column_value
            output_row = OrderedRow(*[d[i] for i in original_columns_order])
            return output_row

        def call_lambda_combine_result(column, chunk):
            request_list = [row[column] for row in chunk]
            response_list = self.__call_api(request_list)
            return [fun_row_change_column_value(row, column, result) for row, result in zip(chunk, response_list)]

        def multiple_column_anonymous(e_chunk, col_names):
            if len(col_names) > 0:
                ec = col_names[0]
                er_chunk = call_lambda_combine_result(ec, e_chunk)
                return multiple_column_anonymous(er_chunk, col_names[1:])
            else:
                return e_chunk

        chunk_it = [iterator[i:i + batch_size] for i in range(0, len(iterator), batch_size)]
        anonymized_chunk_it = [multiple_column_anonymous(a_chunk, column_names) for a_chunk in chunk_it]
        return list(chain.from_iterable(anonymized_chunk_it))

    def get_dataframe(self, data_frame, column_names):
        """
        The function takes 2 parameters. Dataframe and List of Columns that need to be anonymized.
        It returns a dataframe with anonymized data.
        """
        # init
        batch_size = 900
        repartition_count = (((self.__dpu_no - 1) * 2) - 1) * 4 * 4
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        # input df column in order
        original_columns_order = data_frame.columns

        # map function
        def fun(iterator): yield self.__do_anonymous(list(iterator), batch_size, column_names, original_columns_order)

        original_schema = data_frame.schema
        processed_rdd = data_frame \
            .rdd \
            .repartition(repartition_count) \
            .mapPartitions(fun) \
            .flatMap(lambda x: x)

        output_df = spark.createDataFrame(processed_rdd, schema=original_schema).cache()
        return output_df


"""
This class and it's functions are used to create and get a dicionary with countries and their alpha2 codes
"""


class Countries:
    def __init__(self):
        self.__country_dict = {"AF": "Afghanistan",
                               "AX": "Aland Islands",
                               "AL": "Albania",
                               "DZ": "Algeria",
                               "AS": "American Samoa",
                               "AD": "Andorra",
                               "AO": "Angola",
                               "AI": "Anguilla",
                               "AQ": "Antarctica",
                               "AG": "Antigua and Barbuda",
                               "AR": "Argentina",
                               "AM": "Armenia",
                               "AW": "Aruba",
                               "AU": "Australia",
                               "AT": "Austria",
                               "AZ": "Azerbaijan",
                               "BS": "Bahamas",
                               "BH": "Bahrain",
                               "BD": "Bangladesh",
                               "BB": "Barbados",
                               "BY": "Belarus",
                               "BE": "Belgium",
                               "BZ": "Belize",
                               "BJ": "Benin",
                               "BM": "Bermuda",
                               "BT": "Bhutan",
                               "BO": "Bolivia, Plurinational State of",
                               "BQ": "Bonaire, Sint Eustatius and Saba",
                               "BA": "Bosnia and Herzegovina",
                               "BW": "Botswana",
                               "BV": "Bouvet Island",
                               "BR": "Brazil",
                               "IO": "British Indian Ocean Territory",
                               "BN": "Brunei Darussalam",
                               "BG": "Bulgaria",
                               "BF": "Burkina Faso",
                               "BI": "Burundi",
                               "KH": "Cambodia",
                               "CM": "Cameroon",
                               "CA": "Canada",
                               "CV": "Cape Verde",
                               "KY": "Cayman Islands",
                               "CF": "Central African Republic",
                               "TD": "Chad",
                               "CL": "Chile",
                               "CN": "China",
                               "CX": "Christmas Island",
                               "CC": "Cocos (Keeling) Islands",
                               "CO": "Colombia",
                               "KM": "Comoros",
                               "CG": "Congo",
                               "CD": "Congo, The Democratic Republic of the",
                               "CK": "Cook Islands",
                               "CR": "Costa Rica",
                               "CI": "Côte d'Ivoire",
                               "HR": "Croatia",
                               "CU": "Cuba",
                               "CW": "Curaçao",
                               "CY": "Cyprus",
                               "CZ": "Czech Republic",
                               "DK": "Denmark",
                               "DJ": "Djibouti",
                               "DM": "Dominica",
                               "DO": "Dominican Republic",
                               "EC": "Ecuador",
                               "EG": "Egypt",
                               "SV": "El Salvador",
                               "GQ": "Equatorial Guinea",
                               "ER": "Eritrea",
                               "EE": "Estonia",
                               "ET": "Ethiopia",
                               "FK": "Falkland Islands (Malvinas)",
                               "FO": "Faroe Islands",
                               "FJ": "Fiji",
                               "FI": "Finland",
                               "FR": "France",
                               "GF": "French Guiana",
                               "PF": "French Polynesia",
                               "TF": "French Southern Territories",
                               "GA": "Gabon",
                               "GM": "Gambia",
                               "GE": "Georgia",
                               "DE": "Germany",
                               "GH": "Ghana",
                               "GI": "Gibraltar",
                               "GR": "Greece",
                               "GL": "Greenland",
                               "GD": "Grenada",
                               "GP": "Guadeloupe",
                               "GU": "Guam",
                               "GT": "Guatemala",
                               "GG": "Guernsey",
                               "GN": "Guinea",
                               "GW": "Guinea-Bissau",
                               "GY": "Guyana",
                               "HT": "Haiti",
                               "HM": "Heard Island and McDonald Islands",
                               "VA": "Holy See (Vatican City State)",
                               "HN": "Honduras",
                               "HK": "Hong Kong",
                               "HU": "Hungary",
                               "IS": "Iceland",
                               "IN": "India",
                               "ID": "Indonesia",
                               "IR": "Iran, Islamic Republic of",
                               "IQ": "Iraq",
                               "IE": "Ireland",
                               "IM": "Isle of Man",
                               "IL": "Israel",
                               "IT": "Italy",
                               "JM": "Jamaica",
                               "JP": "Japan",
                               "JE": "Jersey",
                               "JO": "Jordan",
                               "KZ": "Kazakhstan",
                               "KE": "Kenya",
                               "KI": "Kiribati",
                               "KP": "Korea, Democratic People's Republic of",
                               "KR": "Korea, Republic of",
                               "KW": "Kuwait",
                               "KG": "Kyrgyzstan",
                               "LA": "Lao People's Democratic Republic",
                               "LV": "Latvia",
                               "LB": "Lebanon",
                               "LS": "Lesotho",
                               "LR": "Liberia",
                               "LY": "Libya",
                               "LI": "Liechtenstein",
                               "LT": "Lithuania",
                               "LU": "Luxembourg",
                               "MO": "Macao",
                               "MK": "Macedonia, Republic of",
                               "MG": "Madagascar",
                               "MW": "Malawi",
                               "MY": "Malaysia",
                               "MV": "Maldives",
                               "ML": "Mali",
                               "MT": "Malta",
                               "MH": "Marshall Islands",
                               "MQ": "Martinique",
                               "MR": "Mauritania",
                               "MU": "Mauritius",
                               "YT": "Mayotte",
                               "MX": "Mexico",
                               "FM": "Micronesia, Federated States of",
                               "MD": "Moldova, Republic of",
                               "MC": "Monaco",
                               "MN": "Mongolia",
                               "ME": "Montenegro",
                               "MS": "Montserrat",
                               "MA": "Morocco",
                               "MZ": "Mozambique",
                               "MM": "Myanmar",
                               "NA": "Namibia",
                               "NR": "Nauru",
                               "NP": "Nepal",
                               "NL": "Netherlands",
                               "NC": "New Caledonia",
                               "NZ": "New Zealand",
                               "NI": "Nicaragua",
                               "NE": "Niger",
                               "NG": "Nigeria",
                               "NU": "Niue",
                               "NF": "Norfolk Island",
                               "MP": "Northern Mariana Islands",
                               "NO": "Norway",
                               "OM": "Oman",
                               "PK": "Pakistan",
                               "PW": "Palau",
                               "PS": "Palestinian Territory, Occupied",
                               "PA": "Panama",
                               "PG": "Papua New Guinea",
                               "PY": "Paraguay",
                               "PE": "Peru",
                               "PH": "Philippines",
                               "PN": "Pitcairn",
                               "PL": "Poland",
                               "PT": "Portugal",
                               "PR": "Puerto Rico",
                               "QA": "Qatar",
                               "RE": "Réunion",
                               "RO": "Romania",
                               "RU": "Russian Federation",
                               "RW": "Rwanda",
                               "BL": "Saint Barthélemy",
                               "SH": "Saint Helena, Ascension and Tristan da Cunha",
                               "KN": "Saint Kitts and Nevis",
                               "LC": "Saint Lucia",
                               "MF": "Saint Martin (French part)",
                               "PM": "Saint Pierre and Miquelon",
                               "VC": "Saint Vincent and the Grenadines",
                               "WS": "Samoa",
                               "SM": "San Marino",
                               "ST": "Sao Tome and Principe",
                               "SA": "Saudi Arabia",
                               "SN": "Senegal",
                               "RS": "Serbia",
                               "SC": "Seychelles",
                               "SL": "Sierra Leone",
                               "SG": "Singapore",
                               "SX": "Sint Maarten (Dutch part)",
                               "SK": "Slovakia",
                               "SI": "Slovenia",
                               "SB": "Solomon Islands",
                               "SO": "Somalia",
                               "ZA": "South Africa",
                               "GS": "South Georgia and the South Sandwich Islands",
                               "ES": "Spain",
                               "LK": "Sri Lanka",
                               "SD": "Sudan",
                               "SR": "Suriname",
                               "SS": "South Sudan",
                               "SJ": "Svalbard and Jan Mayen",
                               "SZ": "Swaziland",
                               "SE": "Sweden",
                               "CH": "Switzerland",
                               "SY": "Syrian Arab Republic",
                               "TW": "Taiwan, Province of China",
                               "TJ": "Tajikistan",
                               "TZ": "Tanzania, United Republic of",
                               "TH": "Thailand",
                               "TL": "Timor-Leste",
                               "TG": "Togo",
                               "TK": "Tokelau",
                               "TO": "Tonga",
                               "TT": "Trinidad and Tobago",
                               "TN": "Tunisia",
                               "TR": "Turkey",
                               "TM": "Turkmenistan",
                               "TC": "Turks and Caicos Islands",
                               "TV": "Tuvalu",
                               "UG": "Uganda",
                               "UA": "Ukraine",
                               "AE": "United Arab Emirates",
                               "GB": "United Kingdom",
                               "US": "United States",
                               "UM": "United States Minor Outlying Islands",
                               "UY": "Uruguay",
                               "UZ": "Uzbekistan",
                               "VU": "Vanuatu",
                               "VE": "Venezuela, Bolivarian Republic of",
                               "VN": "Viet Nam",
                               "VG": "Virgin Islands, British",
                               "VI": "Virgin Islands, U.S.",
                               "WF": "Wallis and Futuna",
                               "EH": "Western Sahara",
                               "YE": "Yemen",
                               "ZM": "Zambia",
                               "ZW": "Zimbabwe"}

    def get_country_by_code(self, alpha2):
        """
        The function takes 1 parameter. The alpha2 to find the respective country name
        It returns the country name, None if the alpha2 is None, empty string if alpha2 is empty string
        or Invalid Code if there is no map for the country code in the countries dictionary
        """
        if alpha2 is None:
            return None
        elif alpha2 == "":
            return ""
        else:
            return self.__country_dict.get(alpha2, "Invalid Code")


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


"""
This class and it's functions are used to make mappings of data frame columns using
expressions stated in configuration files
"""


class Mapping:
    """
    Initialize class with the Spark Session, Spark SQL Context
    and the Dictionary Configuration file S3 Buket and Key info
    """

    def __init__(self, spark, sparkSQLContext, sourceDictionaryS3Bucket, sourceDictionaryS3Key):
        if sparkSQLContext is not None:
            self.__sparkSQLContext = sparkSQLContext
        else:
            raise Exception('The Spark SQL Context is null')
        if spark is not None:
            self.__spark = spark
        else:
            raise Exception('The Spark session is null')
        if ((sourceDictionaryS3Bucket is not None) and (sourceDictionaryS3Key is not None)):
            self.__sourceDictionaryS3Bucket = sourceDictionaryS3Bucket
            self.__sourceDictionaryS3Key = sourceDictionaryS3Key
            s3_client = boto3.client('s3')
            try:
                s3_client_object = s3_client.get_object(
                    Bucket=sourceDictionaryS3Bucket, Key=sourceDictionaryS3Key)
                s3_client_data = s3_client_object['Body'].read().decode(
                    'utf-8')
                self.__sourceDictionary = json.loads(s3_client_data)
            except Exception as error:
                raise error
        else:
            raise Exception(
                'The Mappting Configuration S3 Path and Key can not be null')

    def getSourceDictionary(self):
        """
        The function takes not parameters
        Just to retrieve parsed mapping configuration if needed
        """
        return self.__sourceDictionary

    def __intersection(self, lst1, lst2):
        lst1_upp = [x.upper() for x in lst1]
        lst2_upp = [x.upper() for x in lst2]
        return list(set(lst1_upp) & set(lst2_upp))

    def __combine(self, df_left, df_right):
        left_fields = df_left.columns
        right_fields = df_right.columns

        for l_name in list(set(left_fields)-set(right_fields)):
            df_right = df_right.withColumn(l_name, lit(None))

        for r_name in list(set(right_fields)-set(left_fields)):
            df_left = df_left.withColumn(r_name, lit(None))

        df_right = df_right.select(df_left.columns)
        return df_right.union(df_left)

    def retrieveAsStandardColumns(self, source_specification, source_database, original_columns="drop"):
        """
        The function takes 3 parameters; source_specification, source_database, original_columns (optional).
        Original columns can be either set to keep or drop, to include or exclude original columns.
        It returns the dataframe containing the standardized column names based on the configuration
        """
        if source_database is None:
            raise Exception(
                'source_database parameter can not be Null')

        if (source_database.strip() == ''):
            raise Exception(
                'source_database parameter can not be empty')

        if original_columns not in ['keep', 'drop']:
            raise Exception(
                'original_columns input paramater can only be set to "keep" or "drop"')

        if not type(source_specification) is dict:
            raise Exception(
                'source_specification should be in dictionary type')

        if 'domain' not in source_specification:
            raise Exception(
                'source_specification should include at least a domain criteria')

        domain_output = {}
        filtered_types = []
        for domains in self.__sourceDictionary:
            if source_specification['domain'] == domains['domain']:
                domain_output = domains['output']
                for dtype in domains['types']:
                    matched = True
                    for criteria in source_specification:
                        if (criteria != 'domain'):
                            if criteria not in dtype:
                                matched = False
                            else:
                                if source_specification[criteria] != dtype[criteria]:
                                    matched = False
                    if (matched):
                        filtered_types.append(dtype)

        schema_str = ' StructType([ '
        for column in domain_output:
            if domain_output[column] not in ['String', 'Binary', 'Boolean', 'Date', 'Timestamp',
                                             'Decimal', 'Double', 'Float', 'Byte', 'Integer', 'Long', 'Short', 'Array', 'Map']:
                raise Exception(
                    "output column data types should be valid spark data type")
            schema_str = schema_str + \
                'StructField("' + column.upper() + '", ' + \
                domain_output[column] + 'Type() , True),'
        schema_str = schema_str[:-1] + '])'
        schema = eval(schema_str)
        result_df = self.__spark.createDataFrame([], schema)

        for dtype in filtered_types:
            filter_exp = ""
            self.__sparkSQLContext.sql("use `" + source_database.strip() + "`")
            if 'source_sql' in dtype:
                crt_view = "CREATE OR REPLACE TEMPORARY VIEW `" + dtype['source_name'].strip() + "` AS " + " ".join(dtype['source_sql'])
                self.__sparkSQLContext.sql(crt_view)
            if 'filter_exp' in dtype:
                if (len(dtype['filter_exp'].strip()) > 0):
                    filter_exp = " where " + dtype['filter_exp']
            df = self.__sparkSQLContext.sql(" select * from `" + dtype['source_name'].strip() + "`" + filter_exp)
            for column in dtype['columnStandardization']:
                df = df.withColumn("n_e_w_" + column.upper(),
                                   expr(dtype['columnStandardization'][column]))
            for static_attribute in dtype:
                if static_attribute not in ["filter_exp", "columnStandardization", "source_sql"]:
                    df = df.withColumn(
                        "n_e_w_" + static_attribute.upper(), expr("'" + dtype[static_attribute] + "'"))
            new_column_name_list = list(map(lambda x:  x[6:] if x.startswith(
                'n_e_w_') else "orig_" + x.upper(), df.columns))
            df = df.toDF(*new_column_name_list)
            if (original_columns == "keep"):
                new_columns = list(map(lambda x: x.upper(), domain_output.keys())) + list(
                    map(lambda y: y.upper(), filter(lambda x: x.startswith('orig_'), df.columns)))
                select_columns = self.__intersection(df.columns, new_columns)
                result_df = self.__combine(
                    result_df, df.select(select_columns))
            else:
                select_columns = self.__intersection(
                    df.columns, list(domain_output.keys()))
                result_df = self.__combine(
                    result_df, df.select(select_columns))

        if len(filtered_types) == 0:
            raise Exception(
                'source_specification did not match with any dictionary record')

        return result_df
