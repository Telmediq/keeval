import boto3
import sys

from botocore.exceptions import ClientError
from multiprocessing.pool import ThreadPool


class S3ConfigStore(object):

    def __init__(
            self,
            profile,
            bucket_name,
            prefix=None,
            delimiter='.',
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None
    ):
        self.aws_session = boto3.session.Session(
            profile_name=profile,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter

    @property
    def s3(self):
        return self.aws_session.resource('s3')

    def _preprocess_key(self, key):
        return key.replace(self.delimiter, '/')

    def _dotify_key(self, key):
        return key.replace('/', self.delimiter)

    def read(self, key):

        key = self._preprocess_key(key)
        data = {}
        if self.prefix:
            key = '%s/%s' % (self.prefix, key)
        try:
            obj = self.s3.Object(self.bucket_name, key)
            data[self._dotify_key(key)] = obj.get()['Body'].read().strip().decode('utf8')
            return data
        except ClientError as e:
            sys.stderr.write("Could not read key: " + key + " " + e.response['Error']['Code'] + "\n")
            sys.exit(1)

    def write(self, key, data):
        key = self._preprocess_key(key)

        if self.prefix:
            key = '%s/%s' % (self.prefix, key)
        try:
            obj = self.s3.Bucket(self.bucket_name).put_object(Key=key, Body=data)
            return "Success"
        except ClientError as e:
            sys.stderr.write("Could not write key: " + e.response['Error']['Code'] + "\n")
            sys.exit(1)

    def list(self, key):
        key = self._preprocess_key(key)
        keys_result = []

        if self.prefix:
            key = '%s/%s' % (self.prefix, key)
        try:
            bucket = self.s3.Bucket(self.bucket_name)
            for obj in bucket.objects.filter(Prefix=key):
                keys_result.append(obj.key)
            return keys_result
        except ClientError as e:
            sys.stdout.write("Could not list key: " + e.response['Error']['Code'] + "\n")
            sys.exit(1)

    def read_bulk(self, key_list):
        data_dict = {}
        sys.stderr.write("Reading keys.\n")
        pool = ThreadPool(processes=10)
        data = pool.map(self.read, key_list)
        for i in data:
            for key in i.keys():
                data_dict[key] = i[key]
        return data_dict
