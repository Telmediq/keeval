import boto3
import os
import sys

from botocore.exceptions import ClientError


class S3ConfigStore(object):

    def __init__(self, profile, bucket_name, prefix=None, delimiter='.'):
        self.aws_session = boto3.session.Session(profile_name=profile)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter

    @property
    def s3(self):
        return self.aws_session.resource('s3')

    def _preprocess_key(self, key):
        return key.replace(self.delimiter, '/')

    def read(self, key):

        key = self._preprocess_key(key)

        if self.prefix:
            key = '%s/%s' % (self.prefix, key)
        try:
            obj = self.s3.Object(self.bucket_name, key)
            return obj.get()['Body'].read().strip()
        except ClientError as e:
            sys.stderr.write("Could not read key: ", e.response['Error']['Code'])


    def write(self, key, data):
        key = self._preprocess_key(key)

        if self.prefix:
            key = '%s/%s' % (self.prefix, key)
        try:
            obj = self.s3.Bucket(self.bucket_name).put_object(Key=key, Body=data)
            return "Success"
        except ClientError as e:
            sys.stderr.write("Could not write key: ", e.response['Error']['Code'])
            sys.exit(1)


def run():
    # Look for AWS_PROFILE
    if 'AWS_PROFILE' not in os.environ:
        sys.stderr.write("Please set the environment variable AWS_PROFILE.\n")
        sys.exit(1)
    if 'KEEVAL_BUCKET_NAME' not in os.environ:
        sys.stderr.write("Please set the environment variable KEEVAL_BUCKET_NAME.\n")
        sys.exit(1)

    aws_profile = os.environ['AWS_PROFILE']
    bucket_name = os.environ['KEEVAL_BUCKET_NAME']
    store = S3ConfigStore(aws_profile, bucket_name)

    try:
        action = sys.argv[1]
        key = sys.argv[2]
    except IndexError:
        sys.stderr.write("Please specify both an action and a key.")
        sys.exit(1)

    if action == 'read':
        sys.stdout.write(store.read(key).decode("utf-8"))
        sys.exit(0)

    elif action == 'write':
        data = sys.stdin.read()
        sys.stdout.write(store.write(key, data))
        sys.exit(0)

    else:
        sys.stderr.write("Action must be read or write.")
        sys.exit(1)

if __name__ == "__main__":
    run()
