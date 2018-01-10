import boto3
import os
import sys
import argparse
import json
from botocore.exceptions import ClientError
from multiprocessing.pool import ThreadPool


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

    def _dotify_key(self,key):
        return key.replace('/', self.delimiter)

    def read(self, key):

        key = self._preprocess_key(key)
        data = {}
        if self.prefix:
            key = '%s/%s' % (self.prefix, key)
        try:
            obj = self.s3.Object(self.bucket_name, key)
            data[self._dotify_key(key)] = obj.get()['Body'].read().strip()
            return data
        except ClientError as e:
            sys.stderr.write("Could not read key: " + e.response['Error']['Code'] + "\n")
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
            for object in bucket.objects.filter(Prefix=key):
                keys_result.append(object.key)
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

def run():


    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('action', help='read or write', nargs='?', choices=('read', 'write'))
    parser.add_argument('--bucket', help='s3 Bucket Name', nargs=1, required=False)
    format = parser.add_mutually_exclusive_group()
    format.add_argument('--key', nargs=1, help="s3 Key Name <something.foo.bar>", required=False)
    format.add_argument('--json', help="JSON from stdin", action="store_true", required=False)
    args = parser.parse_args()

    # Look for AWS_PROFILE
    if 'AWS_PROFILE' in os.environ:
        aws_profile = os.environ['AWS_PROFILE']
    else:
        aws_profile = None

    if args.bucket is not None:
        bucket_name = args.bucket[0]
    elif 'KEEVAL_BUCKET_NAME' in os.environ:
        bucket_name = os.environ['KEEVAL_BUCKET_NAME']
    else:
        sys.stderr.write("Please set the environment variable KEEVAL_BUCKET_NAME.\n")
        sys.exit(1)

    store = S3ConfigStore(aws_profile, bucket_name)

    try:
        action = args.action
    except IndexError:
        sys.stderr.write("Please specify both an action and a key.")
        sys.exit(1)

    if args.json:
        # Get json from stdin.
        try:
            from_stdin_json = json.load(sys.stdin)
        except:
            sys.stderr.write("Unable to load JSON from stdin.")
            sys.exit(1)
        response_dict = {}
        for k in from_stdin_json:
            if action == 'read':
                response_dict.update(store.read(k))
        sys.stdout.write(json.dumps(response_dict))
        sys.exit()

    try:
        key = args.key[0]
    except IndexError:
        sys.stderr.write("Please specify a key.")
        sys.exit(1)

    if action == 'read':
        sys.stdout.write(store.read(key)[key])
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
