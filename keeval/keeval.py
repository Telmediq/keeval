import boto3
import os
import sys
import argparse
import json
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
            data = obj.get()['Body'].read().strip()
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
    if 'AWS_PROFILE' not in os.environ:
        sys.stderr.write("Please set the environment variable AWS_PROFILE.\n")
        sys.exit(1)
    if args.bucket is not None:
        bucket_name = args.bucket[0]
    elif 'KEEVAL_BUCKET_NAME' in os.environ:
        bucket_name = os.environ['KEEVAL_BUCKET_NAME']
    else:
        sys.stderr.write("Please set the environment variable KEEVAL_BUCKET_NAME.\n")
        sys.exit(1)

    aws_profile = os.environ['AWS_PROFILE']

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
                key_name = k.split('.')[-1]
                response_dict.update({key_name: store.read(k)})
        sys.stdout.write(json.dumps(response_dict))
        sys.exit()

    try:
        key = args.key[0]
    except IndexError:
        sys.stderr.write("Please specify a key.")
        sys.exit(1)

    if action == 'read':
        sys.stdout.write(store.read(key))
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
