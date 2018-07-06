"""
This module is the entrypoint for keeval. A simple key value read/write tool for s3.


"""
from .configstore import S3ConfigStore

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
