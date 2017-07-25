# Keeval

Keeval can read or write values to s3.

The purpose is to allow programatic access to a centrally available key value store we don't have to run: s3.

## Installation
pip install git+https://github.com/telmediq/keeval.git

## Setup
Set Environment AWS_PROFILE to a vaild profile.
Set Environment KEEVAL_BUCKET_NAME to an existing bucket.

## Reading a key.
keeval read foo.bar.ami_id
Returns 0

## Writing a key.
echo "blahblah" | keeval write foo.bar.ami_id
Returns 0




