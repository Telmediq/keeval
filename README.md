# Keeval

Keeval can read or write values to s3.

The purpose is to allow programatic access to a centrally available key value store we don't have to run: s3.

## Building for distribution.
python setup.py sdist

## Installation
1. Run the following
```
pip install git+https://github.com/telmediq/keeval.git
```
2. Put the following in you ~/.bashrc (or convert for your terminal of choice)
```
export PATH=~/.local/bin:$PATH
```

## Setup
Set Environment AWS_PROFILE to a vaild profile.
Set Environment KEEVAL_BUCKET_NAME to an existing bucket.

## Reading a key.
keeval read foo.bar.ami_id
Returns 0

## Writing a key.
echo "blahblah" | keeval write foo.bar.ami_id
Returns 0

## Read bulk.
This was developed in order to add functionality to https://github.com/Telmediq/helm-build. It will read a bunch of keys into a dict for use in creating output from jinja templates.



