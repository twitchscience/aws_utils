#!/usr/env/python

import boto.ec2
import os
import json

with open(os.path.expanduser("~/.aws/packer.json")) as f:
  credentials = json.loads(f.read())

conn = boto.ec2.connect_to_region("us-west-2",
  aws_access_key_id=credentials["aws_access_key"],
  aws_secret_access_key=credentials["aws_secret_key"])

for image in conn.get_all_images():
  print image.name
