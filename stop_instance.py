import os
import boto3
 
dir_path = os.path.dirname(os.path.realpath(__file__))
foldername = os.path.basename(dir_path)
f = open(os.path.join(dir_path, "aws_keys"), "r")
keys = f.read().split("\n")

ACCESS_KEY = keys[0]
SECRET_KEY = keys[1]
ec2 = boto3.resource('ec2', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name="eu-west-1")

filters = [
    {
        'Name': 'instance-state-name',
        'Values': ['running']
    }
]

instances = ec2.instances.filter(Filters=filters)

RunningInstances = [instance for instance in instances]

for instance in RunningInstances:
    if instance.tags[0]['Value'] == foldername:
        print(instance.id)
        instance.stop()
