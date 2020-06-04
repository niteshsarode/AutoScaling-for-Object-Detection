import boto3
from botocore.exceptions import NoCredentialsError
import os
import subprocess
import time

file_name = 'test.mp4'

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        upload_key_to_sqs(file_name)
        print("Upload Successful!")
        return True
    except FileNotFoundError:
        print("The file was not found.")
        return False
    except NoCredentialsError:
        print("Credentials not available.")
        return False

def upload_key_to_sqs(key_name):
    client = boto3.client('sqs')
    response = client.send_message(
        QueueUrl = 'https://sqs.us-east-1.amazonaws.com/266126404547/input_queue.fifo',
        MessageBody = key_name,
        MessageGroupId = 'id1'
    )
    print("Key queued on sqs!")

def connect_ec2():
    ec2 = boto3.resource('ec2')
    ec2_client = boto3.client('ec2')

    response = ec2_client.describe_instances()
    # print(response)

    for instance in ec2.instances.all():
        dns_name = instance.public_dns_name
        process = subprocess.Popen(["sh", "run_instance.sh", dns_name]) # params = [dns_name_of_ec2_instance, key_of_video]
        time.sleep(2)
        # print(process.poll())
        while True:
            line = process.stdout.readline()
            if not line: break
        # network_interface = ec2.NetworkInterface(i.id)
        # print(network_interface.private_ip_address())
        # if i.state['Name'] == 'stopped':
        #     i.start()


# uploaded = upload_to_aws(file_name, 'inputvideosbucket', file_name)
connect_ec2()