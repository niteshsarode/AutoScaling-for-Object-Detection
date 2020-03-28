from botocore.exceptions import NoCredentialsError, BotoCoreError, ClientError
from ec2_metadata import ec2_metadata
import boto3
import time
import subprocess
import json
import time

ACCESS_KEY = 'AKIAJGHQA4TO7CJIONPQ'
SECRET_KEY = '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'
INPUT_BUCKET = 'videos-bucket-input'
OUTPUT_BUCKET = 'videos-bucket-output'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/285227251910/input_queue.fifo'
PATH = '/home/ubuntu/'

def stop_self_instance():
    ec2 = boto3.client('ec2',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
    instance_id = ec2_metadata.instance_id
    ec2.stop_instances(InstanceIds = [instance_id])

def check_queue():
    sqs = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
    response_receive = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages = 1,
        VisibilityTimeout = 15,
    )
    if 'Messages' in response_receive:
        message = response_receive['Messages']
        return message[0]['Body'], message[0]['ReceiptHandle']
    else:
        return None, None

def delete_from_queue(handle):
    sqs = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
    sqs.delete_message(
        QueueUrl=SQS_QUEUE_URL,
        ReceiptHandle=handle
    )
    print("Key deleted from queue!")

def upload_on_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def upload_on_sqs(key_name):
    client = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
    response = client.send_message(
        QueueUrl = SQS_QUEUE_URL,
        MessageBody = key_name,
        MessageGroupId = 'id1'
    )
    print("Key queued on sqs!")

def get_video_from_s3(key):
    s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
    s3.download_file(INPUT_BUCKET, key, PATH+"darknet/"+key)

def upload_ouput_on_S3(res_file):
    try:
        s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
        with open(PATH+res_file+'.txt', 'rb') as data:
            s3.upload_fileobj(data, OUTPUT_BUCKET, res_file)
        print("File Uploaded!")
    except Exception as e:
        with open('logs.txt','w') as log_file:
            log_file.write(str(e))

def run_object_detection(video_name):
    res = []
    objects = []
    preds = ""
    p = subprocess.Popen(["./darknet","detector","demo",PATH+"darknet/cfg/coco.data",PATH+"darknet/cfg/yolov3-tiny.cfg",PATH+"darknet/yolov3-tiny.weights","video.h264"],cwd=PATH+'darknet',stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(p.stdout.readline, ""):
        res.append(stdout_line)
    p.stdout.close()
    return_code = p.wait()
    if return_code:
        print("Finished!")
    for r in res:
        if '%' in r:
            objects.append(r.split(":")[0])
    objects = list(set(objects))
    with open(PATH+video_name+'.txt','w') as o_file:
        if not objects:
            o_file.write("no object detected")
        else:
            o_file.write(objects[0])
            for o in objects[1:]:
                o_file.write(","+o)    
    upload_ouput_on_S3(video_name)
    
# upload_on_s3('darknet/video.h264',INPUT_BUCKET,'video5.h264')
# upload_on_sqs('video5.h264')
# while True:
key, handle = check_queue()
delete_from_queue(handle)
if key != None:
    print(key)
    get_video_from_s3(key)
    run_object_detection(key)
    print("Processed!")
# stop_self_instance()

# run_object_detection("asdf")