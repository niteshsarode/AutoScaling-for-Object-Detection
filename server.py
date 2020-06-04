import boto3
import time

ACCESS_KEY = '###'
SECRET_KEY = '###'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/285227251910/input_queue.fifo'

def start_instances(instances):
	ec2 = boto3.client('ec2',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
	ec2.start_instances(InstanceIds=instances)

def get_instances():
	ec2 = boto3.resource('ec2',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
	stopped_instances = []
	for instance in ec2.instances.all():
		dns_name = instance.public_dns_name
		if instance.state['Name'] == 'stopped':
			stopped_instances.append(instance.id)
	print(stopped_instances)
	return stopped_instances

def check_sqs_queue():
	client = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY, region_name='us-east-1')
	response_receive = client.receive_message(
		QueueUrl = SQS_QUEUE_URL,
		MaxNumberOfMessages = 10,
		VisibilityTimeout = 5,
	)
	if 'Messages' in response_receive:
		return len(response_receive['Messages'])
	else:
		return 0

while True:
	n = check_sqs_queue()
	available_instances = get_instances()
	n = min(n,len(available_instances))
	if n != 0:
		time.sleep(10)
		start_instances(available_instances[:n])
	time.sleep(3)