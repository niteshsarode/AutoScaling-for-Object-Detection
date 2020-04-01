import boto3
from picamera import PiCamera
import sys
from time import sleep
import subprocess
import threading
import multiprocessing
import RPi.GPIO as GPIO

sensor = 12
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BOARD)
GPIO.setup(sensor, GPIO.IN)

INPUT_BUCKET = 'videos-bucket-input'
OUTPUT_BUCKET = 'videos-bucket-output'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/285227251910/input_queue.fifo'
filepath = '/home/pi/cloud_project/videos/'
PATH = '/home/pi/'

global p
p = multiprocessing.Process()

def upload_on_s3(local_file, bucket, s3_file):
	s3 = boto3.client('s3')
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
        
def upload_output_on_s3(output):
	try:
		s3 = boto3.client('s3')
		with open(PATH+output+'.txt', 'rb') as data:
			s3.upload_fileobj(data, OUTPUT_BUCKET, output)
		print("File Uploaded!")
	except Exception as e:
		with open('logs.txt','w') as log_file:
			log_file.write(str(e))

#upload_on_s3('/home/pi/darknet/test_video.h264',INPUT_BUCKET, 'videofrompi.h264')
	
def upload_on_sqs(key_name):
	client = boto3.client('sqs')
	response = client.send_message(
		QueueUrl = SQS_QUEUE_URL,
		MessageBody = key_name,
		MessageGroupId = 'id1'
	)
	print("Key queued on sqs!")

def upload_on_aws(filename, videoname):
	upload_on_s3(filename, INPUT_BUCKET, videoname)
	upload_on_sqs(videoname)

def run_darknet_thread(file_name):
	FLAG = 1
	thread_darknet = threading.Thread(target = detect_object , args=(file_name,))
	thread_darknet.start()

def detect_object(video,videoname):
	res = []
	objects = []
	p = subprocess.Popen(["./darknet","detector","demo",PATH+"darknet/cfg/coco.data",PATH+"darknet/cfg/yolov3-tiny.cfg",PATH+"darknet/yolov3-tiny.weights",video],cwd=PATH+'darknet',stdout=subprocess.PIPE, universal_newlines=True)
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
	with open(PATH+videoname+'.txt','w') as o_file:
		if not objects:
			o_file.write("no object detected")
		else:
			o_file.write(objects[0])
			for o in objects[1:]:
				o_file.write(","+o)
	upload_output_on_s3(videoname)

def run_darknet_process(filename,videoname):
	global p
	p = multiprocessing.Process(target=detect_object, args=(filename,videoname,))
	p.start()
	
thread_darknet = threading.Thread()

cnt = 0
while True:
	i=GPIO.input(sensor)
	if i == 0:
		print("No Motion")
		sleep(.5)
	elif i == 1:
		videoname = 'video'+str(cnt)+'.h264'
		filename = filepath + videoname
		print("Motion detected")
		camera = PiCamera()
		camera.start_preview()
		camera.start_recording(filename)
		camera.wait_recording(5)
		camera.stop_recording()
		camera.stop_preview()
		camera.close()
		cnt+=1
		if p.is_alive():
			print("Thread running.. uploading on aws..!")
			upload_on_aws(filename,videoname)
		else:
			run_darknet_process(filename,videoname)
		sleep(1)
