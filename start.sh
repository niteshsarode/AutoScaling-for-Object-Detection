#!/bin/sh
Xvfb :1 & export DISPLAY=:1
python3 /home/ubuntu/client.py