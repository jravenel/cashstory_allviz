# -*- coding: utf8 -*-
#!/usr/bin/python
import pandas as pd
import re
import pdb
import requests
from requests.auth import HTTPBasicAuth
import json
import glob
import os
from slacker import Slacker
import time

requests.packages.urllib3.disable_warnings()


API_BASEROUTE = '****YOUR API BASEROUTE HERE***'
SMALL_APP_ID = '****YOUR SMALL APP ID HERE ***'
USERNAME = '****'
PASSWORD = '****'
DATA_SOURCES_DIRECTORY = u'/home/vftp/XXXX'
SLACK_TOKEN = 'xoxp-2937008483-2939222567-26846960770-1e0aa96a80'
SLACK_CHANNEL = '#devhooks'
AUTORELEASE = True

slack = Slacker(SLACK_TOKEN)


def release():
  release_attempts = 5
  if AUTORELEASE:
    while release_attempts > 0:
      res = requests.post(API_BASEROUTE + SMALL_APP_ID + '/release', auth=HTTPBasicAuth(USERNAME, PASSWORD))
      if res.status_code == 200:
        msg = '[{0}] New release done '.format(SMALL_APP_ID)
        print msg
        slack.chat.post_message(SLACK_CHANNEL, msg )
        return
      else :
        release_attempts -= 1
        msg = '[{0}] New release failed for now. Retrying in 2 minutes for {1} times'.format( SMALL_APP_ID, release_attempts)
        print msg
        if release_attempts == 0:
          msg = '[{0}] New release failed after 5 attempts'.format(SMALL_APP_ID)
          slack.chat.post_message(SLACK_CHANNEL, msg)
        time.sleep(120)
        print 'retrigger release'


def upload_files():
  success = True
  for filename in glob.glob(DATA_SOURCES_DIRECTORY + '*'):
    params =  {'data':'{{"filename": "{0}" }}'.format(os.path.basename(filename))}
    files = {'file': open(filename, 'rb')}
    res = requests.post(API_BASEROUTE + SMALL_APP_ID + '/data/sources', data=params, files=files, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    if res.status_code == 200:
      msg ='[{1}] "{0}" uploaded successfully '.format(os.path.basename(filename), SMALL_APP_ID)
      print msg
      slack.chat.post_message(SLACK_CHANNEL, msg)
    else:
      success = False
      msg = '[{1}] "{0}" upload FAILED MISERABLY '.format(os.path.basename(filename), SMALL_APP_ID)
      print msg
      slack.chat.post_message(SLACK_CHANNEL, msg)

  if success == True:
    print 'triggering population'
    res = requests.post(API_BASEROUTE + SMALL_APP_ID + '/populate?stage=staging', auth=HTTPBasicAuth(USERNAME, PASSWORD))
    if res.status_code == 200:
      slack.chat.post_message(SLACK_CHANNEL, '[{0}] Global Population was successful '.format(SMALL_APP_ID))
      release()
    elif res.status_code == 504:
      slack.chat.post_message(SLACK_CHANNEL, '[{0}] Global Population timeout. Will try to release anyway'.format(SMALL_APP_ID))
      time.sleep(120)
      print 'trigger release'
      release()
    else:
      print 'failed'
      slack.chat.post_message(SLACK_CHANNEL, '[{0}] Global Population FAILED MISERABLY '.format(SMALL_APP_ID))


if __name__ == '__main__':
    upload_files()
