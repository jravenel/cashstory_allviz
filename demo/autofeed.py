# -*- coding: utf8 -*-
# !/usr/bin/python
import requests
from requests.auth import HTTPBasicAuth
import json
import glob
import os
from slacker import Slacker
import time
import urllib3

urllib3.disable_warnings()

requests.packages.urllib3.disable_warnings()

INSTANCE_NAME = "Demo staging"
API_BASEROUTE = 'https://api-demo-staging.toucantoco.com/'
SMALL_APP_ID = 'demo'
USERNAME = '***'
PASSWORD = '***'
DATA_SOURCES_DIRECTORY = u'/home/vftp/demo/'

RECIPIENTS = ['dev+autofeed@toucantoco.cm']
VOYAGER_URL = 'http://api-voyager.toucantoco.com/notify'

MAX_WAIT = 60

SLACK_TOKEN = 'xoxp-2937008483-2939222567-26846960770-1e0aa96a80'
SLACK_CHANNEL = '#devhooks'
SLACK_PREFIX = "[{}][{}]".format(INSTANCE_NAME, SMALL_APP_ID)

slack = Slacker(SLACK_TOKEN)

AUTORELEASE = True


def release():
    """
    Release the small app: post to /release, and send a message to slack.
    If something goes wrong try again. No more than <release_attempts> attempts.
    Wait 120s between two attempts.

    """
    release_attempts = 60
    if AUTORELEASE:
        while release_attempts > 0:
            res = try_request(API_BASEROUTE + SMALL_APP_ID + '/release', auth=HTTPBasicAuth(USERNAME, PASSWORD))
            if res.status_code == 200:
                msg = 'New release done'
                print msg
                slack_post_message(SLACK_CHANNEL, msg)
                return
            else:
                release_attempts -= 1
                msg = '[{}] New release failed for now. Code: {}, reason: {}. Retrying in 2 minutes for {} times'.format(
                    SMALL_APP_ID,
                    res.status_code,
                    res.text,
                    release_attempts,
                )
                print msg
                if release_attempts == 0:
                    msg = 'New release failed after 5 attempts'
                    slack_post_message(SLACK_CHANNEL, msg)
                    print msg
                    return
                time.sleep(100)
                print 'retrigger release'


def upload_files():
    """
    Upload files from the data sources directory (DATA_DATA_SOURCES_DIRECTORY)
    to the server. If everything went well, start a synchronous population. If
    the population went well or if timeout, release the small app (call to the
    release() method).

    Note:
        As for now, the release will only start if the population is finished
        before 12 minutes.

    """
    success = True
    data_sources_dir_clean = DATA_SOURCES_DIRECTORY \
        if DATA_SOURCES_DIRECTORY.endswith('/') else DATA_SOURCES_DIRECTORY + '/'
    print 'data sources dir: {}'.format(data_sources_dir_clean)
    for filename in glob.glob(data_sources_dir_clean + '*'):
        print 'file to upload: {}'.format(filename)

        params = {
            'data': '{{"filename": "{}" }}'.format(os.path.basename(filename)),
            'async': True
        }
        files = {'file': open(filename, 'rb')}
        res = try_request(API_BASEROUTE + SMALL_APP_ID + '/data/sources',
                          data=params,
                          files=files,
                          auth=HTTPBasicAuth(USERNAME, PASSWORD))
        if res.status_code == 200:
            msg = '"{}" uploaded successfully '.format(
                os.path.basename(filename)
            )
            print msg
            slack_post_message(SLACK_CHANNEL, msg)
        else:
            success = False
            msg = '"{}" upload FAILED MISERABLY, code: {} '.format(
                os.path.basename(filename),
                res.status_code
            )
            print msg
            slack_post_message(SLACK_CHANNEL, msg)

    if success:
        print 'triggering population'
        res = try_request(API_BASEROUTE + SMALL_APP_ID + '/populate?stage=staging',
                          auth=HTTPBasicAuth(USERNAME, PASSWORD))
        if res.status_code == 200 or res.status_code == 504:
            watch_populate_state()
            print 'trigger release'
            release()
        else:
            print 'population failed <> code: {}, reason: {}'.format(
                res.status_code, res.text
            )
            send_error_by_email()
            slack_post_message(SLACK_CHANNEL, 'Global Population FAILED MISERABLY ')


def watch_populate_state():
    """
    Loop until the populate has ended or the time spent has lasted more than
    MAX_WAIT minutes. Wait two minutes between two http requests to /
    populate/state.

    """
    is_running = True
    end_time = time.time() + (MAX_WAIT * 60)
    while is_running:
        res = try_request(API_BASEROUTE + SMALL_APP_ID + '/populate/state?stage=staging',
                          auth=HTTPBasicAuth(USERNAME, PASSWORD),
                          method=requests.get)
        is_running = json.loads(res.content)['running']
        if time.time() > end_time:
            print 'population has not ended, timeout did.'
            return
        elif not is_running:
            print 'population has ended'
            return

        slack_post_message(SLACK_CHANNEL, 'Population running. {} minutes before time out'.format(
            int((end_time - time.time()) / 60)
        ))
        time.sleep(120)

    slack_post_message(SLACK_CHANNEL, 'Global Population was successful')


def try_request(request, auth, data=None, files=None, method=requests.post):
    """
    Execute a http post request. Catch and log any exceptions.
    Args:
        request: request to send
        auth: http authentication
        data: data for http post request (information)
        files: data for http post request (file in itself)
        method: request method corresponding to the needed http request (get
        or post)

    Returns: status code or -1 if an exception has been raised

    """
    try:
        if data is None and files is None:
            return method(request, auth=auth)
        else:
            return method(request, auth=auth, files=files, data=data)
    except Exception:
        send_error_by_email()
        print 'post request failed'
        return -1


def slack_post_message(channel, msg):
    """
    Post a message to a slack channel using the global slack chat variable.
    The goal of this method is to catch a potential exception and log it if
    any. For now, it catches Exception.

    Args:
        channel: slack channel to post a message on
        msg: message to send

    """
    try:
        slack.chat.post_message(channel, "{} {}".format(SLACK_PREFIX, msg))
    except Exception:
        print 'slack error failed to post message'


def send_error_by_email():
    """
    Send an email to notify for errors.

    """
    try:
        params = {
            'recipients': RECIPIENTS,
            'small_app': SMALL_APP_ID
        }
        requests.post(VOYAGER_URL, data=params)
    except Exception:
        print 'failed to notify error by mail'

if __name__ == '__main__':
    upload_files()
