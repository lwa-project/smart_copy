#!/usr/bin/env python3

import os
import sys
import json
import pytz
import time
import subprocess
from socket import gethostname
from datetime import datetime

from lwa_auth import KEYS as LWA_AUTH_KEYS
from lwa_auth.signed_requests import post as signed_post

URL = "https://lwalab.phys.unm.edu/OpScreen/update"
SITE = gethostname().split('-', 1)[0]
PATH = os.path.dirname(os.path.abspath(__file__))

UTC = pytz.UTC

def _serialize_datetime(value):
    try:
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.isoformat() + 'Z'
    except AttributeError:
        return value

summary = 'NORMAL'
validDRs = (1, 2, 3, 4, 5)
if SITE != 'lwa1':
    validDRs = (1, 2, 3, 4)
    
total_count = 0
active_count = 0
active_progress = []
active_remaining = []
active_speed = []
for dr in validDRs:
    try:
        output = subprocess.check_output([os.path.join(PATH, 'smartQuery.py'), f"QUEUE_SIZE_DR{dr}"])
        output = output.decode()
        output = output.split('\n')[-2]
        status, _, count = output.split(None, 2)
        if status == 'A':
            count = int(count)
            total_count += count
            
        output = subprocess.check_output([os.path.join(PATH, 'smartQuery.py'), f"ACTIVE_ID_DR{dr}"])
        output = output.decode()
        output = output.split('\n')[-2]
        status, _, count = output.split(None, 2)
        if status == 'A':
            try:
                count = int(count)
                count = 1
                
                output = subprocess.check_output([os.path.join(PATH, 'smartQuery.py'), f"ACTIVE_PROGRESS_DR{dr}"])
                output = output.decode()
                output = output.split('\n')[-2]
                status, _, prog = output.split(None, 2)
                if status == 'A':
                    active_progress.append(f"DR{dr} @ {prog}")
                else:
                    active_progress.append(f"DR{dr} @ unknown")
                    
                output = subprocess.check_output([os.path.join(PATH, 'smartQuery.py'), f"ACTIVE_REMAINING_DR{dr}"])
                output = output.decode()
                output = output.split('\n')[-2]
                status, _, remain = output.split(None, 2)
                if status == 'A':
                    active_remaining.append(f"DR{dr} @ {remain}")
                else:
                    active_remaining.append(f"DR{dr} @ unknown")
                    
                output = subprocess.check_output([os.path.join(PATH, 'smartQuery.py'), f"ACTIVE_SPEED_DR{dr}"])
                output = output.decode()
                output = output.split('\n')[-2]
                status, _, speed = output.split(None, 2)
                if status == 'A':
                    active_speed.append(f"DR{dr} @ {speed}")
                else:
                    active_speed.append(f"DR{dr} @ unknown")
                    
            except ValueError:
                count = 0
        active_count += count
        
    except subprocess.CalledProcessError:
        summary = 'ERROR'
        continue
        
data = [{'site': SITE,
         'summary': summary, 
         'total': total_count,
         'active': active_count,
         'progress': active_progress,
         'remaining': active_remaining,
         'speed': active_speed,
         'update': datetime.utcnow()},]
data = json.dumps(data, default=_serialize_datetime)
f = signed_post(LWA_AUTH_KEYS.get(SITE+'-log', kind='private'), URL,
                data={'site': 'elwa', 'subsystem': 'ASP', 'data': data})
f.close()
