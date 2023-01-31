#!/usr/bin/env python3

import os
import sys
import time
import subprocess
from socket import gethostname

from lwa_auth import KEYS as LWA_AUTH_KEYS
from lwa_auth.signed_requests import post as signed_post

URL = "https://lwalab.phys.unm.edu/OpScreen/update"
SITE = gethostname().split('-', 1)[0]
PATH = os.path.dirname(os.path.abspath(__file__))

summary = 'NORMAL'
validDRs = (1, 2, 3, 4, 5)
if SITE != 'lwa1':
    validDRs = (1, 2, 3, 4)
    
total_count = 0
active_count = 0
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
            except ValueError:
                count = 0
        active_count += count
        
    except subprocess.CalledProcessError:
        summary = 'ERROR'
        continue
        
data = [time.time(), SITE, summary, total_count, active_count]
data = [str(v) for v in data]
data = ';;;'.join(data)
f = signed_post(LWA_AUTH_KEYS.get(SITE+'-log', kind='private'), URL,
                data={'site': 'elwa', 'subsystem': 'ASP', 'data': data})
f.close()