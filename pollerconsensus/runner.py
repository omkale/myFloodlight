import os
import signal
import subprocess
import time
import random

no_controllers = 4
x =[i for i in range(0,no_controllers)]
random.shuffle(x)
random.shuffle(x)

try:
    cmd = "python generateServerConfig.py "+"5252 "+str(5252+no_controllers-1)
    p = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
except Exception, e:
    print e

try:
    for i in x:
        serv = 3232+i
        client = 5252+i
        cmd = "python HAController.py "+str(serv)+" "+str(client)+" "+str(i+1)+" "
        print "[RUNNER] ",cmd
        p = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

except (KeyboardInterrupt, SystemExit):
    print "PARENT KILLS"
    os._exit(0)
