ps -ef | grep zmqnet.py | awk '{ print $2 }' | xargs -I {} kill -9 {}
ps -ef | grep HAController.py | awk '{ print $2 }' | xargs -I {} kill -9 {}
ps -ef | grep runner.py | awk '{ print $2 }' | xargs -I {} kill -9 {}
ps -ef | grep networkTest.py | awk '{ print $2 }' | xargs -I {} kill -9 {}
