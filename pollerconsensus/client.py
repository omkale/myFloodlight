import zmq
import threading
import time

no_clients  = 10000
no_requests = 1000

times = list()

def client():
    client_sock = zmq.Context().socket(zmq.REQ)
    client_sock.connect("tcp://127.0.0.1:5252")
    # Setting send and receive timeouts
    client_sock.RCVTIMEO = 3000
    client_sock.SNDTIMEO = 3000

    for i in range(no_requests):
        try:
            client_sock.send("PULSE")
            reply = client_sock.recv()
        except KeyboardInterrupt :
            exit(0)
        except Exception,e:
            print e
    return

def run(i):
    start = time.time()
    client()
    end = time.time()
    elapsed = end - start
    print "Client %s"%str(i+1)
    print "Elapsed (s): %s"%str(elapsed)
    print "Throughput = %s/elapsed : %.2f"%(str(no_requests),no_requests/elapsed)
    times.append((no_requests/elapsed))
    return

threads = list()
for i in range(no_clients):
    t1 = threading.Thread(target=run(i))
    t1.start()
    threads.append(t1)

for i in threads:
    i.join()

sum1 =0
for x in times:
    sum1 = sum1 + x

print "Average throughput: ",sum1/no_clients

