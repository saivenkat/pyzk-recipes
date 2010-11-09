import zookeeper, sys, time, threading
ZOO_OPEN_ACL_UNSAFE = {"perms" : 0x1f, "scheme" : "world", "id" : "anyone"};

class ZkBarrier(object):
    def __init__(self, barriername):
	self.cv = threading.Condition()
	self.connected = False
	self.barrier = "/" + barriername
	zookeeper.set_log_stream(open('/dev/null'))
	def watcher(handle, type, state, path):
	    print 'Connected'
	    self.cv.acquire()
	    self.connected = True
	    self.cv.notify()
	    self.cv.release()

	self.cv.acquire()
	self.handle = zookeeper.init("localhost:2181", watcher, 10000)
	self.cv.wait(10.0)
	if not self.connected:
            print "Connection to ZooKeeper cluster timed out - is a server running on localhost:2181?"
            sys.exit()
	self.cv.release()

    def create(self):
	try:
	    zookeeper.create(self.handle, self.barrier, "barrier", [ZOO_OPEN_ACL_UNSAFE], 0)
	except zookeeper.NodeExistsException:
	    print "Barrier exists - %s" %(self.barrier)
	except Exception, ex:
	    print ex
	    raise ex

    def remove(self):
        try:
	    print "Now removing"
	    (data,stat) = zookeeper.get(self.handle, self.barrier, None)
            zookeeper.delete(self.handle, self.barrier, stat["version"])
	    print "Removed barrier %s" %(self.barrier)
	except zookeeper.NoNodeException:
	    print "No barrier %s" %(barrier)
	    return None
	except Exception, e:
	    print e
	    raise e
    
    def wait_on_exist(self, times, callback):
        self.callback_flag = False
	self.rc = 0
        def notify(handle, rc, stat):
	    self.cv.acquire()
	    self.callback_flag = True
	    self.cv.notify()
	    self.cv.release()
	    self.rc = rc

        self.cv.acquire()
        ret = zookeeper.aexists(self.handle, self.barrier, None, notify )
        assert(ret == zookeeper.OK)
        while (self.rc != -101) and (times >= 0):
	    print "Blocked as barrier is there"
            self.cv.wait(15.0)
	    times = times - 1
        self.cv.release()
	if self.callback_flag:
	    callback(True)
	else:
	    callback(False)

if __name__ == '__main__':
    from threading import Thread
    class Master(Thread):
	def __init__(self):
            Thread.__init__(self)
	def run(self):
	    barrier = ZkBarrier("indexing")
	    barrier.create()
	    print "Created a barrier. Now doing some work..."
	    set(range(10000000)).difference(set())
	    barrier.remove()
	    print "Removing barrier. All workers proceed..."
    class Worker(Thread):
        def __init__(self, n):
	    self.n = n
	    Thread.__init__(self)
        def run(self):
            barrier = ZkBarrier("indexing")
	    print "Worker %s blocked" %(self.n)
            barrier.wait_on_exist(10, self.callback)
        def callback(self, result):
	    if result:
                print "Worker %s starting now to be useful" %(self.n)
	    else:
	        print "Retrying"
		barrier.wait_on_exist(10, self.callback)

    print "Starting master"
    m = Master()
    m.start()
    print "Starting all workers"
    workers = [Worker(x) for x in xrange(5)]
    for w in workers:
        w.start()
    m.join()
    for w in workers:
        w.join()
    print 'Done'
        
    
     
