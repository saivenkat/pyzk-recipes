import zookeeper, sys, time, threading, uuid
ZOO_OPEN_ACL_UNSAFE = {"perms" : 0x1f, "scheme" : "world", "id" : "anyone"};

class ZkBarrier(object):
    def __init__(self, barriername, number_of_workers):
	self.cv = threading.Condition()
	self.connected = False
	self.barrier = "/" + barriername
	self.workers = number_of_workers
	zookeeper.set_log_stream(open('/dev/null'))
	def watcher(handle, type, state, path):
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
	try:
	    zookeeper.create(self.handle, self.barrier, '\x00', [ZOO_OPEN_ACL_UNSAFE], 0)
	except zookeeper.NodeExistsException:
	    pass
	except Exception, ex:
	    print ex
	    raise ex
	self.name = str(uuid.uuid1())


    def enter(self):
        self.name = zookeeper.create(self.handle, self.barrier + "/" + self.name, '\x00', [ZOO_OPEN_ACL_UNSAFE], 3) 
	self.ready = False
        def ready_watcher(handle, rc, stat):
            self.cv.acquire()
            self.cv.notify()
	    self.ready = True
            self.cv.release()
	zookeeper.aexists(self.handle, self.barrier + "/ready", None, ready_watcher)
	children = zookeeper.get_children(self.handle, self.barrier , None)
	while(len(children) < self.workers):
	    self.cv.acquire()
	    if self.ready:
	        break
	    print "Waiting for others. Number of children %s" %(len(children))
	    self.cv.wait(15.0)
	    self.cv.release()
        
	if not self.ready:
	    zookeeper.create(self.handle, self.barrier + "/ready", '\x00', [ZOO_OPEN_ACL_UNSAFE], 1)

        return True

	    
if __name__ == '__main__':
    from threading import Thread
    class Worker(Thread):
        def __init__(self, n):
	    self.n = n
	    Thread.__init__(self)
        def run(self):
            barrier = ZkBarrier("indexing", 5)
	    b = barrier.enter()
            if b:
	        print "Doing work"
	        set(range(10000000)).difference(set())

    print "Starting all workers"
    workers = [Worker(x) for x in xrange(5)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    print 'Done'
