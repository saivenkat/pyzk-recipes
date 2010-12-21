import zookeeper, threading, sys, time
ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class ZooKeeperQueue(object):
  def __init__(self,queuename):
    self.connected = False
    self.queuename = "/" + queuename
    self.cv = threading.Condition()
    zookeeper.set_log_stream(open("/dev/null"))
    def watcher(handle,type,state,path):
      print "Connected"
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
      zookeeper.create(self.handle,self.queuename,"queue top level", [ZOO_OPEN_ACL_UNSAFE],0)
    except zookeeper.NodeExistsException:
      print "Queue already exists"

  def enqueue(self,val):
    """
    Adds a new znode whose contents are val to the queue
    """
    zookeeper.create(self.handle, self.queuename+"/item", val, [ZOO_OPEN_ACL_UNSAFE],zookeeper.SEQUENCE)

  def dequeue(self):
    """
    Removes an item from the queue. Returns None is the queue is empty
    when it is read.
    """
    while True:
      children = sorted(zookeeper.get_children(self.handle, self.queuename,None))
      if len(children) == 0:
        return None
      for child in children:
        data = self.get_and_delete(self.queuename + "/" + children[0])
        if data:
          return data

  def get_and_delete(self,node):
    """
    Atomic get-and-delete operation. Returns None on failure.
    """
    try:
      (data,stat) = zookeeper.get(self.handle, node, None)
      zookeeper.delete(self.handle, node, stat["version"])
      return data
    except zookeeper.NoNodeException:
      # Someone deleted the node in between our get and delete
      return None
    except zookeeper.BadVersionException, e:
      # Someone is modifying the queue in place. You can reasonably
      # either retry to re-read the item, or abort.
      print "Queue item %d modified in place, aborting..." % node
      raise e

  def block_dequeue(self):
    """
    Similar to dequeue, but if the queue is empty, block until an item
    is added and successfully removed.
    """
    def queue_watcher(handle,event,state,path):
      self.cv.acquire()
      self.cv.notify()
      self.cv.release()
    while True:
      self.cv.acquire()
      children = sorted(zookeeper.get_children(self.handle, self.queuename, queue_watcher))
      for child in children:
        data = self.get_and_delete(self.queuename+"/"+children[0])
        if data != None:
          self.cv.release()
          return data
        self.cv.wait()
        self.cv.release()

if __name__ == '__main__':
  zk = ZooKeeperQueue("myfirstqueue")
  print "Enqueuing 100 items"
  from threading import Thread
  for i in xrange(100):
    zk.enqueue("queue item %d" % i)
  print "Done"

  class consumer(Thread):
    def __init__(self, n):
      self.num = n
      Thread.__init__(self)

    def run(self):
      v = zk.dequeue()
      while v != None:
        print "Thread %d: %s" % (self.num, v)
        v = zk.dequeue()
        time.sleep(0.1)
  
  print "Consuming all items in queue with 5 threads"
  threads = [ consumer(x) for x in xrange(5) ]
  for t in threads:
    t.start()
  for t in threads:
    t.join()
  print "Done"
