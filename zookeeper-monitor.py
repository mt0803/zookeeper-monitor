from kazoo.client import KazooClient
import time


class ZooKeeper_Monitor(object):

    def zoo_init(self, host, path):
        self.zk = KazooClient(hosts=host)
        self.zk.start()
        self.path = path
        self.updateTimes = 0
        self.lastUpdatedData = ''
        self.host = host
        self.zk.ensure_path(self.path)

    def createNodeIfNotExist(self):
        if self.zk.exists(self.path):
            pass
        else:
            self.zk.ensure_path(self.path)

    def updateData(self, data):
        self.zk.set(self.path, data)

    def checkIfAlive(self):
        self.createNodeIfNotExist()
        for i in xrange(1, 4):
            self.lastUpdatedData = '%s-%f' % (self.host, time.time())
            try:
                self.updateData(self.lastUpdatedData)
                time.sleep(1)
            except Exception, e:
                return 0
        if self.updateTimes == 3:
            return 1
        else:
            return 0

    def close(self):
        self.zk.stop()


def main():
    zktest = ZooKeeper_Monitor()
    zktest.zoo_init('10.1.77.88:2181', '/hbase-test')

    @zktest.zk.DataWatch(zktest.path)
    def watch_node(data, stat):
        if data == zktest.lastUpdatedData:
            zktest.updateTimes += 1
            print 'changed %d time' % zktest.updateTimes

    print zktest.checkIfAlive()

if __name__ == '__main__':
    main()
