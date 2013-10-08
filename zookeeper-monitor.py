from kazoo.client import KazooClient
from settings import MONITOR_PATH, MONITOR_ZK, UPDATE_INTERVAL, UPDATE_TIME
import time


class ZooKeeper_Monitor(object):

    def zoo_init(self, host, path):
        self.zk = KazooClient(hosts=host)
        self.zk.start()  # connect zk
        self.path = path  # monitor dir
        self.updateTimes = 0
        self.lastUpdatedData = ''
        self.host = host  # zk ip:port
        self.zk.ensure_path(self.path)  # ensure the exist of test dir
        self.zk.set(self.path, 'test')   # set the origin content

    def createNodeIfNotExist(self):
        if self.zk.exists(self.path):
            pass
        else:
            self.zk.ensure_path(self.path)

    def updateData(self, data):  # update the content
        self.zk.set(self.path, data)

    # ********************************************#
    # method: update the content 3 times
    #         then check(updateTimes==3)
    # ********************************************#
    def checkIfAlive(self):
        self.createNodeIfNotExist()
        for i in xrange(0, UPDATE_TIME):
            self.lastUpdatedData = '%s-%f' % (self.host, time.time())
            try:
                self.updateData(self.lastUpdatedData)
                time.sleep(UPDATE_INTERVAL)
            except Exception, e:
                return 0
        if self.updateTimes == 3:
            return 1          # zk is alive
        else:
            return 0          # zk is dead

    def close(self):
        self.zk.stop()


def zk_monitor(host, path):
    zktest = ZooKeeper_Monitor()
    zktest.zoo_init(host, path)

    @zktest.zk.DataWatch(zktest.path)
    def watch_node(data, stat):
        if data == zktest.lastUpdatedData:
            zktest.updateTimes += 1

    print 'host=%s alive=%d' % (host, zktest.checkIfAlive())


def run_monitor():
    for host in MONITOR_ZK:
        zk_monitor(host, MONITOR_PATH)


def main():
    run_monitor()

if __name__ == '__main__':
    main()
