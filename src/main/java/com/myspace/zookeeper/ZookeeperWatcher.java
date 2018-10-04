package com.myspace.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ZookeeperWatcher {
    private static final int SESSION_TIMEOUT = 300000;
    private static class MyWatcher implements Watcher{
        public void process(WatchedEvent event) {
            System.out.println("接收到事件:"+event.getType());
//            event.
        }
    }
    public static void main(String[] args) {
        String path = "/yuzzNode";
        try {
            ZooKeeper zooKeeper = new ZooKeeper("172.22.42.213:2181", SESSION_TIMEOUT, null);
            byte[] data = zooKeeper.getData(path,new MyWatcher(),null);
            System.out.println("data:"+ new String(data));
            TimeUnit.SECONDS.sleep(100);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
