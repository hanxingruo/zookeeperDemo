package com.myspace.curator;


import java.util.Collection;

import com.myspace.util.Comm;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

public class CuratorDemo {

    // 此demo使用的集群，所以有多个ip和端口
    private static String CONNECT_SERVER = Comm.ConnectStrAll;
    private static int SESSION_TIMEOUT = 3000;
    private static int CONNECTION_TIMEOUT = 3000;

    public static void main(String[] args) {
        // 连接 ZooKeeper
        CuratorFramework framework = CuratorFrameworkFactory.
                newClient(CONNECT_SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, new ExponentialBackoffRetry(1000,10));
        // 启动
        framework.start();

        Stat stat = ifExists(framework);

        if(stat != null){
//            update(framework);
//            delete(framework);
//            query(framework);

//            监听事件，只监听一次，不推荐
//            listener1(framework);
        }else {
            add(framework);
        }

//        事务
//        transaction(framework);

//        持久监听，推荐使用
        listener2(framework);
    }

    /**
     * 判断节点是否存在
     * @param cf
     * @return
     */
    public static Stat ifExists(CuratorFramework cf){
        Stat stat = null;
        try {
            stat = cf.checkExists().forPath("/node_curator/test");;
            System.out.println(stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stat;
    }

    /**
     * @Title: add
     * @Description: TODO(增加节点 ， 可以增加 多级节点)
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void add(CuratorFramework cf){
        try {
            String rs = cf.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT).forPath("/node_curator/test","xbq".getBytes());
            System.out.println(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }

    /**
     * @Title: update
     * @Description: TODO(修改指定节点的值)
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void update(CuratorFramework cf){
        try {
            Stat stat = cf.setData().forPath("/node_curator/test", "javaCoder".getBytes());
            System.out.println(stat);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }

    /**
     * @Title: delete
     * @Description: TODO(删除节点或者删除包括子节点在内的父节点)
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void delete(CuratorFramework cf){
        try {
            // 递归删除的话，则输入父节点
            cf.delete().deletingChildrenIfNeeded().forPath("/node_curator");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }

    /**
     * @Title: query
     * @Description: TODO(查询节点的值)
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void query(CuratorFramework cf){
        try {
            byte[] value = cf.getData().forPath("/node_curator/test");
            System.out.println(new String(value));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }

    /**
     * @Title: transaction
     * @Description: TODO(一组crud操作同生同灭)
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void transaction(CuratorFramework cf){
        try {
            // 事务处理， 事务会自动回滚
            Collection<CuratorTransactionResult> results = cf.inTransaction()
                    .create().withMode(CreateMode.PERSISTENT).forPath("/node_xbq1").and()
                    .create().withMode(CreateMode.PERSISTENT).forPath("/node_xbq2").and().commit();
            // 遍历
            for(CuratorTransactionResult result:results){
                System.out.println(result.getResultStat() + "->" + result.getForPath() + "->" + result.getType());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }

    /**
     * @Title: listener1
     * @Description: TODO(监听 事件    --  通过 usingWatcher 方法)
     *  注意：通过CuratorWatcher 去监听指定节点的事件， 只监听一次
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void listener1(CuratorFramework cf){
        try {
            cf.getData().usingWatcher(new CuratorWatcher() {
//                @Override
                public void process(WatchedEvent event) throws Exception {
                    System.out.println("触发事件：" + event.getType());
                }
            }).forPath("/javaCoder");

            System.in.read(); // 挂起，在控制台上输入 才停止
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }

    /**
     * @Title: listener2
     * @Description: TODO(监听 子节点的事件，不监听 自己    --  通过 PathChildrenCacheListener 方法，推荐使用)
     * @param @param cf    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void listener2(CuratorFramework cf) {
        // 节点node_xbq不存在 会新增
        PathChildrenCache cache = new PathChildrenCache(cf, "/node_curator/test", true);
        try {
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            cache.getListenable().addListener(new PathChildrenCacheListener() {
//                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    System.out.println("触发事件：" + event.getType());
                }
            });
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cf.close();
        }
    }
}
