package com.myspace.zookeeper;


import com.myspace.util.Comm;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xbq
 * @version 1.0
 * @ClassName: ZookeeperDemo
 * @Description: 原生API访问zookeeper
 * @date 2017-3-10 下午5:05:16
 */
public class ZookeeperDemo {

    private static final int SESSION_TIMEOUT = 300000;


    public static void main(String[] args) {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper(Comm.ConnectStrSingle, SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println("触发事件：" + event.getType());
                }
            });
            String path = "/yuzzNode";
            String data = "TestxbqCoder";
            doWatch(zooKeeper, path, data);
//            create(zooKeeper, path, data);
            update(zooKeeper, path, data);
//            delete(zooKeeper, path);
//            aclOper(zooKeeper, path, data);
//            aclOper2(zooKeeper, path, data);
//

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     * @Title: create
     * @Description: TODO    增加操作
     * @return: void
     */
    public static void create(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {    // 如果不存在节点，就新建
            // 第三个参数是 权限，第四个参数 代表持久节点
            // 权限分类：OPEN_ACL_UNSAFE：对所有用户开放    READ_ACL_UNSAFE：只读   CREATOR_ALL_ACL： 创建者可以做任何操作
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println(new String(zooKeeper.getData(path, true, null)));
    }

    /**
     * @param zooKeeper
     * @throws InterruptedException
     * @throws KeeperException
     * @Title: update
     * @Description: TODO 修改操作
     * @return: void
     */
    public static void update(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);  // -1标识任何版本号 都可以
        System.out.println(new String(zooKeeper.getData(path, true, null)));
    }

    /**
     * @param zooKeeper
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     * @Title: delete
     * @Description: TODO 删除
     * @return: void
     */
    public static void delete(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) != null) {
            zooKeeper.delete(path, -1);
        }
        if (zooKeeper.exists(path, false) != null) {
            System.out.println(new String(zooKeeper.getData(path, true, null)));
        }
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     * @Title: aclOper
     * @Description: TODO 权限测试    先创建一个只读权限节点，然后更新该节点
     * @return: void
     */
    public static void aclOper(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException {
        // 首先创建一个只读的节点。第三个参数 代表只读权限
        zooKeeper.create(path, data.getBytes(), Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(new String(zooKeeper.getData(path, true, null)));
        // 测试更新节点，因为增加的是 只读的，所以 应该是不可以修改的。发现报错：org.apache.zookeeper.KeeperException$NoAuthException: KeeperErrorCode = NoAuth for /node_xbq
        zooKeeper.setData(path, "testRead_ACL".getBytes(), -1);
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     * @Title: aclOper2
     * @Description: TODO  自定义权限验证
     * 　　　* 自定义schema权限类型:digest,world,auth,ip，这里用 digest举例
     * @return: void
     */
    public static void aclOper2(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException, NoSuchAlgorithmException {
        // 第一个参数是 所有的权限，第二个参数是 通过 用户名和密码 验证
        ACL acl = new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest("root:root")));
        List<ACL> acls = new ArrayList<ACL>();
        acls.add(acl);

        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, data.getBytes(), acls, CreateMode.PERSISTENT);
        }

        zooKeeper.addAuthInfo("digest", "root:root".getBytes());

        // 通过下面的方式 是可以取到值的，因为 加了 用户名和 密码 验证 ，需要 加上  zooKeeper.addAuthInfo("digest", "root:root".getBytes());
        System.out.println(new String(zooKeeper.getData(path, true, null)));
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     * @Title: doWatch
     * @Description: TODO 监听
     * @return: void
     */
    public static void doWatch(ZooKeeper zooKeeper, String path, String data) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 监听 path节点
        zooKeeper.getData(path, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("触发了节点变更事件：" + event.getType());
            }
        }, null);
        // 用更新操作触发 监听事件
//        zooKeeper.setData(path, "updateTest".getBytes(), -1);
    }
}
