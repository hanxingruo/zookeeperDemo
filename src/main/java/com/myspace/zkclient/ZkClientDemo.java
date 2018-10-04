package com.myspace.zkclient;


import com.myspace.util.Comm;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;

/**
 * @ClassName: ZkClientDemo
 * @Description: 使用 zkClient 访问zookeeper
 * 　zkClient主要做了两件事情:
 *
 * 　　　　一件是在session loss和session expire时自动创建新的ZooKeeper实例进行重连。
 *
 * 　　　　另一件是将一次性watcher包装为持久watcher。后者的具体做法是简单的在watcher回调中，重新读取数据的同时再注册相同的watcher实例。
 *
 *       zkClient目前已经运用到了很多项目中，知名的有Dubbo、Kafka、Helix。
 *
 * 　　zkClient jar包下载，或者直接添加maven依赖： http://mvnrepository.com/artifact/com.101tec/zkclient
 *
 * @author xbq
 * @date 2017-3-26 上午11:49:39
 */
public class ZkClientDemo {

    // 此demo使用的集群，所以有多个ip和端口
    private static String CONNECT_SERVER = Comm.ConnectStrAll;
    private static int SESSION_TIMEOUT = 3000;
    private static int CONNECTION_TIMEOUT = 3000;
    private static ZkClient zkClient ;

    static {
        zkClient = new ZkClient(CONNECT_SERVER, SESSION_TIMEOUT,CONNECTION_TIMEOUT,new MyZkSerializer());
    }

    public static void main(String[] args) {

//        add(zkClient);
//        update(zkClient);
//        delete(zkClient);

//        addDiGui(zkClient);
//        deleteDiGui(zkClient);

        subscribe(zkClient);
    }

    /**
     * @Title: add
     * @Description: TODO(增加一个指定节点)
     * @param @param zkClient    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void add(ZkClient zkClient){
        // 如果不存在节点，就新建一个节点
        if(!zkClient.exists("/config")){
            zkClient.createPersistent("/config","javaCoder");
        }
        // 查询一下，看是否增加成功
        String value = zkClient.readData("/config");
        System.out.println("value===" + value);
    }

    /**
     * @Title: addSequential
     * @Description: TODO(递归创建节点)
     * @param @param zkClient    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void addDiGui(ZkClient zkClient){
        // 递归创建节点
        zkClient.createPersistent("/config/userName", true);
        if(zkClient.exists("/config/userName")){
            System.out.println("增加成功！");
        }else {
            System.out.println("增加失败！");
        }
    }

    /**
     * @Title: delete
     * @Description: TODO(删除指定节点)
     * @param @param zkClient    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void delete(ZkClient zkClient){
        // 存在节点才进行删除
        if(zkClient.exists("/config")){
            boolean flag = zkClient.delete("/config");
            System.out.println("删除" + (flag == true ? "成功！" : "失败！"));
        }
    }

    /**
     * @Title: deleteDiGui
     * @Description: TODO(递归删除)
     * @param @param zkClient    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void deleteDiGui(ZkClient zkClient){
        // 存在节点才进行删除
        if(zkClient.exists("/xbq")){
            // 递归删除的时候 只传入 父节点就可以，如果传入 全部的节点，虽然返回的是true，但是依然是没有删除的，
            // 因为zkClient将异常封装好了，进入catch的时候，会返回true，这是一个坑
            boolean flag = zkClient.deleteRecursive("/xbq");
            System.out.println("删除" + (flag == true ? "成功！" : "失败！"));
        }
    }

    /**
     * @Title: update
     * @Description: TODO(修改节点的值)
     * @param @param zkClient    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void update(ZkClient zkClient){
        if(zkClient.exists("/config")){
            zkClient.writeData("/config", "testUpdate");
            // 查询一下，看是否修改成功
            String value = zkClient.readData("/config");
            System.out.println("value===" + value);
        }
    }

    /**
     * @Title: subscribe
     * @Description: TODO(事件订阅, 可用于配置管理)
     * 先订阅，再 操作增删改。（可多个 客户端订阅）
     * @param @param zkClient    设定文件
     * @return void    返回类型
     * @throws
     */
    public static void subscribe(ZkClient zkClient){
        zkClient.subscribeDataChanges("/config/userName", new IZkDataListener() {
//            @Override
            public void handleDataDeleted(String arg0) throws Exception {
                System.out.println("触发了删除事件：" + arg0);
            }

//            @Override
            public void handleDataChange(String arg0, Object arg1) throws Exception {
                System.out.println("触发了改变事件：" + arg0 + "-->" + arg1);
            }
        });

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
