package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author : Zhaoheng Shi
 * @date : 2021/3/12 17:22
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接集群的地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        //用户
        String user = "hadoop";
        //1.获取客户端对象
         fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //3.关闭资源
        fs.close();
    }

    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        //2.创建文件夹
        fs.mkdirs(new Path("/test"));

    }

    // 上传
    /**
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml=> 在项目资源目录下的配置文件 =》代码里面的配置
     *
     */
    @Test
    public void testPut() throws IOException {
        // 参数解读：参数一：表示删除原数据； 参数二：是否允许覆盖；参数三：原数据路径； 参数四：目的地路径
        fs.copyFromLocalFile(false,true,new Path("E:\\tmp\\a.txt"),new Path("/test"));
    }

    // 文件下载
    @Test
    public void testGet() throws IOException {
        // 参数的解读：参数一：原文件是否删除；参数二：原文件路径HDFS； 参数三：目标地址路径Win ; 参数四：
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/test/"), new Path("C:\\"), false);
    }


}
