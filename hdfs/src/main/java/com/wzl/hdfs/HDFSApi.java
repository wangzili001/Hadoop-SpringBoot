package com.wzl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSApi {
    private static Configuration conf;
    private static FileSystem fs;

    @Test
    @Before
    public void HDFSClientStart() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        fs= FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "wangzili");
    }
    @Test
    @After
    public void HDFSClientEnd() throws IOException {
        fs.close();
        System.out.println("操作完成");
    }
    //文件上传
    @Test
    public void CopyFromLocalFile() throws IOException {
        //设置副本系数
//        conf.set("dfs.replication","3");
        fs.copyFromLocalFile(new Path("F:\\mapreduce\\wordcount\\input\\a.txt"),new Path("/wordcount/input/a.txt"));
        System.out.println("上传完成");
    }
    //文件删除
    @Test
    public void deleteFile() throws IOException {
        fs.delete(new Path("/test/10000_access.log"),true);
        System.out.println("删除完成");
    }
    //文件下载
    @Test
    public void CopyToLocalFile() throws IOException{
        //delSrc是指是否将原文件删除
        //useRawLocalFileSystem 是否开启文件效验
        fs.copyToLocalFile(false,new Path("/test/accesslog.txt"),new Path("D:\\accesslog.txt"),true);
        System.out.println("下载完成");
    }
    //修改文件名
    @Test
    public void UpdateFileName() throws IOException{
        fs.rename(new Path("/test/wangzili"),new Path("/test/wzl"));
        System.out.println("修改名字完成");
    }
    //查看文件详情 文件名称，权限，长度，块信息
    @Test
    public void CatFile() throws IOException{
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test"),true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
//            文件名称，权限，长度，块信息
            System.out.println("文件名称:"+fileStatus.getPath().getName());
            System.out.println("权限:"+fileStatus.getOwner());
            System.out.println("权限信息:"+fileStatus.getPermission());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("副本保存在："+host);
                }
            }
        }
        System.out.println("信息输出完成！");
    }
    //判断是文件还是文件夹
    @Test
    public void IsDirectory() throws IOException {
        boolean directory = fs.isDirectory(new Path("/test/wzl"));
        boolean file = fs.isFile(new Path("/test/wzl"));
        System.out.println("是否是文件夹："+directory+"==========  是否是文件:"+file);
        System.out.println("判断完成");
    }
    //下载大文件的第一个块
    @Test
    public void readFileSeek1() throws IOException {
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/hadoop-2.6.0-cdh5.7.0.tar.gz"));
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D://hadoop-2.6.0-cdh5.7.0.tar.gz.part0"));
        //流的对接（只拷贝128M)
        byte[] bytes = new byte[1024];
        for (int i=0;i<1024*128;i++){
            fis.read(bytes);
            fos.write(bytes);
        }
        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
    //下载文件第二个块
    @Test
    public void readFileSeek2() throws IOException{
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/hadoop-2.6.0-cdh5.7.0.tar.gz"));
        //设置指定起点
        fis.seek(1024*1024*128);
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D://hadoop-2.6.0-cdh5.7.0.tar.gz.part1"));

        IOUtils.copyBytes(fis,fos,conf);
        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
