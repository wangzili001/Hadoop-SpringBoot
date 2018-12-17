package Spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 类说明
 * 使用Spring Hadoop 来访问HDFS文件系统
 * @author yoyocheknow
 * @date 2018/8/9 19:42
 */
public class SpringHadoopHDFSApp {
    private ApplicationContext ctx;
    private FileSystem fileSystem;
    //创建目录
    @Test
    public void testMkdir()throws Exception
    {
        fileSystem.mkdirs(new Path("/springhdfs"));
    }

    @Test
    public void testDeldir()throws Exception{
        fileSystem.delete(new Path("/springhdfs"),true);
    }
    //读取HDFS文件内容
    @Test
    public void testText()throws Exception
    {
        FSDataInputStream in = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(in,System.out,2048);
        in.close();
    }

    @Before
    public void setUp(){
        ctx=new ClassPathXmlApplicationContext("beans.xml");
        fileSystem=(FileSystem) ctx.getBean("fileSystem");
    }

    @After
    public void tearDown()throws Exception{
        ctx=null;
        fileSystem.close();
    }
}