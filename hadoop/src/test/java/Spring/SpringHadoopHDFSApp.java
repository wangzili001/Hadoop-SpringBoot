package Spring;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

public class SpringHadoopHDFSApp {
    private ApplicationContext ctx;
    private FileSystem fileSystem;

    @Test
    public  void testMkdir() throws IOException {
        fileSystem.mkdirs(new Path("/springhdfs/"));
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
