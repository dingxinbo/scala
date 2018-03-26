package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Created by dingxinbo on 12/03/18.
 */
//hadoop hdfs java api 操作
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://localhost:8020";

    FileSystem fileSystem = null ;
    Configuration configuration =null ;

    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void create() throws Exception{
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.text"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    @Test
    public void cat() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.text"));
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }

    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.text");
        Path newPath = new Path("/hdfsapi/test/b.text");
        fileSystem.rename(oldPath,newPath);
    }

    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("/home/dingxinbo/data/hello.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    @Test
    public void copyFromLocalFileWithProgress() throws Exception{

        InputStream in = new BufferedInputStream(new FileInputStream(new File("/home/dingxinbo/App/hadoop-2.6.0-cdh5.7.0.tar.gz")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/hadoop"),
                new Progressable() {
            public void progress() {
               System.out.println("."); //带进度提醒信息
            }
        });

        IOUtils.copyBytes(in,output,4096);

    }

    @Test
    public void copyToLocalFile() throws Exception{

        Path hdfsPath = new Path("/hdfsapi/test/b.text");
        Path localPath = new Path("/home/dingxinbo/data/h.txt");

        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus :fileStatuses){
            String isDir = fileStatus.isDirectory() ? "文件夹" :"文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test"),true  );
    }


    @Before
    public void setUp() throws Exception{
        System.out.println("HDFSApp.setUp");
        configuration =new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration);
    }

    @After
    public void tearDown() throws Exception{

        configuration =null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }

}
