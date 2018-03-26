package com.imooc.hadoop.hdfs.spring;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * Created by dingxinbo on 22/03/18.
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {

    @Autowired
    FsShell fsShell;

    @Override
    public void run(String... strings) throws Exception {
        for(FileStatus fileStatus:fsShell.lsr("/output")){
            System.out.println(">"+fileStatus.getPath());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHDFSApp.class,args);
    }
}
