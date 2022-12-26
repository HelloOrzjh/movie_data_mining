package com.orzjh.movie_data_mining.prepare;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/6 20:19
 */
import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class MergeFile {
    private InputStream inputStream;
    private OutputStream outputStream;
    private String localPath;
    private String hdfsPath;

    public MergeFile(String localPath, String hdfsPath) {
        this.localPath = localPath;
        this.hdfsPath = hdfsPath;
    }

    private void writeToOutput(File file) throws IOException {
        if ( file.isFile() ) {
            int len = 0;
            byte[] buffer = new byte[1024];
            inputStream = new BufferedInputStream( new FileInputStream(file) );
            while ( ( len = inputStream.read(buffer) ) > 0 ) {
                outputStream.write(buffer, 0, len);
            }
        } else if ( file.isDirectory() ) {
            File[] listFiles = file.listFiles();
            for (File son_file : listFiles) {
                writeToOutput(son_file);
            }
        }
    }

    private void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }
    }

    private void merge() throws IOException {
        File file = new File(localPath); // 本地文件
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop001:9000");
        FileSystem fs = FileSystem.get(conf);

        outputStream = fs.create( new Path(hdfsPath) );

        writeToOutput(file);
        System.out.println("merge success!");

        byte[] buffer = "".getBytes(StandardCharsets.UTF_8);

        close();
    }

    public static void main(String[] args) throws IOException {
        new MergeFile("./dataset/netflix_dataset/training_set", "/netflix_data_mining/dataset/netflix_dataset/training_set.txt").merge();
    }
}
