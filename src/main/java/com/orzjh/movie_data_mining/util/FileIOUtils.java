package com.orzjh.movie_data_mining.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/21 19:10
 */
public class FileIOUtils {
    private final static Logger LOG = LoggerFactory.getLogger(HiveJdbcUtils.class);
    private String filename = null;
    private File file = null;

    public FileIOUtils(String filename) {

        this.filename = filename;
        this.file = new File(filename);
    }

    public File getFile() {
        return file;
    }

    public void append(String content) throws IOException {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filename,true));
            out.write(content);
            out.close();
        } catch (Exception e) {
            LOG.error("File append failed, msg is " + e.getMessage());
        }
    }

    public void appendLine(String content) throws IOException {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filename,true));
            out.write(content + "\n");
            out.close();
        } catch (Exception e) {
            LOG.error("File appendLine failed, msg is " + e.getMessage());
        }
    }

    public void append(String content, BufferedWriter out) throws IOException {
        try {
            out.write(content);
        } catch (Exception e) {
            LOG.error("File append failed, msg is " + e.getMessage());
        }
    }

    public void appendLine(String content, BufferedWriter out) throws IOException {
        try {
            out.write(content + "\n");
        } catch (Exception e) {
            LOG.error("File appendLine failed, msg is " + e.getMessage());
        }
    }
}
