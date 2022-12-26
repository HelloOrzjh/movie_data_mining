package com.orzjh.movie_data_mining.prepare;

import java.io.*;
import java.util.Scanner;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/18 16:57
 */
public class FormatFile {
    private FileWriter fileWriter;
    private BufferedWriter bufferedWriter;
    private String inputPath;
    private String outputPath;

    public FormatFile(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    private void close() throws IOException {
        bufferedWriter.close();
        fileWriter.close();
    }

    public void format() throws IOException {
        File inputFile = new File(inputPath);
        File outputFile = new File(outputPath);
        fileWriter = new FileWriter(outputFile);
        bufferedWriter = new BufferedWriter(fileWriter);

        Scanner scanner = new Scanner(inputFile);
        int id = -1;
        while (scanner.hasNextLine()) {
            String str = scanner.nextLine();
            if (str.charAt(str.length() - 1) == ':') {
                str = str.substring(0, str.length() - 1);
                id = Integer.parseInt(str);
            } else {
                str = String.valueOf(id) + "," + str + "\n";
                bufferedWriter.write(str);
            }
        }

        System.out.println("format success");
        close();
    }

    public static void main(String[] args) throws IOException {
        new FormatFile("./dataset/netflix_dataset/training_set.txt", "./dataset/netflix_dataset/training_set_modified.txt").format();
        new FormatFile("./dataset/netflix_dataset/probe.txt", "./dataset/netflix_dataset/probe_modified.txt").format();
        new FormatFile("./dataset/netflix_dataset/qualifying.txt", "./dataset/netflix_dataset/qualifying_modified.txt").format();
    }
}
