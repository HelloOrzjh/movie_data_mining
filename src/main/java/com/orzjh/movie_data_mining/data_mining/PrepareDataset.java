package com.orzjh.movie_data_mining.data_mining;

import com.orzjh.movie_data_mining.data_analysis.DistributionOfMovies;
import com.orzjh.movie_data_mining.util.ChartUtils;
import com.orzjh.movie_data_mining.util.FileIOUtils;
import com.orzjh.movie_data_mining.util.HiveJdbcUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/26 13:15
 * 准备评分数据集
 */
public class PrepareDataset {
    private final String PATH = "./result/ratings-modified.csv";

    private HiveJdbcUtils hiveJdbcUtils;

    private FileIOUtils fileIOUtils;

    public PrepareDataset() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT user_id, movie_id, rating " +
                "FROM ml_25m_ratings " +
                "ORDER BY rating_timestamp ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);
        BufferedWriter out = new BufferedWriter(new FileWriter(PATH,true));

        fileIOUtils.append("userId,movieId,rating\n", out);

        while (res.next()) {
            String line = "";
            line += res.getString("user_id") + ",";
            line += res.getString("movie_id") + ",";
            line += res.getString("rating") + "\n";

            fileIOUtils.append(line, out);
        }
    }

    public static void main(String[] args) throws Exception {
        new PrepareDataset().run();
    }
}
