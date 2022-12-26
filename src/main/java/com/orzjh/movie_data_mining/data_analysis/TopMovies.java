package com.orzjh.movie_data_mining.data_analysis;

import com.orzjh.movie_data_mining.util.FileIOUtils;
import com.orzjh.movie_data_mining.util.HiveJdbcUtils;

import java.sql.ResultSet;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/22 10:09
 * 选取评分最高的20部电影
 */
public class TopMovies {
    private final String PATH = "./result/topMovies.txt";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;

    public TopMovies() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT movie_id, COUNT(*) AS cnt, AVG(rating) AS average " +
                "FROM ml_25m_ratings " +
                "GROUP BY movie_id " +
                "HAVING COUNT(*) > 2000 " +
                "ORDER BY average DESC " +
                "LIMIT 20 ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);
        String hql2;
        ResultSet res2;

        while (res.next()) {
            String title = "";
            String line = "";

            hql2 = "SELECT * " +
                    "FROM ml_25m_movies " +
                    "WHERE movie_id = " + res.getInt("movie_id");
            res2 = hiveJdbcUtils.executeQuery(hql2);
            while (res2.next()) {
                title = res2.getString("title");
            }

            line += "movie_id:" + res.getString("movie_id") + ", ";
            line += "title:" + title + ", ";
            line += "cnt:" + res.getString("cnt") + ", ";
            line += "average:" + res.getString("average");
            fileIOUtils.appendLine(line);

        }
    }

    public static void main(String[] args) throws Exception {
        new TopMovies().run();
    }
}
