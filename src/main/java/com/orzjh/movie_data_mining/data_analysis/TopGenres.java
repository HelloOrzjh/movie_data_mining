package com.orzjh.movie_data_mining.data_analysis;

import com.orzjh.movie_data_mining.util.FileIOUtils;
import com.orzjh.movie_data_mining.util.HiveJdbcUtils;

import java.sql.ResultSet;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/22 17:50
 * 选取评分最高的20个标签
 */
public class TopGenres {
    private final String PATH = "./result/topGenres.txt";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;

    public TopGenres() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT ml_25m_movies_genres.genre, AVG(ml_25m_ratings.rating) AS average, COUNT(*) AS cnt " +
                "FROM ml_25m_movies_genres " +
                "JOIN ml_25m_ratings ON ml_25m_movies_genres.movie_id = ml_25m_ratings.movie_id " +
                "GROUP BY ml_25m_movies_genres.genre " +
                "ORDER BY average DESC " +
                "LIMIT 20";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);

        while (res.next()) {
            String line = "";

            line += "genre:" + res.getString("genre") + ", ";
            line += "cnt:" + res.getString("cnt") + ", ";
            line += "average:" + res.getString("average");
            fileIOUtils.appendLine(line);
        }
    }

    public static void main(String[] args) throws Exception {
        new TopGenres().run();
    }
}
