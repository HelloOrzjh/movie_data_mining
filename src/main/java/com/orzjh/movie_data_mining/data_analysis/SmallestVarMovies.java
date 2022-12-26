package com.orzjh.movie_data_mining.data_analysis;

import com.orzjh.movie_data_mining.util.ChartUtils;
import com.orzjh.movie_data_mining.util.FileIOUtils;
import com.orzjh.movie_data_mining.util.HiveJdbcUtils;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/24 19:58
 * 查找评分方差最小的20部电影
 */
public class SmallestVarMovies {
    private final String PATH = "./result/smallestVarMovies.txt";
    private final String CHART_PATH = "./pages/smallestVarMovies.html";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;
    private ChartUtils chartUtils;

    public SmallestVarMovies() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
        chartUtils = new ChartUtils(CHART_PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT movie_id, VARIANCE(rating) AS var " +
                "FROM ml_25m_ratings " +
                "GROUP BY movie_id " +
                "HAVING COUNT(rating) > 2000 " +
                "ORDER BY var " +
                "LIMIT 20 ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);
        String hql2;
        ResultSet res2;

        List<String> x_titles = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();

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
            line += "var:" + res.getString("var") + ", ";
            fileIOUtils.appendLine(line);

        }
    }

    public static void main(String[] args) throws Exception {
        new SmallestVarMovies().run();
    }
}
