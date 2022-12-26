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
 * Create by 2022/12/24 16:27
 * 选取评分次数最多的20个电影并画柱形图
 */
public class MostCountMovies {
    private final String PATH = "./result/mostCountMovies.txt";
    private final String CHART_PATH = "./pages/mostCountMovies.html";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;
    private ChartUtils chartUtils;

    public MostCountMovies() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
        chartUtils = new ChartUtils(CHART_PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT movie_id, COUNT(*) AS cnt " +
                "FROM ml_25m_ratings " +
                "GROUP BY movie_id " +
                "HAVING COUNT(*) > 2000 " +
                "ORDER BY cnt DESC " +
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
            line += "cnt:" + res.getString("cnt") + ", ";
            fileIOUtils.appendLine(line);

            x_titles.add(title);
            datas.add(res.getInt("cnt"));
        }

        chartUtils.generateBar(x_titles, datas);
    }

    public static void main(String[] args) throws Exception {
        new MostCountMovies().run();
    }
}
