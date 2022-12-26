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
 * Create by 2022/12/24 17:02
 * 查找评分次数最多的20个标签
 */
public class MostCountGenres {
    private final String PATH = "./result/mostCountGenres.txt";
    private final String CHART_PATH = "./pages/mostCountGenres.html";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;
    private ChartUtils chartUtils;

    public MostCountGenres() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
        chartUtils = new ChartUtils(CHART_PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT b.genre, COUNT(*) AS cnt " +
                "FROM ml_25m_ratings a " +
                "JOIN ml_25m_movies_genres b " +
                "ON a.movie_id = b.movie_id " +
                "GROUP BY b.genre " +
                "ORDER BY cnt DESC " +
                "LIMIT 20 ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);

        List<String> keys = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();

        while (res.next()) {
            String line = "";

            line += "genre:" + res.getString("b.genre") + ", ";
            line += "cnt:" + res.getString("cnt") + ", ";
            fileIOUtils.appendLine(line);

            keys.add(res.getString("b.genre"));
            values.add(res.getInt("cnt"));
        }

        chartUtils.generatePie(keys, values);
    }

    public static void main(String[] args) throws Exception {
        new MostCountGenres().run();
    }
}
