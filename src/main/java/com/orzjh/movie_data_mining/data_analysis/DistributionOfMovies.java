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
 * Create by 2022/12/24 20:41
 * 统计电影评分分布情况
 */
public class DistributionOfMovies {
    private final String PATH = "./result/distributionOfMovies.txt";
    private final String CHART_PATH = "./pages/distributionOfMovies.html";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;
    private ChartUtils chartUtils;

    public DistributionOfMovies() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
        chartUtils = new ChartUtils(CHART_PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT movie_id, AVG(rating) AS average " +
                "FROM ml_25m_ratings " +
                "GROUP BY movie_id " +
                "ORDER BY average ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);

        List<String> x_titles = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();

        for(int i = 0; i <= 5; i++) {
            x_titles.add(String.valueOf(i));
        }

        while (res.next()) {
            String line = "";
            line += "average:" + res.getString("average") + ", ";
            fileIOUtils.appendLine(line);

            datas.add(res.getFloat("average"));
        }

        chartUtils.generateBar(x_titles, datas);
    }

    public static void main(String[] args) throws Exception {
         new DistributionOfMovies().run();
    }
}
