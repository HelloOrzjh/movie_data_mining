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
 * Create by 2022/12/24 20:31
 * 统计用户评分分布情况
 */
public class DistributionOfRatings {
    private final String PATH = "./result/distributionOfRatings.txt";
    private final String CHART_PATH = "./pages/distributionOfRatings.html";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;
    private ChartUtils chartUtils;

    public DistributionOfRatings() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
        chartUtils = new ChartUtils(CHART_PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT rating, COUNT(*) AS cnt " +
                "FROM ml_25m_ratings " +
                "GROUP BY rating " +
                "ORDER BY rating ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);

        List<String> x_titles = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();

        while (res.next()) {
            String line = "";

            line += "rating:" + res.getString("rating") + ", ";
            line += "cnt:" + res.getString("cnt") + ", ";
            fileIOUtils.appendLine(line);

            x_titles.add(res.getString("rating"));
            datas.add(res.getInt("cnt"));
        }

//        chartUtils.generateLine(x_titles, datas, true);
        chartUtils.generateBar(x_titles, datas);
    }

    public static void main(String[] args) throws Exception {
        new DistributionOfRatings().run();
    }
}
