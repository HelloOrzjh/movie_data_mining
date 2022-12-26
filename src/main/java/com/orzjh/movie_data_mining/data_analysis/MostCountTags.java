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
 * Create by 2022/12/24 19:39
 * 查找用户标记次数最多的20个标签
 */
public class MostCountTags {
    private final String PATH = "./result/mostCountTags.txt";
    private final String CHART_PATH = "./pages/mostCountTags.html";
    private HiveJdbcUtils hiveJdbcUtils;
    private FileIOUtils fileIOUtils;
    private ChartUtils chartUtils;

    public MostCountTags() {
        hiveJdbcUtils = new HiveJdbcUtils();
        fileIOUtils = new FileIOUtils(PATH);
        chartUtils = new ChartUtils(CHART_PATH);
    }

    public void run() throws Exception {
        String hql = "SELECT tag, COUNT(*) AS cnt " +
                "FROM ml_25m_tags " +
                "GROUP BY tag " +
                "ORDER BY cnt DESC " +
                "LIMIT 20 ";
        ResultSet res = hiveJdbcUtils.executeQuery(hql);

        List<String> keys = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();

        while (res.next()) {
            String line = "";

            line += "tag:" + res.getString("tag") + ", ";
            line += "cnt:" + res.getString("cnt") + ", ";
            fileIOUtils.appendLine(line);

            keys.add(res.getString("tag"));
            values.add(res.getInt("cnt"));
        }

        chartUtils.generatePie(keys, values);
    }

    public static void main(String[] args) throws Exception {
        new MostCountTags().run();
    }
}
