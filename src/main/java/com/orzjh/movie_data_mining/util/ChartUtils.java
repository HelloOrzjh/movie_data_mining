package com.orzjh.movie_data_mining.util;

import org.icepear.echarts.Bar;
import org.icepear.echarts.Line;
import org.icepear.echarts.Pie;
import org.icepear.echarts.charts.line.LineSeries;
import org.icepear.echarts.charts.pie.PieDataItem;
import org.icepear.echarts.render.Engine;

import java.util.List;

/**
 * @author Orzjh
 * @version 1.0
 * Create by 2022/12/23 20:47
 */
public class ChartUtils {
    private String outputPath;

    public ChartUtils(String outputPath) {
        this.outputPath = outputPath;
    }

    public void generateBar(String[] x_titles, Object[] datas) {
        Bar bar = new Bar()
                .setLegend()
                .setTooltip("item")
                .addXAxis(x_titles)
                .addYAxis()
                .addSeries(datas);

        Engine engine = new Engine();
        engine.render(outputPath, bar);
    }

    public void generateBar(List<String> x_titles, List<Object> datas) {
        generateBar(x_titles.toArray(new String[x_titles.size()]), datas.toArray(new Object[datas.size()]));
    }

    public void generateLine(String[] x_titles, Object[] datas, boolean isSmooth) {
        Line line = new Line()
                .setLegend()
                .setTooltip("item")
                .addXAxis(x_titles)
                .addYAxis()
                .addSeries(new LineSeries()
                        .setData(datas)
                        .setSmooth(isSmooth));

        Engine engine = new Engine();
        engine.render(outputPath, line);
    }

    public void generateLine(List<String> x_titles, List<Object> datas, boolean isSmooth) {
        generateLine(x_titles.toArray(new String[x_titles.size()]), datas.toArray(new Object[datas.size()]), isSmooth);
    }

    public void generatePie(String[] keys, Integer[] values) {
        PieDataItem[] pieDataItems = new PieDataItem[values.length];
        for(int i = 0; i < values.length; i++) {
            pieDataItems[i] = new PieDataItem().setName(keys[i]).setValue(values[i]);
        }

        Pie pie = new Pie()
                .setLegend()
                .setTooltip("item")
                .addSeries(pieDataItems);

        Engine engine = new Engine();
        engine.render(outputPath, pie);
    }

    public void generatePie(List<String> keys, List<Object> values) {
        generatePie(keys.toArray(new String[keys.size()]), values.toArray(new Integer[values.size()]));
    }
}
