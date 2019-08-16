package ml.als

import java.awt.Color

import org.jfree.chart.{ChartFactory, ChartFrame}
import org.jfree.chart.axis.{AxisLocation, NumberAxis}
import org.jfree.chart.labels.StandardCategoryToolTipGenerator
import org.jfree.chart.plot.DatasetRenderingOrder
import org.jfree.chart.renderer.category.LineAndShapeRenderer
import org.jfree.data.category.DefaultCategoryDataset

object Chart {

  def plotBarLineChart(title: String, xLable: String, yBarLabel: String, yBarMin: Double, yBarMax: Double, yLineLabel: String, dataBarChart: DefaultCategoryDataset, dataLinechart: DefaultCategoryDataset): Unit = {

    // draw bar chart
    val chart = ChartFactory
      .createBarChart(
        "",
        xLable,
        yBarLabel,
        dataBarChart,
        org.jfree.chart.plot.PlotOrientation.VERTICAL,
        true, // include legend
        true, // display tooltips
        false // remove url generator
      )

    val plot = chart.getCategoryPlot
    plot.setBackgroundPaint(new Color(255, 255, 255))
    plot.setDomainAxisLocation(AxisLocation.BOTTOM_OR_RIGHT)
    plot.setDataset(1, dataLinechart)
    plot.mapDatasetToRangeAxis(1, 1)

    // draw bar y
    val vn = plot.getRangeAxis
    vn.setRange(yBarMin, yBarMax)
    vn.setAutoTickUnitSelection(true)

    // draw line y
    val axis2 = new NumberAxis(yLineLabel)
    plot.setRangeAxis(1, axis2)
    val renderer2 = new LineAndShapeRenderer()
    renderer2.setBaseToolTipGenerator(new StandardCategoryToolTipGenerator())

    // draw bar first
    plot.setRenderer(1, renderer2)
    plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD)

    val frame = new ChartFrame(title, chart)
    frame.setSize(10, 10)
    frame.pack()
    frame.setVisible(true)

  }

}
