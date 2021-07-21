package com.github.moriking.kafka.consumer

import org.charts.dataviewer.DataViewer
import org.charts.dataviewer.api.config.DataViewerConfiguration
import org.charts.dataviewer.api.data.PlotData
import org.charts.dataviewer.api.trace.BarTrace

class Plot(title: String, xTitle: String, yTitle: String) {
  private val dataViewer = DataViewer(title)
  private val values = mutableMapOf<String, Long>()

  init {
    with (DataViewerConfiguration()) {
      plotTitle = title
      setxAxisTitle(xTitle)
      setyAxisTitle(yTitle)
      dataViewer.updateConfiguration(this)
    }
  }

  fun updateValue(key: String, value: Long) {
    values[key] = value
  }

  fun build() {
    dataViewer.resetPlot()

    val barTrace = BarTrace<Any>()
    barTrace.setxArray(values.keys.toTypedArray())
    barTrace.setyArray(values.values.toTypedArray())
    val plotData = PlotData(barTrace)
    dataViewer.updatePlot(plotData)
  }
}

fun main() {
  val plot = Plot("title", "Xs", "Ys")
  plot.updateValue("One", 12)
  plot.updateValue("Two", 14)
  plot.updateValue("Three", 2)
  plot.build()
}
