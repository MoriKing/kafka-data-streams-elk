package com.github.moriking.kafka.consumer

import org.charts.dataviewer.DataViewer
import org.charts.dataviewer.api.config.DataViewerConfiguration
import org.charts.dataviewer.api.data.PlotData
import org.charts.dataviewer.api.trace.BarTrace

class Plot(title: String, xTitle: String, yTitle: String) {
  private val dataViewer = DataViewer(title.replace(" ", ""))
  private val values = mutableMapOf<String, Long>("" to 0)

  init {
    with(DataViewerConfiguration()) {
      plotTitle = title
      setxAxisTitle(xTitle)
      setyAxisTitle(yTitle)
      showLegend(false)
      dataViewer.updateConfiguration(this)
    }
  }

  fun updateValue(key: String, value: Long) {
    values[key] = value
  }

  fun build() = with(BarTrace<Any>()) {
    setxArray(values.keys.toTypedArray())
    setyArray(values.values.toTypedArray())
    dataViewer.updatePlot(PlotData(this))
  }
}
