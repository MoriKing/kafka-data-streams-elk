package com.github.moriking.kafka.consumer

import org.charts.dataviewer.DataViewer
import org.charts.dataviewer.api.config.DataViewerConfiguration
import org.charts.dataviewer.api.data.PlotData
import org.charts.dataviewer.api.trace.BarTrace
import java.util.*

class Plot(
  private val title: String,
  private val xTitle: String,
  private val yTitle: String,
  private val marginBottom: Int = 60
) {
  private val dataViewer = DataViewer(title.replace(" ", ""))
  private val values = TreeMap<String, Long>(mapOf("" to 0))

  fun updateValue(key: String, value: Long) {
    values[key] = value
  }

  fun build() {
    dataViewer.resetPlot()
    updateConfiguration()
    with(BarTrace<Any>()) {
      setxArray(values.keys.toTypedArray())
      setyArray(values.values.toTypedArray())
      dataViewer.updatePlot(PlotData(this))
    }
  }

  private fun updateConfiguration() = with(DataViewerConfiguration()) {
    plotTitle = title
    marginBottom = this@Plot.marginBottom
    setxAxisTitle(xTitle)
    setyAxisTitle(yTitle)
    showLegend(false)
    dataViewer.updateConfiguration(this)
  }
}