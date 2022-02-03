package pl.michalsz.spark
package model

case class Temperature(temperatureId: BigInt,
                       minimumTemperature: Double,
                       maximumTemperature: Double,
                       description: String)
