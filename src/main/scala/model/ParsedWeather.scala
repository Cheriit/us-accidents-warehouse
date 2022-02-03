package pl.michalsz.spark
package model

import java.sql.Timestamp

case class ParsedWeather(dateTime: Timestamp,
                         weatherAirportCode: String,
                         temperature: Double,
                         visibility: Double,
                         weatherCondition: String)

