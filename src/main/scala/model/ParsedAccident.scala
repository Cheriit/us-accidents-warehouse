package pl.michalsz.spark
package model

import java.sql.Timestamp

case class ParsedAccident(id: String,
                          startTime: Timestamp,
                          endTime: Timestamp,
                          distance: Double,
                          severity: Int,
                          street: String,
                          zipcode: String,
                          crossing: Boolean,
                          railway: Boolean,
                          stop: Boolean,
                          airportCode: String)
