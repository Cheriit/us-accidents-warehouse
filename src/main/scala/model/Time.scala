package pl.michalsz.spark
package model

import java.sql.Date

case class Time(timeId: Date,
                year: Short,
                month: Short,
                day: Short,
                dayOfWeek: String)
