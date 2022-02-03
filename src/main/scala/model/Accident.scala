package pl.michalsz.spark
package model

import java.sql.Date

case class Accident(distance: Double,
                    numberOfAccidents: Array[Byte],
                    severity: Int,
                    timeId: Date,
                    locationId: BigInt,
                    surroundingId: BigInt,
                    temperatureId: BigInt,
                    visibilityId: BigInt,
                    weatherConditionId: BigInt)
