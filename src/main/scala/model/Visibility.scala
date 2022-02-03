package pl.michalsz.spark
package model

case class Visibility(visibilityId: BigInt,
                      minimumDistance: Double,
                      maximumDistance: Double,
                      description: String)
