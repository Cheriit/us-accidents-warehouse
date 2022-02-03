package pl.michalsz.spark
package model

case class Surrounding(surroundingId: BigInt,
                       crossing: Boolean,
                       railway: Boolean,
                       stop: Boolean)
