package pl.michalsz.spark
package model

case class Location(locationId: BigInt,
                    zipcode: String,
                    airportCode: String,
                    city: String,
                    county: String,
                    state: String,
                    country: String,
                    street: String)
