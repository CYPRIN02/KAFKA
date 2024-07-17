package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class MeanLatencyForURLResponse(
                                 url: String,
                                 meanLatency: Long
                               )


object MeanLatencyForURLResponse {
  implicit val format: OFormat[MeanLatencyForURLResponse] = Json.format[MeanLatencyForURLResponse]
}
