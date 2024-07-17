package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class VisitCountResponse(
                       url: String,
                       count: Long
                     )

object VisitCountResponse {
  implicit val format: OFormat[VisitCountResponse] = Json.format[VisitCountResponse]
}
