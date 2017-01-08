package com.evojam.play.elastic4s

import scala.language.implicitConversions
import play.api.libs.json.{Json, Reads, Writes}
import com.sksamuel.elastic4s.{Hit, HitAs, HitReader, Indexable}
import com.evojam.play.elastic4s.json.GetResponseWithJson
import com.sksamuel.elastic4s.get.RichGetResponse

/**
  * Provides interoperability with Play JSON formatters.
  */
trait PlayElasticJsonSupport {
  implicit def playElasticJsonGetResponsePimp(r: RichGetResponse): GetResponseWithJson =
    new GetResponseWithJson(r.original)

  implicit def jsonReadsToHitAs[A: Reads]: HitReader[A] = new HitReader[A] {
    override def read(hit: Hit): Either[Throwable, A] = {
      try {
        Right(Json.parse(hit.sourceAsString).as[A])
      } catch {
        case e: Throwable => Left(e)
      }
    }
  }

  implicit def jsonWritesToIndexable[A: Writes]: Indexable[A] = new Indexable[A] {
    override def json(t: A): String = Json.toJson(t).toString()
  }

}
