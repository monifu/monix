/*
 * Copyright (c) 2014-2022 Monix Contributors.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.catnap

import cats.~>
import cats.effect.{ Async, Concurrent }
import monix.execution.CancelableFuture
import monix.execution.internal.AttemptCallback
import monix.execution.schedulers.TrampolineExecutionContext.immediate
import scala.concurrent.{ Future => ScalaFuture }

/**
  * A type class for conversions from [[scala.concurrent.Future]] or
  * other Future-like data type (e.g. Java's `CompletableFuture`).
  *
  * N.B. to use its syntax, you can import [[monix.catnap.syntax]]:
  * {{{
  *   import monix.catnap.syntax._
  *   import scala.concurrent.Future
  *   // Used here only for Future.apply as the ExecutionContext
  *   import monix.execution.Scheduler.Implicits.global
  *   // Can use any data type implementing Async or Concurrent
  *   import cats.effect.IO
  *
  *   val io = IO(Future(1 + 1)).futureLift
  * }}}
  *
  * `IO` provides its own `IO.fromFuture` of course, however
  * `FutureLift` is generic and works with
  * [[monix.execution.CancelableFuture CancelableFuture]] as well.
  *
  * {{{
  *   import monix.execution.{CancelableFuture, Scheduler, FutureUtils}
  *   import scala.concurrent.Promise
  *   import scala.concurrent.duration._
  *   import scala.util.Try
  *
  *   def delayed[A](event: => A)(implicit s: Scheduler): CancelableFuture[A] = {
  *     val p = Promise[A]()
  *     val c = s.scheduleOnce(1.second) { p.complete(Try(event)) }
  *     CancelableFuture(p.future, c)
  *   }
  *
  *   // The result will be cancelable:
  *   val sum: IO[Int] = IO(delayed(1 + 1)).futureLift
  * }}}
  */
trait FutureLift[F[_], Future[_]] extends (FutureLift.Lambda[F, Future, *] ~> F) {
  def apply[A](fa: F[Future[A]]): F[A]
}

object FutureLift extends internal.FutureLiftForPlatform {
  type Lambda[F[_], Future[_], A] = F[Future[A]]

  /**
    * Accessor for [[FutureLift]] values that are in scope.
    * {{{
    *   import cats.effect.IO
    *   import scala.concurrent.Future
    *   import scala.concurrent.ExecutionContext.Implicits.global
    *
    *   val F = FutureLift[IO, Future]
    *
    *   val task: IO[Int] = F.apply(IO(Future(1 + 1)))
    * }}}
    */
  def apply[F[_], Future[_]](implicit F: FutureLift[F, Future]): FutureLift[F, Future] = F

  /**
    * Applies [[FutureLift.apply]] to the given parameter.
    *
    * {{{
    *   import cats.effect.IO
    *   import scala.concurrent.Future
    *   import scala.concurrent.ExecutionContext.Implicits.global
    *
    *   val ioa = FutureLift.from(IO(Future(1 + 1)))
    * }}}
    */
  def from[F[_], Future[_], A](fa: F[Future[A]])(implicit F: FutureLift[F, Future]): F[A] =
    F.apply(fa)

  /**
    * Utility for converting [[scala.concurrent.Future Future]] values into
    * data types that implement
    * [[https://typelevel.org/cats-effect/typeclasses/async.html cats.effect.Async]].
    *
    * N.B. the implementation discriminates
    * [[monix.execution.CancelableFuture CancelableFuture]] via sub-typing,
    * and if the given future is cancelable, then the resulting instance
    * is also cancelable.
    */
  def scalaToAsync[F[_], MF[T] <: ScalaFuture[T], A](fa: F[MF[A]])(implicit F: Async[F]): F[A] =
    F.flatMap(fa) { future =>
      future.value match {
        case Some(value) => F.fromTry(value)
        case _ => startAsync(future)
      }
    }

  /**
    * Utility for converting [[scala.concurrent.Future Future]] values into
    * data types that implement
    * [[https://typelevel.org/cats-effect/typeclasses/concurrent.html cats.effect.Concurrent]].
    *
    * N.B. the implementation discriminates
    * [[monix.execution.CancelableFuture CancelableFuture]] via sub-typing,
    * and if the given future is cancelable, then the resulting instance
    * is also cancelable.
    */
  def scalaToConcurrent[F[_], MF[T] <: ScalaFuture[T], A](fa: F[MF[A]])(implicit F: Concurrent[F]): F[A] =
    F.flatMap(fa) { future =>
      future.value match {
        case Some(value) => F.fromTry(value)
        case _ =>
          future match {
            case cf: CancelableFuture[A] @unchecked =>
              startCancelable(cf)
            case _ =>
              startAsync(future)
          }
      }
    }

  /**
    * A generic function that subsumes both [[scalaToAsync]] and
    * [[scalaToConcurrent]].
    *
    * N.B. this works with [[monix.execution.CancelableFuture]]
    * if the given `Future` is such an instance.
    */
  def scalaToConcurrentOrAsync[F[_], MF[T] <: ScalaFuture[T], A](fa: F[MF[A]])(
    implicit F: Concurrent[F] OrElse Async[F]
  ): F[A] = {

    F.unify match {
      case ref: Concurrent[F] @unchecked =>
        scalaToConcurrent[F, MF, A](fa)(ref)
      case ref =>
        scalaToAsync[F, MF, A](fa)(ref)
    }
  }

  /**
    * Implicit instance of [[FutureLift]] for converting from
    * [[scala.concurrent.Future]] or [[monix.execution.CancelableFuture]] to
    * any `Concurrent` or `Async` data type.
    */
  implicit def scalaFutureLiftForConcurrentOrAsync[F[_], MF[T] <: ScalaFuture[T]](
    implicit F: Concurrent[F] OrElse Async[F]
  ): FutureLift[F, MF] = {

    F.unify match {
      case ref: Concurrent[F] @unchecked =>
        new FutureLift[F, MF] {
          def apply[A](fa: F[MF[A]]): F[A] =
            scalaToConcurrent[F, MF, A](fa)(ref)
        }
      case ref =>
        new FutureLift[F, MF] {
          def apply[A](fa: F[MF[A]]): F[A] =
            scalaToAsync[F, MF, A](fa)(ref)
        }
    }
  }

  /**
    * Provides extension methods when imported in scope via [[syntax]].
    *
    * {{{
    *   import monix.catnap.syntax._
    * }}}
    */
  trait Syntax[F[_], Future[_], A] extends Any {
    def source: F[Future[A]]

    /**
      * Lifts a `Future` data type into `F[_]`, thus performing a conversion.
      *
      * See [[FutureLift]].
      */
    def futureLift(implicit F: FutureLift[F, Future]): F[A] =
      F.apply(source)
  }

  /**
    * Deprecated method, which happened on extending `FunctionK`.
    */
  implicit class Deprecated[F[_], Future[_]](val inst: FutureLift[F, Future]) {
    /** DEPRECATED — switch to [[FutureLift.apply]]. */
    @deprecated("Switch to FutureLift.apply", since = "3.0.0-RC3")
    def futureLift[A](fa: F[Future[A]]): F[A] = {
      // $COVERAGE-OFF$
      inst(fa)
      // $COVERAGE-ON$
    }
  }

  private def start[A](fa: ScalaFuture[A], cb: Either[Throwable, A] => Unit): Unit = {
    implicit val ec = immediate
    fa.onComplete(AttemptCallback.toTry(cb))
  }

  private def startAsync[F[_], A](fa: ScalaFuture[A])(implicit F: Async[F]): F[A] =
    F.async_ { cb =>
      start(fa, cb)
    }

  private def startCancelable[F[_], A](fa: CancelableFuture[A])(implicit F: Concurrent[F]): F[A] =
    F.cancelable { cb =>
      start(fa, cb)
      F.delay(fa.cancel())
    }
}
