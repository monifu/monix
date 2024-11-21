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

import cats.effect.{ Async, ExitCase, IO, Sync }

object Overrides {

  implicit val syncIO: Sync[IO] = new CustomSyncIO

  implicit val asyncIO: Async[IO] =
    new CustomSyncIO with Async[IO] {
      def async[A](k: (Either[Throwable, A] => Unit) => Unit): IO[A] =
        IO.ioEffect.async_(k)
      def asyncF[A](k: (Either[Throwable, A] => Unit) => IO[Unit]): IO[A] =
        IO.ioEffect.asyncF(k)
    }

  class CustomSyncIO extends Sync[IO] {
    def suspend[A](thunk: => IO[A]): IO[A] =
      IO.ioEffect.defer(thunk)
    def bracketCase[A, B](acquire: IO[A])(use: A => IO[B])(release: (A, ExitCase[Throwable]) => IO[Unit]): IO[B] =
      IO.ioEffect.bracketCase(acquire)(use)(release)
    def raiseError[A](e: Throwable): IO[A] =
      IO.ioEffect.raiseError(e)
    def handleErrorWith[A](fa: IO[A])(f: Throwable => IO[A]): IO[A] =
      IO.ioEffect.handleErrorWith(fa)(f)
    def pure[A](x: A): IO[A] =
      IO.ioEffect.pure(x)
    def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] =
      IO.ioEffect.flatMap(fa)(f)
    def tailRecM[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
      IO.ioEffect.tailRecM(a)(f)
  }
}
