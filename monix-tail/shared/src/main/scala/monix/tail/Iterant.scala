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

package monix.tail

import java.io.PrintStream

import cats.implicits._
import cats.effect.{ Async, Effect, Sync, _ }
import cats.{
  ~>,
  Applicative,
  CoflatMap,
  Defer,
  Eq,
  Functor,
  FunctorFilter,
  MonadError,
  Monoid,
  MonoidK,
  Order,
  Parallel,
  StackSafeMonad
}
import monix.catnap.{ ConcurrentChannel, ConsumerF }
import monix.execution.BufferCapacity.Bounded
import monix.execution.{ BufferCapacity, ChannelType }
import monix.execution.ChannelType.{ MultiProducer, SingleConsumer }
import monix.execution.annotations.UnsafeProtocol
import monix.execution.compat.internal._

import scala.util.control.NonFatal
import monix.execution.internal.Platform.{ recommendedBatchSize, recommendedBufferChunkSize }
import monix.tail.batches.{ Batch, BatchCursor }
import monix.tail.internal._
import monix.tail.internal.Constants.emptyRef
import org.reactivestreams.Publisher

import scala.collection.immutable.LinearSeq
import scala.collection.mutable
import scala.concurrent.duration.{ Duration, FiniteDuration }
import cats.effect.Temporal

/** The `Iterant` is a type that describes lazy, possibly asynchronous
  * streaming of elements using a pull-based protocol.
  *
  * It is similar somewhat in spirit to Scala's own
  * `collection.immutable.Stream` and with Java's `Iterable`, except
  * that it is more composable and more flexible due to evaluation being
  * controlled by an `F[_]` monadic type that you have to supply
  * (like `monix.eval.Task`, `monix.eval.Coeval` or
  * `cats.effect.IO`) which will control the evaluation. In other words,
  * this `Iterant` type is capable of strict or lazy, synchronous or
  * asynchronous evaluation.
  *
  * Consumption of an `Iterant` happens typically in a loop where
  * the current step represents either a signal that the stream
  * is over, or a (head, rest) pair, very similar in spirit to
  * Scala's standard `List` or `Iterable`.
  *
  * The type is an ADT, meaning a composite of the following types:
  *
  *  - [[monix.tail.Iterant.Next Next]] which signals a single strict
  *    element, the `head` and a `rest` representing the rest of the stream
  *
  *  - [[monix.tail.Iterant.NextBatch NextBatch]] is a variation on `Next`
  *    for signaling a whole batch of elements by means of a
  *    [[monix.tail.batches.Batch Batch]], a type that's similar with
  *    Scala's `Iterable`, along with the `rest` of the stream.
  *
  *  - [[monix.tail.Iterant.NextCursor NextCursor]] is a variation on `Next`
  *    for signaling a whole strict batch of elements as a traversable
  *    [[monix.tail.batches.BatchCursor BatchCursor]], a type that's similar
  *    with Scala's `Iterator`, along with the `rest` of the stream.
  *
  *  - [[monix.tail.Iterant.Suspend Suspend]] is for suspending the
  *    evaluation of a stream.
  *
  *  - [[monix.tail.Iterant.Concat Concat]] represents the concatenation
  *    of two streams.
  *
  *  - [[monix.tail.Iterant.Scope Scope]] is to specify the acquisition
  *    and release of resources. It is effectively the encoding of
  *    [[https://typelevel.org/cats-effect/typeclasses/bracket.html Bracket]].
  *
  *  - [[monix.tail.Iterant.Halt Halt]] represents an empty stream,
  *    signaling the end, either in success or in error.
  *
  *  - [[monix.tail.Iterant.Last Last]] represents a one-element
  *    stream, where `Last(item)` as an optimisation on
  *    `Next(item, F.pure(Halt(None)), F.unit)`.
  *
  * ==Parametric Polymorphism==
  *
  * The `Iterant` type accepts as type parameter an `F` monadic type
  * that is used to control how evaluation happens. For example you can
  * use `monix.eval.Task`, in which case the streaming can have
  * asynchronous behavior, or you can use `monix.eval.Coeval`
  * in which case it can behave like a normal, synchronous `Iterable`.
  *
  * As restriction, this `F[_]` type used should be stack safe in
  * `map` and `flatMap`, otherwise you might get stack-overflow
  * exceptions. This is why in general the type class required
  * for `F` is `cats.effect.Sync`.
  *
  * When building instances, type `F[_]` which handles the evaluation
  * needs to be specified upfront. Example:
  *
  * {{{
  *   import cats.effect.IO
  *   import monix.eval.{Task, Coeval}
  *
  *   // Builds an Iterant powered by Monix's Task
  *   Iterant[Task].of(1, 2, 3)
  *
  *   // Builds an Iterant powered by Monix's Coeval
  *   Iterant[Coeval].of(1, 2, 3)
  *
  *   // Builds an Iterant powered by Cats's IO
  *   Iterant[IO].of(1, 2, 3)
  * }}}
  *
  * You'll usually pick between `Task`, `Coeval` or `IO` for your
  * needs.
  *
  * ==Attribution==
  *
  * This type was inspired by the `Streaming` type in the
  * [[https://typelevel.org/cats/ Typelevel Cats]] library (later moved
  * to [[https://github.com/stew/dogs Dogs]]), originally committed in
  * Cats by Erik Osheim. It was also inspired by other push-based
  * streaming abstractions, like the `Iteratee` or `IAsyncEnumerable`.
  *
  * @tparam F is the data type that controls evaluation; note that
  *         it must be stack-safe in its `map` and `flatMap`
  *         operations
  *
  * @tparam A is the type of the elements produced by this Iterant
  *
  * @define catsOrderInterop ==Cats Order and Scala Interop==
  *
  *         Monix prefers to work with [[cats.Order]] for assessing the order
  *         of elements that have an ordering defined, instead of
  *         [[scala.math.Ordering]].
  *
  *         We do this for consistency, as Monix is now building on top of Cats.
  *         This may change in the future, depending on what happens with
  *         [[https://github.com/typelevel/cats/issues/2455 typelevel/cats#2455]].
  *
  *         Building a `cats.Order` is easy to do if you already have a
  *         Scala `Ordering` instance:
  *         {{{
  *           import cats.Order
  *
  *           case class Person(name: String, age: Int)
  *
  *           // Starting from a Scala Ordering
  *           implicit val scalaOrderingForPerson: Ordering[Person] =
  *             new Ordering[Person] {
  *               def compare(x: Person, y: Person): Int =
  *                 x.age.compareTo(y.age) match {
  *                   case 0 => x.name.compareTo(y.name)
  *                   case o => o
  *                 }
  *             }
  *
  *           // Building a cats.Order from it
  *           implicit val catsOrderForPerson: Order[Person] =
  *             Order.fromOrdering
  *         }}}
  *
  *         You can also do that in reverse, so you can prefer `cats.Order`
  *         (due to Cats also exposing laws and tests for free) and build a
  *         Scala `Ordering` when needed:
  *         {{{
  *           val scalaOrdering = catsOrderForPerson.toOrdering
  *         }}}
  *
  * @define catsEqInterop ==Cats Eq and Scala Interop==
  *
  *         Monix prefers to work with [[cats.Eq]] for assessing the equality
  *         of elements that have an ordering defined, instead of
  *         [[scala.math.Equiv]].
  *
  *         We do this because Scala's `Equiv` has a default instance defined
  *         that's based on universal equality and that's a big problem, because
  *         when using the `Eq` type class, it is universal equality that we
  *         want to avoid and there have been countless of bugs in the ecosystem
  *         related to both universal equality and `Equiv`. Thankfully people
  *         are working to fix it.
  *
  *         We also do this for consistency, as Monix is now building on top of
  *         Cats. This may change in the future, depending on what happens with
  *         [[https://github.com/typelevel/cats/issues/2455 typelevel/cats#2455]].
  *
  *         Defining `Eq` instance is easy and we can use universal equality
  *         in our definitions as well:
  *         {{{
  *           import cats.Eq
  *
  *           case class Address(host: String, port: Int)
  *
  *           implicit val eqForAddress: Eq[Address] =
  *             Eq.fromUniversalEquals
  *         }}}
  */
sealed abstract class Iterant[F[_], A] extends Product with Serializable {
  self =>

  import Iterant._

  /** @see [[Iterant.Visitor]]. */
  private[tail] def accept[R](visitor: Iterant.Visitor[F, A, R]): R

  /** Appends a stream to the end of the source, effectively
    * concatenating them.
    *
    * The right hand side is suspended in the `F[_]` data type, thus
    * allowing for laziness.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3, 4
    *   Iterant[Task].of(1, 2) ++ Task.eval {
    *     Iterant[Task].of(3, 4)
    *   }
    * }}}
    *
    * @param rhs is the iterant to append at the end of our source.
    */
  final def ++[B >: A](rhs: F[Iterant[F, B]])(implicit F: Sync[F]): Iterant[F, B] =
    IterantConcat.concat(self.upcast[B], rhs)

  /** Prepends an element to the iterant, returning a new
    * iterant that will start with the given `head` and then
    * continue with the source.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3, 4
    *   1 +: Iterant[Task].of(2, 3, 4)
    * }}}
    *
    * @param head is the element to prepend at the start of
    *        this iterant
    */
  final def +:[B >: A](head: B)(implicit F: Applicative[F]): Iterant[F, B] =
    Next(head, F.pure(self.upcast[B]))

  /** Appends the right hand side element to the end of this iterant.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3, 4
    *   Iterant[Task].of(1, 2, 3) :+ 4
    * }}}
    *
    * @param elem is the element to append at the end
    */
  final def :+[B >: A](elem: B)(implicit F: Sync[F]): Iterant[F, B] =
    IterantConcat.concat[F, B](this.upcast[B], F.pure(Iterant.lastS(elem)))(F)

  /** Appends the given stream to the end of the source, effectively
    * concatenating them.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3, 4
    *   Iterant[Task].of(1, 2) ++ Iterant[Task].of(3, 4)
    * }}}
    *
    * @param rhs is the (right hand side) lazily evaluated iterant to concatenate at
    *        the end of this iterant.
    */
  final def ++[B >: A](rhs: => Iterant[F, B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantConcat.concat(this.upcast[B], F.delay(rhs))(F)

  /** Explicit covariance operator.
    *
    * The [[Iterant]] type isn't covariant in type param `A`, because
    * covariance doesn't play well with a higher-kinded type like
    * `F[_]`.  So in case you have an `Iterant[F, A]`, but need an
    * `Iterant[F, B]`, knowing that `A extends B`, then you can do an
    * `upcast`.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val source: Iterant[Task, List[Int]] = Iterant.suspend(Task[Iterant[Task, List[Int]]](???))
    *
    *   // This will trigger an error because of the invariance:
    *   // val sequences: Iterant[Task, Seq[Int]] = source
    *
    *   // But this will work just fine:
    *   val sequence: Iterant[Task, Seq[Int]] = source.upcast[Seq[Int]]
    * }}}
    */
  final def upcast[B >: A]: Iterant[F, B] =
    this.asInstanceOf[Iterant[F, B]]

  /** Converts the source `Iterant` that emits `A` elements into an
    * iterant that emits `Either[Throwable, A]`, thus materializing
    * whatever error that might interrupt the stream.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import monix.execution.exceptions.DummyException
    *
    *   // Yields Right(1), Right(2), Right(3)
    *   Iterant[Task].of(1, 2, 3).attempt
    *
    *   // Yields Right(1), Right(2), Left(DummyException())
    *   (Iterant[Task].of(1, 2) ++
    *     Iterant[Task].raiseError[Int](DummyException("dummy"))).attempt
    * }}}
    */
  final def attempt(implicit F: Sync[F]): Iterant[F, Either[Throwable, A]] =
    IterantAttempt(self)

  /** Optimizes the access to the source by periodically gathering
    * items emitted into batches of the specified size and emitting
    * [[monix.tail.Iterant.NextBatch NextBatch]] nodes.
    *
    * For this operation we have this law:
    *
    * `source.batched(16) <-> source`
    *
    * This means that the result will emit exactly what the source
    * emits, however the underlying representation will be different,
    * the emitted notes being of type `NextBatch`, wrapping arrays
    * with the length equal to the given `count`.
    *
    * Very similar in behavior with [[bufferTumbling]], however the
    * batches are implicit, not explicit. Useful for optimization.
    */
  def batched(count: Int)(implicit F: Sync[F]): Iterant[F, A] =
    IterantBuffer.batched(self, count)(F)

  /** Periodically gather items emitted by an iterant into bundles
    * and emit these bundles rather than emitting the items one at a
    * time. This version of `buffer` is emitting items once the
    * internal buffer has reached the given count.
    *
    * If the source iterant completes, then the current buffer gets
    * signaled downstream. If the source triggers an error then the
    * current buffer is being dropped and the error gets propagated
    * immediately.
    *
    * {{{
    *   import monix.eval.Coeval
    *
    *   // Yields Seq(1, 2, 3), Seq(4, 5, 6), Seq(7)
    *   Iterant[Coeval].of(1, 2, 3, 4, 5, 6, 7).bufferTumbling(3)
    * }}}
    *
    * @see [[bufferSliding]] for the more flexible version that allows
    *      to specify a `skip` argument.
    *
    * @param count the maximum size of each buffer before it should
    *        be emitted
    */
  def bufferTumbling(count: Int)(implicit F: Sync[F]): Iterant[F, Seq[A]] =
    bufferSliding(count, count)

  /** Returns an iterant that emits buffers of items it collects from
    * the source iterant. The resulting iterant emits buffers
    * every `skip` items, each containing `count` items.
    *
    * If the source iterant completes, then the current buffer gets
    * signaled downstream. If the source triggers an error then the
    * current buffer is being dropped and the error gets propagated
    * immediately.
    *
    * For `count` and `skip` there are 3 possibilities:
    *
    *  1. in case `skip == count`, then there are no items dropped and
    *     no overlap, the call being equivalent to `buffer(count)`
    *  1. in case `skip < count`, then overlap between buffers
    *     happens, with the number of elements being repeated being
    *     `count - skip`
    *  1. in case `skip > count`, then `skip - count` elements start
    *     getting dropped between windows
    *
    * Example:
    *
    * {{{
    *   import monix.eval.Coeval
    *
    *   val source = Iterant[Coeval].of(1, 2, 3, 4, 5, 6, 7)
    *
    *   // Yields Seq(1, 2, 3), Seq(4, 5, 6), Seq(7)
    *   source.bufferSliding(3, 3)
    *
    *   // Yields Seq(1, 2, 3), Seq(5, 6, 7)
    *   source.bufferSliding(3, 4)
    *
    *   // Yields Seq(1, 2, 3), Seq(3, 4, 5), Seq(5, 6, 7)
    *   source.bufferSliding(3, 2)
    * }}}
    *
    * @param count the maximum size of each buffer before it should
    *        be emitted
    *
    * @param skip how many items emitted by the source iterant should
    *        be skipped before starting a new buffer. Note that when
    *        skip and count are equal, this is the same operation as
    *        `bufferTumbling(count)`
    */
  final def bufferSliding(count: Int, skip: Int)(implicit F: Sync[F]): Iterant[F, Seq[A]] =
    IterantBuffer.sliding(self, count, skip)

  /** Builds a new iterant by applying a partial function to all
    * elements of the source on which the function is defined.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 2, 4, 6
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6)
    *     .map { x => Option(x).filter(_ % 2 == 0) }
    *     .collect { case Some(x) => x }
    * }}}
    *
    * @param pf the partial function that filters and maps the iterant
    * @tparam B the element type of the returned iterant.
    *
    * @return a new iterant resulting from applying the partial
    *         function `pf` to each element on which it is defined and
    *         collecting the results. The order of the elements is
    *         preserved.
    */
  final def collect[B](pf: PartialFunction[A, B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantCollect(this, pf)(F)

  /** Alias for [[concat]]. */
  final def concat[B](implicit ev: A <:< Iterant[F, B], F: Sync[F]): Iterant[F, B] =
    flatten(ev, F)

  /** Given an `Iterant` that generates `Iterant` elements, concatenates
    * all the generated iterants.
    *
    * Equivalent with: `source.flatMap(x => x)`
    */
  final def flatten[B](implicit ev: A <:< Iterant[F, B], F: Sync[F]): Iterant[F, B] =
    flatMap(x => x)(F)

  /** Alias for [[flatMap]]. */
  final def concatMap[B](f: A => Iterant[F, B])(implicit F: Sync[F]): Iterant[F, B] =
    flatMap(f)

  /** Applies the function to the elements of the source and
    * concatenates the results.
    *
    * This operation is the monadic "bind", with all laws it entails.
    *
    * Also note that the implementation can use constant memory
    * depending on usage, thus it can be used in tail recursive loops.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Effectively equivalent with .filter
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).flatMap { elem =>
    *     if (elem % 2 == 0)
    *       Iterant[Task].pure(elem)
    *     else
    *       Iterant[Task].empty[Int]
    *   }
    * }}}
    *
    * @param f is the function mapping elements from the
    *        source to iterants
    */
  final def flatMap[B](f: A => Iterant[F, B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantConcat.flatMap(this, f)(F)

  /** Counts the total number of elements emitted by the source.
    *
    * Example:
    *
    * {{{
    *   import cats.effect.IO
    *
    *   // Yields 100
    *   Iterant[IO].range(0, 100).countL
    *
    *   // Yields 1
    *   Iterant[IO].pure(1).countL
    *
    *   // Yields 0
    *   Iterant[IO].empty[Int].countL
    * }}}
    */
  final def countL(implicit F: Sync[F]): F[Long] =
    foldLeftL(0L)((c, _) => c + 1)

  /** Suppress duplicate consecutive items emitted by the source.
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Coeval
    *
    *   // Yields 1, 2, 1, 3, 2, 4
    *   Iterant[Coeval].of(1, 1, 1, 2, 2, 1, 1, 3, 3, 3, 2, 2, 4, 4, 4)
    *     .distinctUntilChanged
    * }}}
    *
    * Duplication is detected by using the equality relationship
    * provided by the `cats.Eq` type class. This allows one to
    * override the equality operation being used (e.g. maybe the
    * default `.equals` is badly defined, or maybe you want reference
    * equality, so depending on use case).
    *
    * $catsEqInterop
    *
    * @param A is the `cats.Eq` instance that defines equality for `A`
    */
  final def distinctUntilChanged(implicit F: Sync[F], A: Eq[A]): Iterant[F, A] =
    distinctUntilChangedByKey(identity)(F, A)

  /** Given a function that returns a key for each element emitted by
    * the source, suppress consecutive duplicate items.
    *
    * Example:
    *
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Coeval
    *
    *   // Yields 1, 2, 3, 4
    *   Iterant[Coeval].of(1, 3, 2, 4, 2, 3, 5, 7, 4)
    *     .distinctUntilChangedByKey(_ % 2)
    * }}}
    *
    * Duplication is detected by using the equality relationship
    * provided by the `cats.Eq` type class. This allows one to
    * override the equality operation being used (e.g. maybe the
    * default `.equals` is badly defined, or maybe you want reference
    * equality, so depending on use case).
    *
    * $catsEqInterop
    *
    * @param key is a function that returns a `K` key for each element,
    *        a value that's then used to do the deduplication
    *
    * @param K is the `cats.Eq` instance that defines equality for
    *        the key type `K`
    */
  final def distinctUntilChangedByKey[K](key: A => K)(implicit F: Sync[F], K: Eq[K]): Iterant[F, A] =
    IterantDistinctUntilChanged(self, key)(F, K)

  /** Given a routine make sure to execute it whenever the current
    * stream reaches the end, successfully, in error, or canceled.
    *
    * Implements `cats.effect.Bracket.guarantee`.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   def iterant: Iterant[Task, Int] =
    *     Iterant.delay(???)
    *
    *   iterant.guarantee(Task.eval {
    *     println("Releasing resources!")
    *   })
    * }}}
    *
    * @param f is the function to execute on early stop
    */
  final def guarantee(f: F[Unit])(implicit F: Sync[F]): Iterant[F, A] =
    guaranteeCase(_ => f)

  /** Returns a new iterant in which `f` is scheduled to be executed
    * on [[Iterant.Halt halt]] or if canceled.
    *
    * Implements `cats.effect.Bracket.guaranteeCase`.
    *
    * This would typically be used to ensure that a finalizer
    * will run at the end of the stream.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import cats.effect.ExitCase
    *
    *   def iterant: Iterant[Task, Int] =
    *     Iterant.delay(???)
    *
    *   iterant.guaranteeCase(err => Task.eval {
    *     err match {
    *       case ExitCase.Completed =>
    *         println("Completed successfully!")
    *       case ExitCase.Error(e) =>
    *         e.printStackTrace()
    *       case ExitCase.Canceled =>
    *         println("Was stopped early!")
    *     }
    *   })
    * }}}
    *
    * @param f is the finalizer to execute when streaming is
    *        terminated, by successful completion, error or
    *        cancellation
    */
  final def guaranteeCase(f: ExitCase[Throwable] => F[Unit])(implicit F: Applicative[F]): Iterant[F, A] =
    Scope[F, Unit, A](F.unit, _ => F.pure(this), (_, e) => f(e))

  /** Drops the first `n` elements (from the start).
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 4, 5
    *   Iterant[Task].of(1, 2, 3, 4, 5).drop(3)
    * }}}
    *
    * @param n the number of elements to drop
    * @return a new iterant that drops the first ''n'' elements
    *         emitted by the source
    */
  final def drop(n: Int)(implicit F: Sync[F]): Iterant[F, A] =
    IterantDrop(self, n)(F)

  /** Drops the last `n` elements (from the end).
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2
    *   Iterant[Task].of(1, 2, 3, 4, 5).dropLast(3)
    * }}}
    *
    * @param n the number of elements to drop
    * @return a new iterant that drops the last ''n'' elements
    *         emitted by the source
    */
  final def dropLast(n: Int)(implicit F: Sync[F]): Iterant[F, A] =
    IterantDropLast(self, n)(F)

  /** Drops the longest prefix of elements that satisfy the given
    * predicate and returns a new iterant that emits the rest.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 4, 5
    *   Iterant[Task].of(1, 2, 3, 4, 5).dropWhile(_ < 4)
    * }}}
    *
    * @param p is the predicate used to test whether the current
    *        element should be dropped, if `true`, or to interrupt
    *        the dropping process, if `false`
    *
    * @return a new iterant that drops the elements of the source
    *         until the first time the given predicate returns `false`
    */
  final def dropWhile(p: A => Boolean)(implicit F: Sync[F]): Iterant[F, A] =
    IterantDropWhile(self, p)

  /** Drops the longest prefix of elements that satisfy the given
    * function and returns a new Iterant that emits the rest.
    *
    * In comparison with [[dropWhile]], this version accepts a function
    * that takes an additional parameter: the zero-based index of the
    * element.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 3, 4, 5
    *   Iterant[Task].of(1, 2, 3, 4, 5)
    *     .dropWhileWithIndex((value, index) => value >= index * 2)
    * }}}
    *
    * @param p is the predicate used to test whether the current
    *        element should be dropped, if `true`, or to interrupt
    *        the dropping process, if `false`
    *
    * @return a new iterant that drops the elements of the source
    *         until the first time the given predicate returns `false`
    */
  final def dropWhileWithIndex(p: (A, Int) => Boolean)(implicit F: Sync[F]): Iterant[F, A] =
    IterantDropWhileWithIndex(self, p)

  /** Dumps incoming events to standard output with provided prefix.
    *
    * Utility that can be used for debugging purposes.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import monix.execution.Scheduler.Implicits.global
    *
    *   Iterant[Task].range(0, 4)
    *     .dump("O")
    *     .completedL
    *
    *   // 0: O --> next-batch --> 0
    *   // 1: O --> next-batch --> 1
    *   // 2: O --> next-batch --> 2
    *   // 3: O --> next-batch --> 3
    *   // 4: O --> halt --> no error
    * }}}
    */
  final def dump(prefix: String, out: PrintStream = System.out)(implicit F: Sync[F]): Iterant[F, A] =
    IterantDump(this, prefix, out)

  /** Returns `true` in case the given predicate is satisfied by any
    * of the emitted items, or `false` in case the end of the stream
    * has been reached with no items satisfying the given predicate.
    *
    * Example: {{{
    *   import monix.eval.Coeval
    *
    *   val source = Iterant[Coeval].of(1, 2, 3, 4)
    *
    *   // Yields true
    *   source.existsL(_ % 2 == 0)
    *
    *   // Yields false
    *   source.existsL(_ % 7 == 0)
    * }}}
    *
    * @param p is a predicate function that's going to test each item
    *        emitted by the source until we get a positive match for
    *        one of them or until the stream ends
    *
    * @return `true` if any of the items satisfies the given predicate
    *        or `false` if none of them do
    */
  final def existsL(p: A => Boolean)(implicit F: Sync[F]): F[Boolean] = {
    val next = Left(false)
    foldWhileLeftL(false)((_, e) => if (p(e)) Right(true) else next)
  }

  /** Left associative fold using the function `op` that can be
    * short-circuited.
    *
    * On execution the stream will be traversed from left to right,
    * and the given function will be called with the prior result,
    * accumulating state either until the end, or until `op` returns
    * a `Right` result, when the summary is returned.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Sums first 10 items
    *   Iterant[Task].range(0, 1000).foldWhileLeftL((0, 0)) {
    *     case ((sum, count), e) =>
    *       val next = (sum + e, count + 1)
    *       if (count + 1 < 10) Left(next) else Right(next)
    *   }
    *
    *   // Implements exists(predicate)
    *   Iterant[Task].of(1, 2, 3, 4, 5).foldWhileLeftL(false) {
    *     (default, e) =>
    *       if (e == 3) Right(true) else Left(default)
    *   }
    *
    *   // Implements forall(predicate)
    *   Iterant[Task].of(1, 2, 3, 4, 5).foldWhileLeftL(true) {
    *     (default, e) =>
    *       if (e != 3) Right(false) else Left(default)
    *   }
    * }}}
    *
    * @see [[Iterant.foldWhileLeftL]] for the lazy, potentially
    *      asynchronous version.
    *
    * @param seed is the start value
    * @param op is the binary operator returning either `Left`,
    *        signaling that the state should be evolved or a `Right`,
    *        signaling that the process can be short-circuited and
    *        the result returned immediately
    *
    * @return the result of inserting `op` between consecutive
    *         elements of this iterant, going from left to right with
    *         the `seed` as the start value, or `seed` if the iterant
    *         is empty
    */
  final def foldWhileLeftL[S](seed: => S)(op: (S, A) => Either[S, S])(implicit F: Sync[F]): F[S] =
    IterantFoldWhileLeftL.strict(self, seed, op)

  /** Filters the iterant by the given predicate function, returning
    * only those elements that match.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 2, 4, 6
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).filter(_ % 2 == 0)
    * }}}
    *
    * @param p the predicate used to test elements.
    *
    * @return a new iterant consisting of all elements that satisfy
    *         the given predicate. The order of the elements is
    *         preserved.
    */
  final def filter(p: A => Boolean)(implicit F: Sync[F]): Iterant[F, A] =
    IterantFilter(this, p)(F)

  /** Alias to [[filter]] to support syntax in for comprehension, i.e.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   case class Person(age: Int)
    *
    *   val peopleSource: Iterant[Task, Person] =
    *     Iterant.range[Task](1, 100).map(Person.apply)
    *
    *   for {
    *     adult <- peopleSource if adult.age >= 18
    *   } yield adult
    * }}}
    */
  final def withFilter(p: A => Boolean)(implicit F: Sync[F]): Iterant[F, A] =
    filter(p)

  /** Returns `true` in case the given predicate is satisfied by all
    * of the emitted items, or `false` in case the given predicate
    * fails for any of those items.
    *
    * Example: {{{
    *   import monix.eval.Coeval
    *
    *   val source = Iterant[Coeval].of(1, 2, 3, 4)
    *
    *   // Yields false
    *   source.forallL(_ % 2 == 0)
    *
    *   // Yields true
    *   source.existsL(_ < 10)
    * }}}
    *
    * @param p is a predicate function that's going to test each item
    *        emitted by the source until we get a negative match for
    *        one of them or until the stream ends
    *
    * @return `true` if all of the items satisfy the given predicate
    *        or `false` if any of them don't
    */
  final def forallL(p: A => Boolean)(implicit F: Sync[F]): F[Boolean] = {
    val next = Left(true)
    foldWhileLeftL(true)((_, e) => if (!p(e)) Right(false) else next)
  }

  /** Consumes the source iterable, executing the given callback for
    * each element.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Prints all elements, each one on a different line
    *   Iterant[Task].of(1, 2, 3).foreach { elem =>
    *     println("Elem: " + elem.toString)
    *   }
    * }}}
    *
    * @param cb is the callback to call for each element emitted
    *        by the source.
    */
  final def foreach(cb: A => Unit)(implicit F: Sync[F]): F[Unit] =
    map(cb)(F).completedL

  /** Upon evaluation of the result, consumes this iterant to
    * completion.
    *
    * Example: {{{
    *   import cats.implicits._
    *   import monix.eval.Task
    *
    *   // Whatever...
    *   val iterant = Iterant[Task].range(0, 10000)
    *
    *   val onFinish: Task[Unit] =
    *     iterant.completedL >> Task.eval(println("Done!"))
    * }}}
    */
  final def completedL(implicit F: Sync[F]): F[Unit] =
    IterantCompleteL(this)(F)

  /** Returns a new stream by mapping the supplied function over the
    * elements of the source.
    *
    * {{{
    *   import monix.eval.Task
    *
    *   // Yields 2, 4, 6
    *   Iterant[Task].of(1, 2, 3).map(_ * 2)
    * }}}
    *
    * @param f is the mapping function that transforms the source
    *
    * @return a new iterant that's the result of mapping the given
    *         function over the source
    */
  final def map[B](f: A => B)(implicit F: Sync[F]): Iterant[F, B] =
    IterantMap(this, f)(F)

  /** Returns a new stream by mapping the supplied function over the
    * elements of the source yielding `Iterant` consisting of `NextBatch` nodes.
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.tail.batches.Batch
    *
    *   // Yields 1, 2, 3, 4, 5
    *   Iterant[Task].of(List(1, 2, 3), List(4), List(5)).mapBatch(Batch.fromSeq(_))
    *   // Yields 2, 4, 6
    *   Iterant[Task].of(1, 2, 3).mapBatch(x => Batch(x * 2))
    * }}}
    *
    * @param f is the mapping function that transforms the source into batches.
    *
    * @return a new iterant that's the result of mapping the given
    *         function over the source
    */
  final def mapBatch[B](f: A => Batch[B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantMapBatch(this, f)(F)

  /** Optionally selects the first element.
    *
    * {{{
    *   import monix.eval.Task
    *
    *   // Yields Some(1)
    *   Iterant[Task].of(1, 2, 3, 4).headOptionL
    *
    *   // Yields None
    *   Iterant[Task].empty[Int].headOptionL
    * }}}
    *
    * @return the first element of this iterant if it is nonempty, or
    *         `None` if it is empty, in the `F` context.
    */
  final def headOptionL(implicit F: Sync[F]): F[Option[A]] =
    IterantHeadOptionL(self)(F)

  /** Optionally selects the last element.
    *
    * {{{
    *   import monix.eval.Task
    *
    *   // Yields Some(4)
    *   Iterant[Task].of(1, 2, 3, 4).lastOptionL
    *
    *   // Yields None
    *   Iterant[Task].empty[Int].lastOptionL
    * }}}
    *
    * @return the last element of this iterant if it is nonempty, or
    *         `None` if it is empty, in the `F` context.
    */
  final def lastOptionL(implicit F: Sync[F]): F[Option[A]] =
    foldLeftL(Option.empty[A])((_, a) => Some(a))(F)

  /** Given a mapping function that returns a possibly lazy or
    * asynchronous result, applies it over the elements emitted by the
    * stream.
    *
    * {{{
    *   import monix.eval.Task
    *
    *   Iterant[Task].of(1, 2, 3, 4).mapEval { elem =>
    *     Task.eval {
    *       println("Received: " + elem.toString)
    *       elem * 2
    *     }
    *   }
    * }}}
    *
    * @param f is the mapping function that transforms the source
    *
    * @return a new iterant that's the result of mapping the given
    *         function over the source,
    */
  final def mapEval[B](f: A => F[B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantMapEval(this, f)(F)

  /** Given a predicate, finds the first item that satisfies it,
    * returning `Some(a)` if available, or `None` otherwise.
    *
    * {{{
    *   import monix.eval.Coeval
    *
    *   // Yields Some(2)
    *   Iterant[Coeval].of(1, 2, 3, 4).findL(_ % 2 == 0)
    *
    *   // Yields None
    *   Iterant[Coeval].of(1, 2, 3, 4).findL(_ > 10)
    * }}}
    *
    * The stream is traversed from beginning to end, the process
    * being interrupted as soon as it finds one element matching
    * the predicate, or until the stream ends.
    *
    * @param p is the function to test the elements of the source
    *
    * @return either `Some(value)` in case `value` is an element
    *         emitted by the source, found to satisfy the predicate,
    *         or `None` otherwise
    */
  def findL(p: A => Boolean)(implicit F: Sync[F]): F[Option[A]] = {
    val init = Option.empty[A]
    val next = Left(init)
    foldWhileLeftL(init) { (_, a) =>
      if (p(a)) Right(Some(a)) else next
    }
  }

  /** Given evidence that type `A` has a `cats.Monoid` implementation,
    * folds the stream with the provided monoid definition.
    *
    * For streams emitting numbers, this effectively sums them up.
    * For strings, this concatenates them.
    *
    * Example:
    *
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Task
    *
    *   // Yields 10
    *   Iterant[Task].of(1, 2, 3, 4).foldL
    *
    *   // Yields "1234"
    *   Iterant[Task].of("1", "2", "3", "4").foldL
    * }}}
    *
    * Note, in case you don't have a `Monoid` instance in scope,
    * but you feel like you should, try one of these imports:
    *
    * {{{
    *   // everything
    *   import cats.implicits._
    *   // a la carte:
    *   import cats.instances.all._
    * }}}
    *
    * @param A is the `cats.Monoid` type class instance that's needed
    *          in scope for folding the source
    *
    * @return the result of combining all elements of the source,
    *         or the defined `Monoid.empty` element in case the
    *         stream is empty
    */
  final def foldL(implicit F: Sync[F], A: Monoid[A]): F[A] =
    foldLeftL(A.empty)(A.combine)

  /** Left associative fold using the function `op`.
    *
    * On execution the stream will be traversed from left to right,
    * and the given function will be called with the prior result,
    * accumulating state until the end, when the summary is returned.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 15 (1 + 2 + 3 + 4 + 5)
    *   Iterant[Task].of(1, 2, 3, 4, 5).foldLeftL(0)(_ + _)
    * }}}
    *
    * @param seed is the start value
    * @param op is the binary operator
    *
    * @return the result of inserting `op` between consecutive
    *         elements of this iterant, going from left to right with
    *         the `seed` as the start value, or `seed` if the iterant
    *         is empty.
    */
  final def foldLeftL[S](seed: => S)(op: (S, A) => S)(implicit F: Sync[F]): F[S] =
    IterantFoldLeftL(self, seed)(op)(F)

  /** Left associative fold using the function `op` that can be
    * short-circuited.
    *
    * On execution the stream will be traversed from left to right,
    * and the given function will be called with the prior result,
    * accumulating state either until the end, or until `op` returns
    * a `Right` result, when the summary is returned.
    *
    * The results are returned in the `F[_]` functor context, meaning
    * that we can have lazy or asynchronous processing and we can
    * suspend side effects, depending on the `F` data type being used.
    *
    * Example using `cats.effect.IO`: {{{
    *   import cats.implicits._
    *   import cats.effect.IO
    *
    *   // Sums first 10 items
    *   Iterant[IO].range(0, 1000).foldWhileLeftEvalL(IO((0, 0))) {
    *     case ((sum, count), e) =>
    *       IO {
    *         val next = (sum + e, count + 1)
    *         if (count + 1 < 10) Left(next) else Right(next)
    *       }
    *   }
    *
    *   // Implements exists(predicate)
    *   Iterant[IO].of(1, 2, 3, 4, 5).foldWhileLeftEvalL(IO(false)) {
    *     (default, e) =>
    *       IO { if (e == 3) Right(true) else Left(default) }
    *   }
    *
    *   // Implements forall(predicate)
    *   Iterant[IO].of(1, 2, 3, 4, 5).foldWhileLeftEvalL(IO(true)) {
    *     (default, e) =>
    *       IO { if (e != 3) Right(false) else Left(default) }
    *   }
    * }}}
    *
    * @see [[Iterant.foldWhileLeftL]] for the strict version.
    *
    * @param seed is the start value
    * @param op is the binary operator returning either `Left`,
    *        signaling that the state should be evolved or a `Right`,
    *        signaling that the process can be short-circuited and
    *        the result returned immediately
    *
    * @return the result of inserting `op` between consecutive
    *         elements of this iterant, going from left to right with
    *         the `seed` as the start value, or `seed` if the iterant
    *         is empty
    */
  final def foldWhileLeftEvalL[S](seed: F[S])(op: (S, A) => F[Either[S, S]])(implicit F: Sync[F]): F[S] =
    IterantFoldWhileLeftL.eval(self, seed, op)

  /**
    * Lazily fold the stream to a single value from the right.
    *
    * This is the common `foldr` operation from Haskell's `Foldable`,
    * or `foldRight` from `cats.Foldable`, but with the difference that
    * `Iterant` is a lazy data type and thus it has to operate in the `F[_]`
    * context.
    *
    * Here's for example how [[existsL]], [[forallL]] and `++` could
    * be expressed in terms of `foldRightL`:
    *
    * {{{
    *   import cats.implicits._
    *   import cats.effect.Sync
    *
    *   def exists[F[_], A](fa: Iterant[F, A], p: A => Boolean)
    *     (implicit F: Sync[F]): F[Boolean] = {
    *
    *     fa.foldRightL(F.pure(false)) { (a, next) =>
    *       if (p(a)) F.pure(true) else next
    *     }
    *   }
    *
    *   def forall[F[_], A](fa: Iterant[F, A], p: A => Boolean)
    *     (implicit F: Sync[F]): F[Boolean] = {
    *
    *     fa.foldRightL(F.pure(true)) { (a, next) =>
    *       if (!p(a)) F.pure(false) else next
    *     }
    *   }
    *
    *   def concat[F[_], A](lh: Iterant[F, A], rh: Iterant[F, A])
    *     (implicit F: Sync[F]): Iterant[F, A] = {
    *
    *     Iterant.suspend[F, A] {
    *       lh.foldRightL(F.pure(rh)) { (a, rest) =>
    *         F.pure(Iterant.nextS(a, rest))
    *       }
    *     }
    *   }
    * }}}
    *
    * In this example we are short-circuiting the processing in case
    * we find the one element that we are looking for, otherwise we
    * keep traversing the stream until the end, finally returning
    * the default value in case we haven't found what we were looking
    * for.
    *
    * @see [[foldWhileLeftL]] and [[foldWhileLeftEvalL]]
    *
    * @param b is the starting value; in case `f` is a binary operator,
    *        this is typically its left-identity (zero)
    *
    * @param f is the function to be called that folds the list,
    *        receiving the current element being iterated on
    *        (first param) and the (lazy) result from recursively
    *        combining the rest of the list (second param)
    */
  final def foldRightL[B](b: F[B])(f: (A, F[B]) => F[B])(implicit F: Sync[F]): F[B] =
    IterantFoldRightL(self, b, f)(F)

  /** Creates a new stream from the source that will emit a specific `separator`
    * between every pair of elements.
    *
    * {{{
    *   import monix.eval.Coeval
    *
    *   // Yields 1, 0, 2, 0, 3
    *   Iterant[Coeval].of(1, 2, 3).intersperse(0)
    * }}}
    *
    * @param separator the separator
    */
  final def intersperse(separator: A)(implicit F: Sync[F]): Iterant[F, A] =
    IterantIntersperse(self, separator)

  /** Creates a new stream from the source that will emit the `start` element
    * followed by the upstream elements paired with the `separator`
    * and lastly the `end` element.
    *
    * {{{
    *   import monix.eval.Coeval
    *
    *   // Yields '<', 'a', '-', 'b', '>'
    *   Iterant[Coeval].of('a', 'b').intersperse('<', '-', '>')
    * }}}
    *
    * @param start the first element emitted
    * @param separator the separator
    * @param end the last element emitted
    */
  final def intersperse(start: A, separator: A, end: A)(implicit F: Sync[F]): Iterant[F, A] =
    start +: IterantIntersperse(self, separator) :+ end

  /** Given a functor transformation from `F` to `G`, lifts the source
    * into an iterant that is going to use the resulting `G` for
    * evaluation.
    *
    * This can be used for replacing the underlying `F` type into
    * something else. For example say we have an iterant that uses
    * `monix.eval.Coeval`, but we want to convert it into
    * one that uses `monix.eval.Task` for evaluation:
    *
    * {{{
    *   import cats.~>
    *   import monix.eval._
    *
    *   // Source is using Coeval for evaluation
    *   val source = Iterant[Coeval].of(1, 2, 3, 4)
    *
    *   // Transformation to an Iterant of Task
    *   source.mapK(Coeval.liftTo[Task])
    * }}}
    *
    * This operator can be used for more than transforming the `F`
    * type into something else.
    *
    * @param f is the functor transformation that's used to transform
    *          the source into an iterant that uses `G` for evaluation
    *
    * @tparam G is the data type that is going to drive the evaluation
    *           of the resulting iterant
    */
  final def mapK[G[_]](f: F ~> G)(implicit G: Sync[G]): Iterant[G, A] =
    IterantLiftMap(self, f)(G)

  /** Takes the elements of the source iterant and emits the
    * element that has the maximum key value, where the key is
    * generated by the given function.
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Coeval
    *
    *   case class Person(name: String, age: Int)
    *
    *   // Yields Some(Person("Peter", 23))
    *   Iterant[Coeval].of(Person("Peter", 23), Person("May", 21))
    *     .maxByL(_.age)
    *
    *   // Yields None
    *   Iterant[Coeval].empty[Person].maxByL(_.age)
    * }}}
    *
    * $catsOrderInterop
    *
    * @param key is the function that returns the key for which the
    *            given ordering is defined
    *
    * @param K  is the `cats.Order` type class instance that's going
    *           to be used for comparing elements
    *
    * @return the maximum element of the source stream, relative
    *         to its key generated by the given function and the
    *         given ordering
    */
  final def maxByL[K](key: A => K)(implicit F: Sync[F], K: Order[K]): F[Option[A]] =
    reduceL((max, a) => if (K.compare(key(max), key(a)) < 0) a else max)

  /** Given a `cats.Order` over the stream's elements, returns the
    * maximum element in the stream.
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Coeval
    *
    *   // Yields Some(20)
    *   Iterant[Coeval].of(1, 10, 7, 6, 8, 20, 3, 5).maxL
    *
    *   // Yields None
    *   Iterant[Coeval].empty[Int].maxL
    * }}}
    *
    * $catsOrderInterop
    *
    * @param A is the `cats.Order` type class instance that's going
    *          to be used for comparing elements
    *
    * @return the maximum element of the source stream, relative
    *         to the defined `Order`
    */
  final def maxL(implicit F: Sync[F], A: Order[A]): F[Option[A]] =
    reduceL((max, a) => if (A.compare(max, a) < 0) a else max)

  /** Takes the elements of the source iterant and emits the
    * element that has the minimum key value, where the key is
    * generated by the given function.
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Coeval
    *
    *   case class Person(name: String, age: Int)
    *
    *   // Yields Some(Person("May", 21))
    *   Iterant[Coeval].of(Person("Peter", 23), Person("May", 21))
    *     .minByL(_.age)
    *
    *   // Yields None
    *   Iterant[Coeval].empty[Person].minByL(_.age)
    * }}}
    *
    * $catsOrderInterop
    *
    * @param key is the function that returns the key for which the
    *            given ordering is defined
    *
    * @param K  is the `cats.Order` type class instance that's going
    *           to be used for comparing elements
    *
    * @return the minimum element of the source stream, relative
    *         to its key generated by the given function and the
    *         given ordering
    */
  final def minByL[K](key: A => K)(implicit F: Sync[F], K: Order[K]): F[Option[A]] =
    reduceL((max, a) => if (K.compare(key(max), key(a)) > 0) a else max)

  /** Given a `cats.Order` over the stream's elements, returns the
    * minimum element in the stream.
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Coeval
    *
    *   // Yields Some(3)
    *   Iterant[Coeval].of(10, 7, 6, 8, 20, 3, 5).minL
    *
    *   // Yields None
    *   Iterant[Coeval].empty[Int].minL
    * }}}
    *
    * $catsOrderInterop
    *
    * @param A is the `cats.Order` type class instance that's going
    *          to be used for comparing elements
    *
    * @return the minimum element of the source stream, relative
    *         to the defined `Order`
    */
  final def minL(implicit F: Sync[F], A: Order[A]): F[Option[A]] =
    reduceL((max, a) => if (A.compare(max, a) > 0) a else max)

  /** In case this Iterant is empty, switch to the given backup. */
  final def switchIfEmpty(backup: Iterant[F, A])(implicit F: Sync[F]): Iterant[F, A] =
    IterantSwitchIfEmpty(this, backup)

  /** Reduces the elements of the source using the specified
    * associative binary operator, going from left to right, start to
    * finish.
    *
    * Example:
    *
    * {{{
    *   import monix.eval.Coeval
    *
    *   // Yields Some(10)
    *   Iterant[Coeval].of(1, 2, 3, 4).reduceL(_ + _)
    *
    *   // Yields None
    *   Iterant[Coeval].empty[Int].reduceL(_ + _)
    * }}}
    *
    * @param op is an associative binary operation that's going
    *           to be used to reduce the source to a single value
    *
    * @return either `Some(value)` in case the stream is not empty,
    *         `value` being the result of inserting `op` between
    *         consecutive elements of this iterant, going from left
    *         to right, or `None` in case the stream is empty
    */
  final def reduceL(op: (A, A) => A)(implicit F: Sync[F]): F[Option[A]] =
    IterantReduce(self, op)

  /** Repeats the items emitted by the source continuously
    *
    * It terminates either on error or if the source is empty.
    *
    * In case repetition on empty streams is desired, then combine with
    * [[retryIfEmpty]]:
    *
    * {{{
    *   import monix.eval.Coeval
    *   import scala.util.Random
    *
    *   val stream = Iterant[Coeval].suspend(Coeval {
    *     val nr = Random.nextInt()
    *     if (nr % 10 != 0)
    *       Iterant[Coeval].empty[Int]
    *     else
    *       Iterant[Coeval].of(1, 2, 3)
    *   })
    *
    *   // Will eventually repeat elements 1, 2, 3
    *   stream.retryIfEmpty(None).repeat
    * }}}
    */
  final def repeat(implicit F: Sync[F]): Iterant[F, A] =
    IterantRepeat(self)

  /**
    * Retries processing the source stream after the search is
    * detected as being empty.
    *
    * {{{
    *   import monix.eval.Coeval
    *   import scala.util.Random
    *
    *   val stream = Iterant[Coeval].suspend(Coeval {
    *     val nr = Random.nextInt()
    *     if (nr % 10 != 0)
    *       Iterant[Coeval].empty[Int]
    *     else
    *       Iterant[Coeval].of(1, 2, 3)
    *   })
    *
    *   // Will eventually stream elements 1, 2, 3
    *   stream.retryIfEmpty(None)
    * }}}
    *
    * @param maxRetries is an optional integer specifying a maximum
    *        number of retries before it gives up and returns an
    *        empty stream
    */
  final def retryIfEmpty(maxRetries: Option[Int])(implicit F: Sync[F]): Iterant[F, A] =
    IterantRetryIfEmpty(self, maxRetries)

  /** Returns an `Iterant` that mirrors the behavior of the source,
    * unless the source is terminated with an error, in which case
    * the streaming of events continues with the specified backup
    * sequence generated by the given partial function.
    *
    * The created `Iterant` mirrors the behavior of the source in
    * case the source does not end with an error or if the thrown
    * `Throwable` is not matched.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import monix.execution.exceptions.DummyException
    *
    *   val prefix = Iterant[Task].of(1, 2, 3, 4)
    *   val suffix = Iterant[Task].raiseError[Int](DummyException("dummy"))
    *   val fa = prefix ++ suffix
    *
    *   fa.onErrorRecoverWith {
    *     case _: DummyException =>
    *       Iterant[Task].pure(5)
    *   }
    * }}}
    *
    * See [[onErrorHandleWith]] for the version that takes a total
    * function as a parameter.
    *
    * @param pf is a function that matches errors with a
    *        backup throwable that is subscribed when the source
    *        throws an error.
    */
  final def onErrorRecoverWith[B >: A](pf: PartialFunction[Throwable, Iterant[F, B]])(
    implicit F: Sync[F]
  ): Iterant[F, B] =
    onErrorHandleWith { ex =>
      if (pf.isDefinedAt(ex)) pf(ex)
      else Iterant.raiseError[F, B](ex)
    }

  /** Returns an `Iterant` that mirrors the behavior of the source,
    * unless the source is terminated with an error, in which case
    * the streaming of events continues with the specified backup
    * sequence generated by the given function.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import monix.execution.exceptions.DummyException
    *
    *   val prefix = Iterant[Task].of(1, 2, 3, 4)
    *   val suffix = Iterant[Task].raiseError[Int](DummyException("dummy"))
    *   val fa = prefix ++ suffix
    *
    *   fa.onErrorHandleWith {
    *     case _: DummyException =>
    *       Iterant[Task].pure(5)
    *     case other =>
    *       Iterant[Task].raiseError[Int](other)
    *   }
    * }}}
    *
    * See [[onErrorRecoverWith]] for the version that takes a partial
    * function as a parameter.
    *
    * @param f is a function that matches errors with a
    *        backup throwable that is subscribed when the source
    *        throws an error.
    */
  final def onErrorHandleWith[B >: A](f: Throwable => Iterant[F, B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantOnErrorHandleWith(self.upcast, f)

  /** Returns an `Iterant` that mirrors the behavior of the source,
    * unless the source is terminated with an error, in which
    * case the streaming of events fallbacks to an iterant
    * emitting a single element generated by the backup function.
    *
    * The created `Iterant` mirrors the behavior of the source
    * in case the source does not end with an error or if the
    * thrown `Throwable` is not matched.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import monix.execution.exceptions.DummyException
    *
    *   val prefix = Iterant[Task].of(1, 2, 3, 4)
    *   val suffix = Iterant[Task].raiseError[Int](DummyException("dummy"))
    *   val fa = prefix ++ suffix
    *
    *   fa.onErrorRecover {
    *     case _: DummyException => 5
    *   }
    * }}}
    *
    * See [[onErrorHandle]] for the version that takes a
    * total function as a parameter.
    *
    * @param pf - a function that matches errors with a
    *        backup element that is emitted when the source
    *        throws an error.
    */
  final def onErrorRecover[B >: A](pf: PartialFunction[Throwable, B])(implicit F: Sync[F]): Iterant[F, B] =
    onErrorHandle { e =>
      if (pf.isDefinedAt(e)) pf(e)
      else throw e
    }

  /** Returns an `Iterant` that mirrors the behavior of the source,
    * unless the source is terminated with an error, in which
    * case the streaming of events fallbacks to an iterant
    * emitting a single element generated by the backup function.
    *
    * Example: {{{
    *   import monix.eval.Task
    *   import monix.execution.exceptions.DummyException
    *
    *   val prefix = Iterant[Task].of(1, 2, 3, 4)
    *   val suffix = Iterant[Task].raiseError[Int](DummyException("dummy"))
    *   val fa = prefix ++ suffix
    *
    *   fa.onErrorHandle { _ => 5 }
    * }}}
    *
    * See [[onErrorRecover]] for the version that takes a
    * partial function as a parameter.
    *
    * @param f is a function that matches errors with a
    *        backup element that is emitted when the source
    *        throws an error.
    */
  final def onErrorHandle[B >: A](f: Throwable => B)(implicit F: Sync[F]): Iterant[F, B] =
    onErrorHandleWith { e =>
      Iterant.pure[F, B](f(e))
    }

  /** Returns a new `Iterant` that mirrors the source, but ignores
    * any errors in case they happen.
    */
  final def onErrorIgnore(implicit F: Sync[F]): Iterant[F, A] =
    onErrorHandleWith(_ => Iterant.empty[F, A])

  /** Lazily zip two iterants together, the elements of the emitted
    * tuples being fetched in parallel.
    *
    * This is the parallel version of [[zip]], the results are
    * still ordered, but it can yield non-deterministic ordering
    * of effects when fetching the elements of an emitted tuple.
    *
    * @param rhs is the other iterant to zip the source with (the
    *        right hand side)
    */
  final def parZip[B](rhs: Iterant[F, B])(implicit F: Sync[F], P: Parallel[F]): Iterant[F, (A, B)] =
    (self.parZipMap(rhs))((a, b) => (a, b))

  /** Lazily zip two iterants together, in parallel, using the given
    * function `f` to produce output values.
    *
    * This is like [[zipMap]], except that the element pairs are
    * processed in parallel (ordered results, but non-deterministic
    * ordering of effects).
    *
    * @param rhs is the other iterant to zip the source with (the
    *        right hand side)
    *
    * @param f is the mapping function to transform the zipped
    *        `(A, B)` elements
    */
  final def parZipMap[B, C](rhs: Iterant[F, B])(f: (A, B) => C)(implicit F: Sync[F], P: Parallel[F]): Iterant[F, C] =
    IterantZipMap.par(this, rhs, f)

  /** Applies the function to the elements of the source and
    * concatenates the results.
    *
    * This variant of [[flatMap]] is not referentially transparent,
    * because it tries to apply function `f` immediately, in case the
    * `Iterant` is in a `NextCursor` or `NextBatch` state.
    *
    * To be used for optimizations, but keep in mind it's unsafe, as
    * its application isn't referentially transparent.
    *
    * @param f is the function mapping elements from the source to
    *        iterants
    */
  final def unsafeFlatMap[B](f: A => Iterant[F, B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantConcat.unsafeFlatMap(this)(f)(F)

  /** Creates a new iterant that upon evaluation will select
    * the first `n` elements from the source and then stop,
    * in the order they are emitted by the source.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).take(3)
    * }}}
    *
    * @param n is the number of elements to take from this iterant
    *
    * @return a new iterant instance that on evaluation will emit
    *         only the first `n` elements of this iterant
    */
  final def take(n: Int)(implicit F: Sync[F]): Iterant[F, A] =
    IterantTake(self, n)

  /** Creates a new iterable that only emits the last `n` elements
    * emitted by the source.
    *
    * In case the source triggers an error, then the underlying buffer
    * gets dropped and the error gets emitted immediately.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).take(3)
    * }}}
    *
    * @param n is the number of elements to take from the end of the
    *        stream.
    *
    * @return a new iterant instance that on evaluation will emit the
    *         last `n` elements of the source
    */
  final def takeLast(n: Int)(implicit F: Sync[F]): Iterant[F, A] =
    IterantTakeLast(self, n)

  /** Takes longest prefix of elements that satisfy the given predicate
    * and returns a new iterant that emits those elements.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2, 3
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).takeWhile(_ < 4)
    * }}}
    *
    * @param p is the function that tests each element, stopping
    *          the streaming on the first `false` result
    *
    * @return a new iterant instance that on evaluation will all
    *         elements of the source for as long as the given predicate
    *         returns `true`, stopping upon the first `false` result
    */
  final def takeWhile(p: A => Boolean)(implicit F: Sync[F]): Iterant[F, A] =
    IterantTakeWhile(self, p)(F)

  /** Takes longest prefix of elements zipped with their indices that satisfy the given predicate
    * and returns a new iterant that emits those elements.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 1, 2
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).takeWhileWithIndex((_, idx) => idx != 2)
    * }}}
    *
    * @param p is the function that tests each element, stopping
    *          the streaming on the first `false` result
    *
    * @return a new iterant instance that on evaluation will all
    *         elements of the source for as long as the given predicate
    *         returns `true`, stopping upon the first `false` result
    */
  final def takeWhileWithIndex(p: (A, Long) => Boolean)(implicit F: Sync[F]): Iterant[F, A] =
    IterantTakeWhileWithIndex(self, p)(F)

  /** Takes every n-th element, dropping intermediary elements
    * and returns a new iterant that emits those elements.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 2, 4, 6
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).takeEveryNth(2)
    *
    *   // Yields 1, 2, 3, 4, 5, 6
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).takeEveryNth(1)
    * }}}
    *
    * @param n is the sequence number of an element to be taken (must be > 0)
    *
    * @return a new iterant instance that on evaluation will return only every n-th
    *         element of the source
    */
  final def takeEveryNth(n: Int)(implicit F: Sync[F]): Iterant[F, A] =
    IterantTakeEveryNth(self, n)

  /** Drops the first element of the source iterant, emitting the rest.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields 2, 3, 4
    *   Iterant[Task].of(1, 2, 3, 4).tail
    * }}}
    *
    * @return a new iterant that upon evaluation will emit all
    *         elements of the source, except for the head
    */
  final def tail(implicit F: Sync[F]): Iterant[F, A] =
    IterantTail(self)(F)

  /** Lazily interleaves two iterants together, starting with the first
    * element from `self`.
    *
    * The length of the result will be the shorter of the two
    * arguments.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val lh = Iterant[Task].of(11, 12)
    *   val rh = Iterant[Task].of(21, 22, 23)
    *
    *   // Yields 11, 21, 12, 22
    *   lh.interleave(rh)
    * }}}
    *
    * @param rhs is the other iterant to interleave the source with (the
    *        right hand side)
    */
  final def interleave[B >: A](rhs: Iterant[F, B])(implicit F: Sync[F]): Iterant[F, B] =
    IterantInterleave(self.upcast[B], rhs)(F)

  /**
    * Consumes the source by pushing it to the specified channel.
    *
    * @param channel is a [[monix.catnap.ProducerF ProducerF]] value that
    *        will be used for consuming the stream
    */
  final def pushToChannel(channel: Producer[F, A])(implicit F: Sync[F]): F[Unit] =
    IterantPushToChannel(self, channel)

  /**
    * Create a [[monix.catnap.ConsumerF ConsumerF]] value that can be used to
    * consume events from the channel.
    *
    * The returned value is a
    * [[https://typelevel.org/cats-effect/datatypes/resource.html Resource]],
    * because a consumer can be unsubscribed from the channel early, with its
    * internal buffer being garbage collected and the finalizers of the source
    * being triggered.
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.tail.Iterant.Consumer
    *
    *   def sum(channel: Consumer[Task, Int], acc: Long = 0): Task[Long] =
    *     channel.pull.flatMap {
    *       case Right(a) =>
    *         sum(channel, acc + a)
    *       case Left(None) =>
    *         Task.pure(acc)
    *       case Left(Some(e)) =>
    *         Task.raiseError(e)
    *     }
    *
    *   Iterant[Task].range(0, 10000).consume.use { consumer =>
    *     sum(consumer)
    *   }
    * }}}
    *
    * @see [[consumeWithConfig]] for fine tuning the internal buffer of the
    *      created consumer
    */
  final def consume(implicit F: Concurrent[F]): Resource[F, Consumer[F, A]] =
    consumeWithConfig(ConsumerF.Config.default)(F, cs)

  /** Version of [[consume]] that allows for fine tuning the underlying
    * buffer used.
    *
    * There are two parameters that can be configured:
    *
    *  - the [[monix.execution.BufferCapacity BufferCapacity]], which can be
    *    [[monix.execution.BufferCapacity.Unbounded Unbounded]], for an
    *    unlimited internal buffer in case the consumer is definitely faster
    *    than the producer, or [[monix.execution.BufferCapacity.Bounded Bounded]]
    *    in case back-pressuring a slow consumer is desirable
    *
    *  - the [[monix.execution.ChannelType.ConsumerSide ChannelType.ConsumerSide]],
    *    which specifies if this consumer will use multiple workers in parallel
    *    or not; this is an optimization, with the safe choice being
    *    [[monix.execution.ChannelType.MultiConsumer MultiConsumer]], which
    *    specifies that multiple workers can use the created consumer in
    *    parallel, pulling data from multiple threads at the same time; whereas
    *    [[monix.execution.ChannelType.SingleConsumer SingleConsumer]] specifies
    *    that the data will be read sequentially by a single worker, not in
    *    parallel; this being a risky optimization
    *
    * @param config is the configuration object for fine tuning the behavior
    *        of the created consumer, see
    *        [[monix.catnap.ConsumerF.Config ConsumerF.Config1]]
    */
  @UnsafeProtocol
  final def consumeWithConfig(config: ConsumerF.Config)(
    implicit F: Concurrent[F]
  ): Resource[F, Consumer[F, A]] = {
    IterantConsume(self, config)(F, cs)
  }

  /**
    * Converts this `Iterant` to a [[monix.catnap.ChannelF]].
    */
  final def toChannel(implicit F: Concurrent[F]): Channel[F, A] =
    new Channel[F, A] {
      def consume: Resource[F, Consumer[F, A]] =
        self.consume
      def consumeWithConfig(config: ConsumerF.Config): Resource[F, Consumer[F, A]] =
        self.consumeWithConfig(config)
    }

  /** Converts this `Iterant` into an `org.reactivestreams.Publisher`.
    *
    * Meant for interoperability with other Reactive Streams
    * implementations. Also useful because it turns the `Iterant`
    * into another data type with a push-based communication protocol
    * with back-pressure.
    *
    * Usage sample:
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.execution.rstreams.SingleAssignSubscription
    *   import org.reactivestreams.{Publisher, Subscriber, Subscription}
    *
    *   def sum(source: Publisher[Int], requestSize: Int): Task[Long] =
    *     Task.create { (_, cb) =>
    *       val sub = SingleAssignSubscription()
    *
    *       source.subscribe(new Subscriber[Int] {
    *         private[this] var requested = 0L
    *         private[this] var sum = 0L
    *
    *         def onSubscribe(s: Subscription): Unit = {
    *           sub := s
    *           requested = requestSize
    *           s.request(requestSize)
    *         }
    *
    *         def onNext(t: Int): Unit = {
    *           sum += t
    *           if (requestSize != Long.MaxValue) requested -= 1
    *
    *           if (requested <= 0) {
    *             requested = requestSize
    *             sub.request(requestSize)
    *           }
    *         }
    *
    *         def onError(t: Throwable): Unit =
    *           cb.onError(t)
    *         def onComplete(): Unit =
    *           cb.onSuccess(sum)
    *       })
    *
    *       // Cancelable that can be used by Task
    *       sub
    *     }
    *
    *   // Needed for `Effect[Task]`
    *   import monix.execution.Scheduler.Implicits.global
    *
    *   val pub = Iterant[Task].of(1, 2, 3, 4).toReactivePublisher
    *
    *   // Yields 10
    *   sum(pub, requestSize = 128)
    * }}}
    *
    * See the [[http://www.reactive-streams.org/ Reactive Streams]]
    * for details.
    */
  final def toReactivePublisher(implicit F: Effect[F]): Publisher[A] =
    IterantToReactivePublisher(self)

  /** Applies a binary operator to a start value and all elements of
    * this `Iterant`, going left to right and returns a new
    * `Iterant` that emits on each step the result of the applied
    * function.
    *
    * Similar to [[foldLeftL]], but emits the state on each
    * step. Useful for modeling finite state machines.
    *
    * Example showing how state can be evolved and acted upon:
    * {{{
    *   import monix.eval.Task
    *
    *   sealed trait State[+A] { def count: Int }
    *   case object Init extends State[Nothing] { def count = 0 }
    *   case class Current[A](current: A, count: Int) extends State[A]
    *
    *   // Whatever...
    *   val source = Iterant[Task].range(0, 1000)
    *
    *   val scanned = source.scan(Init : State[Int]) { (acc, a) =>
    *     acc match {
    *       case Init => Current(a, 1)
    *       case Current(_, count) => Current(a, count + 1)
    *     }
    *   }
    *
    *   scanned
    *     .takeWhile(_.count < 10)
    *     .collect { case Current(a, _) => a }
    * }}}
    *
    * @see [[scan0]] for the version that emits seed element at the beginning
    *
    * @param seed is the initial state
    * @param op is the function that evolves the current state
    *
    * @return a new iterant that emits all intermediate states being
    *         resulted from applying function `op`
    */
  final def scan[S](seed: => S)(op: (S, A) => S)(implicit F: Sync[F]): Iterant[F, S] =
    IterantScan(self, seed, op)

  /** Applies a binary operator to a start value and all elements of
    * this `Iterant`, going left to right and returns a new
    * `Iterant` that emits on each step the result of the applied
    * function.
    *
    * This is a version of [[scan]] that emits seed element at the beginning,
    * similar to `scanLeft` on Scala collections.
    */
  final def scan0[S](seed: => S)(op: (S, A) => S)(implicit F: Sync[F]): Iterant[F, S] =
    suspend(F.map(F.delay(seed))(s => s +: scan(s)(op)))

  /** Applies a binary operator to a start value and all elements of
    * this `Iterant`, going left to right and returns a new
    * `Iterant` that emits on each step the result of the applied
    * function.
    *
    * Similar with [[scan]], but this can suspend and evaluate
    * side effects in the `F[_]` context, thus allowing for
    * asynchronous data processing.
    *
    * Similar to [[foldLeftL]] and [[foldWhileLeftEvalL]], but
    * emits the state on each step. Useful for modeling finite
    * state machines.
    *
    * Example showing how state can be evolved and acted upon:
    *
    * {{{
    *   import monix.eval.Task
    *
    *   sealed trait State[+A] { def count: Int }
    *   case object Init extends State[Nothing] { def count = 0 }
    *   case class Current[A](current: Option[A], count: Int)
    *     extends State[A]
    *
    *   // Dummies
    *   case class Person(id: Int, name: String, age: Int)
    *   def requestPersonDetails(id: Int): Task[Option[Person]] = Task.delay(???)
    *
    *   // Whatever
    *   val source = Iterant[Task].range(0, 1000)
    *   // Initial state
    *   val seed = Task.now(Init : State[Person])
    *
    *   val scanned = source.scanEval(seed) { (state, id) =>
    *     requestPersonDetails(id).map { a =>
    *       state match {
    *         case Init =>
    *           Current(a, 1)
    *         case Current(_, count) =>
    *           Current(a, count + 1)
    *       }
    *     }
    *   }
    *
    *   scanned
    *     .takeWhile(_.count < 10)
    *     .collect { case Current(Some(a), _) => a }
    * }}}
    *
    * @see [[scan]] for the version that does not require using `F[_]`
    *      in the provided operator
    *
    * @see [[scanEval0]] for the version that emits seed element at the
    *      beginning
    *
    * @param seed is the initial state
    * @param op is the function that evolves the current state
    *
    * @return a new iterant that emits all intermediate states being
    *         resulted from applying the given function
    */
  final def scanEval[S](seed: F[S])(op: (S, A) => F[S])(implicit F: Sync[F]): Iterant[F, S] =
    IterantScanEval(self, seed, op)

  /** Applies a binary operator to a start value and all elements of
    * this `Iterant`, going left to right and returns a new
    * `Iterant` that emits on each step the result of the applied
    * function.
    *
    * This is a version of [[scanEval]] that emits seed element at the beginning,
    * similar to `scanLeft` on Scala collections.
    */
  final def scanEval0[S](seed: F[S])(op: (S, A) => F[S])(implicit F: Sync[F]): Iterant[F, S] =
    Iterant.suspend(F.map(seed)(s => s +: self.scanEval(F.pure(s))(op)))

  /** Given a mapping function that returns a `B` type for which we have
    * a [[cats.Monoid]] instance, returns a new stream that folds the incoming
    * elements of the sources using the provided `Monoid[B].combine`, with the
    * initial seed being the `Monoid[B].empty` value, emitting the generated values
    * at each step.
    *
    * Equivalent with [[scan]] applied with the given [[cats.Monoid]], so given
    * our `f` mapping function returns a `B`, this law holds:
    *
    * `stream.scanMap(f) <-> stream.scan(Monoid[B].empty)(Monoid[B].combine)`
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import monix.eval.Task
    *
    *   // Yields 2, 6, 12, 20, 30, 42
    *   Iterant[Task].of(1, 2, 3, 4, 5, 6).scanMap(x => x * 2)
    * }}}
    *
    * @see [[scanMap0]] for the version that emits empty element at the beginning
    *
    * @param f is the mapping function applied to every incoming element of this `Iterant`
    *          before folding using `Monoid[B].combine`
    *
    * @return a new `Iterant` that emits all intermediate states being
    *         resulted from applying `Monoid[B].combine` function
    */
  final def scanMap[B](f: A => B)(implicit F: Sync[F], B: Monoid[B]): Iterant[F, B] =
    self.scan(B.empty)((acc, a) => B.combine(acc, f(a)))

  /** Given a mapping function that returns a `B` type for which we have
    * a [[cats.Monoid]] instance, returns a new stream that folds the incoming
    * elements of the sources using the provided `Monoid[B].combine`, with the
    * initial seed being the `Monoid[B].empty` value, emitting the generated values
    * at each step.
    *
    * This is a version of [[scanMap]] that emits seed element at the beginning.
    */
  final def scanMap0[B](f: A => B)(implicit F: Sync[F], B: Monoid[B]): Iterant[F, B] =
    B.empty +: self.scanMap(f)

  /** Given evidence that type `A` has a `scala.math.Numeric` implementation,
    * sums the stream of elements.
    *
    * An alternative to [[foldL]] which does not require any imports and works
    * in cases `cats.Monoid` is not defined for values (e.g. `A = Char`)
    */
  final def sumL(implicit F: Sync[F], A: Numeric[A]): F[A] =
    foldLeftL(A.zero)(A.plus)

  /** Aggregates all elements in a `List` and preserves order.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   // Yields List(1, 2, 3, 4)
    *   Iterant[Task].of(1, 2, 3, 4).toListL
    * }}}
    *
    * Note that this operation is dangerous, since if the iterant is
    * infinite then this operation is non-terminating, the process
    * probably blowing up with an out of memory error sooner or later.
    */
  final def toListL(implicit F: Sync[F]): F[List[A]] =
    IterantFoldLeftL.toListL(self)(F)

  /**
    * Pull the first element out of this Iterant and return it and the rest.
    * If the returned Option is None, the remainder is always empty.
    *
    * The value returned is wrapped in Iterant to preserve resource safety,
    * and consumption of the rest must not leak outside of use. The returned
    * Iterant always contains a single element
    *
    * {{{
    *   import cats._, cats.implicits._, cats.effect._
    *
    *    def unconsFold[F[_]: Sync, A: Monoid](iterant: Iterant[F, A]): F[A] = {
    *     def go(iterant: Iterant[F, A], acc: A): Iterant[F, A] =
    *       iterant.uncons.flatMap {
    *         case (None, _) => Iterant.pure(acc)
    *         case (Some(a), rest) => go(rest, acc |+| a)
    *       }
    *
    *     go(iterant, Monoid[A].empty).headOptionL.map(_.getOrElse(Monoid[A].empty))
    *   }
    * }}}
    *
    */
  final def uncons(implicit F: Sync[F]): Iterant[F, (Option[A], Iterant[F, A])] =
    IterantUncons(self)

  /** Lazily zip two iterants together.
    *
    * The length of the result will be the shorter of the two
    * arguments.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val lh = Iterant[Task].of(11, 12, 13, 14)
    *   val rh = Iterant[Task].of(21, 22, 23, 24, 25)
    *
    *   // Yields (11, 21), (12, 22), (13, 23), (14, 24)
    *   lh.zip(rh)
    * }}}
    *
    * @param rhs is the other iterant to zip the source with (the
    *        right hand side)
    */
  final def zip[B](rhs: Iterant[F, B])(implicit F: Sync[F]): Iterant[F, (A, B)] =
    (self.zipMap(rhs))((a, b) => (a, b))

  /** Lazily zip two iterants together, using the given function `f` to
    * produce output values.
    *
    * The length of the result will be the shorter of the two
    * arguments.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val lh = Iterant[Task].of(11, 12, 13, 14)
    *   val rh = Iterant[Task].of(21, 22, 23, 24, 25)
    *
    *   // Yields 32, 34, 36, 38
    *   lh.zipMap(rh) { (a, b) => a + b }
    * }}}
    *
    * @param rhs is the other iterant to zip the source with (the
    *        right hand side)
    *
    * @param f is the mapping function to transform the zipped
    *        `(A, B)` elements
    */
  final def zipMap[B, C](rhs: Iterant[F, B])(f: (A, B) => C)(implicit F: Sync[F]): Iterant[F, C] =
    IterantZipMap.seq(this, rhs, f)

  /** Zips the emitted elements of the source with their indices.
    *
    * The length of the result will be the same as the source.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val source = Iterant[Task].of("Sunday", "Monday", "Tuesday", "Wednesday")
    *
    *   // Yields ("Sunday", 0), ("Monday", 1), ("Tuesday", 2), ("Wednesday", 3)
    *   source.zipWithIndex
    * }}}
    */
  final def zipWithIndex(implicit F: Sync[F]): Iterant[F, (A, Long)] =
    IterantZipWithIndex(this)
}

/** Defines the standard [[Iterant]] builders.
  *
  * @define NextDesc The [[monix.tail.Iterant.Next Next]] state
  *         of the [[Iterant]] represents a `item` / `rest`
  *         cons pair, where the head `item` is a strict value.
  *
  *         Note that `item` being a strict value means that it is
  *         already known, whereas the `rest` is meant to be lazy and
  *         can have asynchronous behavior as well, depending on the `F`
  *         type used.
  *
  *         See [[monix.tail.Iterant.NextCursor NextCursor]]
  *         for a state where the head is a strict immutable list.
  *
  * @define NextCursorDesc The [[monix.tail.Iterant.NextCursor NextCursor]] state
  *         of the [[Iterant]] represents an `batch` / `rest` cons pair,
  *         where `batch` is an [[scala.collection.Iterator Iterator]]
  *         type that can generate a whole batch of elements.
  *
  *         Useful for doing buffering, or by giving it an empty iterator,
  *         useful to postpone the evaluation of the next element.
  *
  * @define NextBatchDesc The [[monix.tail.Iterant.NextBatch NextBatch]] state
  *         of the [[Iterant]] represents an `batch` / `rest` cons pair,
  *         where `batch` is an [[scala.collection.Iterable Iterable]]
  *         type that can generate a whole batch of elements.
  *
  * @define SuspendDesc The [[monix.tail.Iterant.Suspend Suspend]] state
  *         of the [[Iterant]] represents a suspended stream to be
  *         evaluated in the `F` context. It is useful to delay the
  *         evaluation of a stream by deferring to `F`.
  *
  * @define ConcatDesc The [[monix.tail.Iterant.Concat Concat]] state
  *         of the [[Iterant]] represents a state that specifies
  *         the concatenation of two streams.
  *
  * @define ScopeDesc The [[monix.tail.Iterant.Scope Scope]] state
  *         of the [[Iterant]] represents a stream that is able to
  *         specify the acquisition and release of a resource, to
  *         be used in generating stream events.
  *
  *         `Scope` is effectively the encoding of
  *         [[https://typelevel.org/cats-effect/typeclasses/bracket.html Bracket]],
  *         necessary for safe handling of resources. The `use`
  *         parameter is supposed to trigger a side effectful action
  *         that allocates resources, which are then used via `use`
  *         and released via `close`.
  *
  *         Note that this is often used in combination with
  *         [[Iterant.Suspend Suspend]] and data types like
  *         [[https://typelevel.org/cats-effect/concurrency/ref.html cats.effect.concurrent.Ref]]
  *         in order to communicate the acquired resources between
  *         `open`, `use` and `close`.
  *
  * @define LastDesc The [[monix.tail.Iterant.Last Last]] state of the
  *         [[Iterant]] represents a completion state as an alternative to
  *         [[monix.tail.Iterant.Halt Halt(None)]], describing one
  *         last element.
  *
  *         It is introduced as an optimization, being equivalent to
  *         `Next(item, F.pure(Halt(None)), F.unit)`, to avoid extra processing
  *         in the monadic `F[_]` and to short-circuit operations such as
  *         concatenation and `flatMap`.
  *
  * @define HaltDesc The [[monix.tail.Iterant.Halt Halt]] state
  *         of the [[Iterant]] represents the completion state
  *         of a stream, with an optional exception if an error
  *         happened.
  *
  *         `Halt` is received as a final state in the iteration process.
  *         This state cannot be followed by any other element and
  *         represents the end of the stream.
  *
  *         @see [[Iterant.Last]] for an alternative that signals one
  *         last item, as an optimisation
  *
  * @define builderSuspendByName Promote a non-strict value representing a
  *         stream to a stream of the same type, effectively delaying
  *         its initialisation.
  *
  * @define headParamDesc is the current element to be signaled
  *
  * @define lastParamDesc is the last element being signaled, after which
  *         the consumer can stop the iteration
  *
  * @define cursorParamDesc is an [[scala.collection.Iterator Iterator]] type
  *         that can generate elements by traversing a collection, a standard
  *         array or any `Iterator`
  *
  * @define generatorParamDesc is a [[scala.collection.Iterable Iterable]]
  *         type that can generate elements by traversing a collection,
  *         a standard array or any `Iterable`
  *
  * @define restParamDesc is the next state in the sequence that
  *         will produce the rest of the stream when evaluated
  *
  * @define stopParamDesc is a computation to be executed in case
  *         streaming is stopped prematurely, giving it a chance
  *         to do resource cleanup (e.g. close file handles)
  *
  * @define exParamDesc is an error to signal at the end of the stream,
  *        or `None` in case the stream has completed normally
  *
  * @define suspendByNameParam is the by-name parameter that will generate
  *         the stream when evaluated
  *
  * @define openParamDesc is an effect that should allocate necessary
  *         resources to be used in `use` and released in `close`
  *
  * @define useParamDesc is the stream created via this scope
  *
  * @define closeParamDesc is an effect that should deallocate
  *         acquired resources via `open` and that will be executed
  *         no matter what
  *
  * @define concatLhDesc is the left hand side of the concatenation,
  *         to be processed before the right-hand side
  *
  * @define concatRhDesc is the rest of the stream, to be processed
  *         after the left-hand side is
  *
  * @define intervalAtFixedRateDesc Creates an iterant that
  *         emits auto-incremented natural numbers (longs).
  *         at a fixed rate, as given by the specified `period`.
  *         The amount of time it takes to process an incoming
  *         value gets subtracted from provided `period`, thus
  *         created iterant tries to emit events spaced by the
  *         given time interval, regardless of how long further
  *         processing takes
  *
  * @define intervalWithFixedDelayDesc Creates an iterant that
  *         emits auto-incremented natural numbers (longs) spaced
  *         by a given time interval. Starts from 0 with no delay,
  *         after which it emits incremented numbers spaced by the
  *         `period` of time. The given `period` of time acts as a
  *         fixed delay between successive events.
  */
object Iterant extends IterantInstances {
  /**
    * Alias for [[monix.catnap.ConsumerF]], using `Option[Throwable]` as
    * the completion event, to be compatible with [[Iterant]].
    *
    * @see the docs for [[monix.catnap.ConsumerF ConsumerF]]
    */
  type Consumer[F[_], A] = monix.catnap.ConsumerF[F, Option[Throwable], A]

  /**
    * Alias for [[monix.catnap.ProducerF]], using `Option[Throwable]` as
    * the completion event, to be compatible with [[Iterant]].
    *
    * @see the docs for [[monix.catnap.ProducerF ProducerF]]
    */
  type Producer[F[_], A] = monix.catnap.ProducerF[F, Option[Throwable], A]

  /**
    * Alias for [[monix.catnap.ChannelF]], using `Option[Throwable]` as
    * the completion event, to be compatible with [[Iterant]].
    *
    * @see the docs for [[monix.catnap.ChannelF ChannelF]]
    */
  type Channel[F[_], A] = monix.catnap.ChannelF[F, Option[Throwable], A]

  /**
    * Returns an [[IterantBuilders]] instance for the specified `F`
    * monadic type that can be used to build [[Iterant]] instances.
    *
    * This is used to achieve the
    * [[https://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially-Applied Type]]
    * technique.
    *
    * Example:
    * {{{
    *   import monix.eval.Task
    *
    *   Iterant[Task].range(0, 10)
    * }}}
    */
  def apply[F[_]]: IterantBuilders.Apply[F] = new IterantBuilders.Apply[F]()

  /** Alias for [[now]]. */
  def pure[F[_], A](a: A): Iterant[F, A] =
    now[F, A](a)

  /** Creates a stream that depends on resource allocated by a
    * monadic value, ensuring the resource is released.
    *
    * Typical use-cases are working with files or network sockets
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import cats.effect.IO
    *   import java.io.PrintWriter
    *
    *   val printer =
    *     Iterant.resource {
    *       IO(new PrintWriter("./lines.txt"))
    *     } { writer =>
    *       IO(writer.close())
    *     }
    *
    *   // Safely use the resource, because the release is
    *   // scheduled to happen afterwards
    *   val writeLines = printer.flatMap { writer =>
    *     Iterant[IO]
    *       .fromIterator(Iterator.from(1))
    *       .mapEval(i => IO { writer.println(s"Line #$$i") })
    *   }
    *
    *   // Write 100 numbered lines to the file
    *   // closing the writer when finished
    *   writeLines.take(100).completedL
    * }}}
    *
    * @param acquire resource to acquire at the start of the stream
    * @param release function that releases the acquired resource
    */
  def resource[F[_], A](acquire: F[A])(release: A => F[Unit])(implicit F: Sync[F]): Iterant[F, A] = {

    resourceCase(acquire)((a, _) => release(a))
  }

  /** Creates a stream that depends on resource allocated by a
    * monadic value, ensuring the resource is released.
    *
    * Typical use-cases are working with files or network sockets
    *
    * Example:
    * {{{
    *   import cats.effect._
    *   import java.io.PrintWriter
    *
    *   val printer =
    *     Iterant.resource {
    *       IO(new PrintWriter("./lines.txt"))
    *     } { writer =>
    *       IO(writer.close())
    *     }
    *
    *   // Safely use the resource, because the release is
    *   // scheduled to happen afterwards
    *   val writeLines = printer.flatMap { writer =>
    *     Iterant[IO]
    *       .fromIterator(Iterator.from(1))
    *       .mapEval(i => IO { writer.println(s"Line #$$i") })
    *   }
    *
    *   // Write 100 numbered lines to the file
    *   // closing the writer when finished
    *   writeLines.take(100).completedL
    * }}}
    *
    * @param acquire an effect that acquires an expensive resource
    * @param release function that releases the acquired resource
    */
  def resourceCase[F[_], A](acquire: F[A])(release: (A, ExitCase[Throwable]) => F[Unit])(
    implicit F: Sync[F]
  ): Iterant[F, A] = {

    Scope[F, A, A](acquire, a => F.pure(Iterant.pure(a)), release)
  }

  /** Lifts a strict value into the stream context, returning a
    * stream of one element.
    */
  def now[F[_], A](a: A): Iterant[F, A] =
    lastS(a)

  /** Builds a stream state equivalent with [[Iterant.Last]].
    *
    * $LastDesc
    *
    * @param item $lastParamDesc
    */
  def lastS[F[_], A](item: A): Iterant[F, A] =
    Last(item)

  /** Lifts a non-strict value into the stream context, returning a
    * stream of one element that is lazily evaluated.
    */
  def eval[F[_], A](a: => A)(implicit F: Sync[F]): Iterant[F, A] =
    Suspend(F.delay(lastS[F, A](a)))

  /** Alias for [[eval]]. */
  def delay[F[_], A](a: => A)(implicit F: Sync[F]): Iterant[F, A] =
    eval(a)(F)

  /** Lifts a value from monadic context into the stream context,
    * returning a stream of one element
    */
  def liftF[F[_], A](fa: F[A])(implicit F: Functor[F]): Iterant[F, A] =
    Suspend(F.map(fa)(lastS))

  /** Alias for [[Iterant.suspend[F[_],A](fa* suspend]].
    *
    * $builderSuspendByName
    *
    * @param fa $suspendByNameParam
    */
  def defer[F[_], A](fa: => Iterant[F, A])(implicit F: Sync[F]): Iterant[F, A] =
    suspend(fa)

  /** $builderSuspendByName
    *
    * @param fa $suspendByNameParam
    */
  def suspend[F[_], A](fa: => Iterant[F, A])(implicit F: Sync[F]): Iterant[F, A] =
    suspend[F, A](F.delay(fa))

  /** Defers the stream generation to the underlying evaluation
    * context (e.g. `Task`, `Coeval`, `IO`, etc), building a reference
    * equivalent with [[Iterant.Suspend]].
    *
    * $SuspendDesc
    *
    * @param rest $restParamDesc
    */
  def suspend[F[_], A](rest: F[Iterant[F, A]]): Iterant[F, A] =
    Suspend[F, A](rest)

  /** Returns an empty stream that ends with an error. */
  def raiseError[F[_], A](ex: Throwable): Iterant[F, A] =
    Halt[F, A](Some(ex))

  /** Keeps calling `f` and concatenating the resulting iterants for
    * each `scala.util.Left` event emitted by the source,
    * concatenating the resulting iterants and generating
    * events out of `scala.util.Right[B]` values.
    *
    * Based on Phil Freeman's
    * [[http://functorial.com/stack-safety-for-free/index.pdf Stack Safety for Free]].
    */
  def tailRecM[F[_], A, B](a: A)(f: A => Iterant[F, Either[A, B]])(implicit F: Sync[F]): Iterant[F, B] =
    IterantConcat.tailRecM(a)(f)

  /** Converts any standard `Array` into a stream. */
  def fromArray[F[_], A](xs: Array[A])(implicit F: Applicative[F]): Iterant[F, A] =
    NextBatch(Batch.fromArray(xs), F.pure(empty[F, A]))

  /** Converts any `scala.collection.Seq` into a stream. */
  def fromSeq[F[_], A](xs: Seq[A])(implicit F: Applicative[F]): Iterant[F, A] =
    xs match {
      case ref: LinearSeq[_] =>
        fromList[F, A](ref.asInstanceOf[LinearSeq[A]])(F)
      case ref: IndexedSeq[_] =>
        fromIndexedSeq[F, A](ref.asInstanceOf[IndexedSeq[A]])(F)
      case _ =>
        fromIterable(xs)(F)
    }

  /** Converts any Scala `collection.immutable.LinearSeq` into
    * a stream.
    */
  def fromList[F[_], A](xs: LinearSeq[A])(implicit F: Applicative[F]): Iterant[F, A] =
    NextBatch(Batch.fromSeq(xs), F.pure(empty[F, A]))

  /** Converts any Scala `collection.IndexedSeq` into a stream
    * (e.g. `Vector`).
    */
  def fromIndexedSeq[F[_], A](xs: IndexedSeq[A])(implicit F: Applicative[F]): Iterant[F, A] =
    NextBatch(Batch.fromIndexedSeq(xs), F.pure(empty[F, A]))

  /** Converts a `scala.collection.Iterable` into a stream. */
  def fromIterable[F[_], A](xs: Iterable[A])(implicit F: Applicative[F]): Iterant[F, A] = {
    val bs = if (hasDefiniteSize(xs)) recommendedBatchSize else 1
    NextBatch(Batch.fromIterable(xs, bs), F.pure(empty[F, A]))
  }

  /** Converts a `scala.collection.Iterator` into a stream. */
  def fromIterator[F[_], A](xs: Iterator[A])(implicit F: Applicative[F]): Iterant[F, A] = {
    val bs = if (hasDefiniteSize(xs)) recommendedBatchSize else 1
    NextCursor[F, A](BatchCursor.fromIterator(xs, bs), F.pure(empty))
  }

  /** Converts a [[monix.tail.batches.Batch Batch]] into a stream. */
  def fromBatch[F[_], A](xs: Batch[A])(implicit F: Applicative[F]): Iterant[F, A] =
    NextBatch(xs, F.pure(empty[F, A]))

  /** Converts a [[monix.tail.batches.BatchCursor BatchCursor]] into a stream. */
  def fromBatchCursor[F[_], A](xs: BatchCursor[A])(implicit F: Applicative[F]): Iterant[F, A] =
    NextCursor(xs, F.pure(empty[F, A]))

  /** Given an `org.reactivestreams.Publisher`, converts it into an `Iterant`.
    *
    * @see [[Iterant.toReactivePublisher]] for converting an `Iterant` to
    *      a reactive publisher.
    *
    * @param publisher is the `org.reactivestreams.Publisher` reference to
    *        wrap into an [[Iterant]]
    *
    * @param requestCount a strictly positive number, representing the size
    *        of the buffer used and the number of elements requested on each
    *        cycle when communicating demand, compliant with the
    *        reactive streams specification. If `Int.MaxValue` is given,
    *        then no back-pressuring logic will be applied (e.g. an unbounded
    *        buffer is used and the source has a license to stream as many
    *        events as it wants)
    *
    * @param eagerBuffer can activate or deactivate the "eager buffer" mode in
    *        which the buffer gets pre-filled before the `Iterant`'s consumer is
    *        ready to process it — this prevents having pauses due to
    *        back-pressuring the `Subscription.request(n)` calls
    */
  def fromReactivePublisher[F[_], A](
    publisher: Publisher[A],
    requestCount: Int = recommendedBufferChunkSize,
    eagerBuffer: Boolean = true
  )(
    implicit F: Async[F]
  ): Iterant[F, A] = {
    IterantFromReactivePublisher(publisher, requestCount, eagerBuffer)
  }

  /** Given an initial state and a generator function that produces the
    * next state and the next element in the sequence, creates an
    * `Iterant` that keeps generating `NextBatch` items produced
    * by our generator function with default `recommendedBatchSize`.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val f = (x: Int) => (x + 1, x * 2)
    *   val seed = 1
    *   val stream = Iterant.fromStateAction[Task, Int, Int](f)(seed)
    *
    *   // Yields 2, 3, 5, 9
    *   stream.take(5)
    * }}}
    *
    * @see [[fromLazyStateAction]] for version supporting `F[_]`
    *     in result of generator function and seed element
    */
  def fromStateAction[F[_], S, A](f: S => (A, S))(seed: => S)(implicit F: Sync[F]): Iterant[F, A] = {
    def loop(state: S): Iterant[F, A] = {
      var toProcess = recommendedBatchSize
      var currentState = state
      val buffer = mutable.Buffer[A]()

      while (toProcess > 0) {
        val (elem, newState) = f(currentState)
        buffer.append(elem)
        currentState = newState
        toProcess -= 1
      }
      NextBatch[F, A](Batch.fromSeq(buffer.toSeq), F.delay(loop(currentState)))
    }
    try loop(seed)
    catch { case e if NonFatal(e) => Halt(Some(e)) }
  }

  /** Given an initial state and a generator function that produces the
    * next state and the next element in the sequence in `F[_]` context, creates an
    * `Iterant` that keeps generating `Next` items produced by our generator function.
    *
    * Example: {{{
    *   import monix.eval.Task
    *
    *   val f = (x: Int) => Task((x + 1, x * 2))
    *   val seed = Task.pure(1)
    *   val stream = Iterant.fromLazyStateAction[Task, Int, Int](f)(seed)
    *
    *   // Yields 2, 3, 5, 9
    *   stream.take(5)
    * }}}
    *
    * @see [[fromStateAction]] for version without `F[_]` context which
    *     generates `NextBatch` items
    */
  def fromLazyStateAction[F[_], S, A](f: S => F[(A, S)])(seed: => F[S])(
    implicit F: Sync[F]
  ): Iterant[F, A] = {
    def loop(state: S): F[Iterant[F, A]] =
      try {
        f(state).map {
          case (elem, newState) =>
            Next(elem, F.defer(loop(newState)))
        }
      } catch {
        case e if NonFatal(e) => F.pure(Halt(Some(e)))
      }
    Suspend(F.defer(seed.flatMap(loop)))
  }

  /**
    * Transforms any [[monix.catnap.ConsumerF]] into an `Iterant` stream.
    *
    * This allows for example consuming from a
    * [[monix.catnap.ConcurrentChannel ConcurrentChannel]].
    *
    * @param consumer is the [[monix.catnap.ConsumerF]] value to transform
    *        into an `Iterant`
    *
    * @param maxBatchSize is the maximum size of the emitted
    *        [[Iterant.NextBatch]] nodes, effectively specifying how many
    *        items can be pulled from the queue and processed in batches
    */
  def fromConsumer[F[_], A](consumer: Consumer[F, A], maxBatchSize: Int = recommendedBufferChunkSize)(
    implicit F: Async[F]
  ): Iterant[F, A] = {

    IterantFromConsumer(consumer, maxBatchSize)
  }

  /**
    * Transforms any [[monix.catnap.ChannelF]] into an `Iterant` stream.
    *
    * This allows for example consuming from a
    * [[monix.catnap.ConcurrentChannel ConcurrentChannel]].
    *
    * @param channel is the [[monix.catnap.ChannelF]] value from which the
    *        created stream will consume events
    *
    * @param bufferCapacity is the capacity of the internal buffer being
    *        created; it can be either of limited capacity or unbounded
    *
    * @param maxBatchSize is the maximum size of the emitted
    *        [[Iterant.NextBatch]] nodes, effectively specifying how many
    *        items can be pulled from the queue and processed in batches
    */
  def fromChannel[F[_], A](
    channel: Channel[F, A],
    bufferCapacity: BufferCapacity = Bounded(recommendedBufferChunkSize),
    maxBatchSize: Int = recommendedBufferChunkSize
  )(implicit F: Async[F]): Iterant[F, A] = {

    val config = ConsumerF.Config(
      capacity = Some(bufferCapacity),
      consumerType = Some(SingleConsumer)
    )
    fromResource(channel.consumeWithConfig(config)).flatMap { consumer =>
      fromConsumer(consumer, maxBatchSize)
    }
  }

  /**
    * Transforms any `cats.effect.Resource` into an [[Iterant]].
    *
    * See the
    * [[https://typelevel.org/cats-effect/datatypes/resource.html documentation for Resource]].
    *
    * {{{
    *   import cats.effect.Resource
    *   import cats.effect.IO
    *   import java.io._
    *
    *   def openFileAsResource(file: File): Resource[IO, FileInputStream] =
    *     Resource.make(IO(new FileInputStream(file)))(h => IO(h.close()))
    *
    *   def openFileAsStream(file: File): Iterant[IO, FileInputStream] =
    *     Iterant[IO].fromResource(openFileAsResource(file))
    * }}}
    *
    * This example would be equivalent with usage of [[Iterant.resource]]:
    *
    * {{{
    *   def openFileAsResource2(file: File): Iterant[IO, FileInputStream] = {
    *     Iterant.resource(IO(new FileInputStream(file)))(h => IO(h.close()))
    *   }
    * }}}
    *
    * This means that `flatMap` is safe to use:
    *
    * {{{
    *   def readLines(file: File): Iterant[IO, String] =
    *     openFileAsStream(file).flatMap { in =>
    *       val buf = new BufferedReader(new InputStreamReader(in, "utf-8"))
    *       Iterant[IO].repeatEval(buf.readLine())
    *         .takeWhile(_ != null)
    *     }
    * }}}
    */
  def fromResource[F[_], A](r: Resource[F, A])(implicit F: Sync[F]): Iterant[F, A] =
    r match {
      case fa: Resource.Allocate[F, A] @unchecked =>
        Iterant
          .resourceCase(fa.resource) { (a, ec) =>
            a._2(ec)
          }
          .map(_._1)
      case fa: Resource.Bind[F, Any, A] @unchecked =>
        Iterant.suspendS(F.delay {
          Iterant.fromResource(fa.source).flatMap { a =>
            Iterant.fromResource(fa.fs(a))
          }
        })
      case fa: Resource.Suspend[F, A] @unchecked =>
        Iterant.suspendS(F.map(fa.resource)(fromResource(_)))
    }

  /**
    * Returns a [[monix.catnap.ProducerF ProducerF]] instance, along with
    * an [[Iterant]] connected to it.
    *
    * Internally a [[monix.catnap.ConcurrentChannel ConcurrentChannel]] is used,
    * the paired `Iterant` acting as a [[monix.catnap.ConsumerF ConsumerF]],
    * connecting via
    * [[monix.catnap.ConcurrentChannel.consume ConcurrentChannel.consume]].
    *
    * @param bufferCapacity is the [[monix.execution.BufferCapacity capacity]]
    *        of the internal buffer being created per evaluated `Iterant` stream
    *
    * @param maxBatchSize is the maximum size of the [[Iterant.NextBatch]]
    *        nodes being emitted; this determines the maximum number of
    *        events being processed at any one time
    *
    * @param producerType (UNSAFE) specifies if there are multiple concurrent
    *        producers that will push events on the channel, or not;
    *        [[monix.execution.ChannelType.MultiProducer MultiProducer]] is
    *        the sane, default choice; only use
    *        [[monix.execution.ChannelType.SingleProducer SingleProducer]]
    *        for optimization purposes, for when you know what you're doing
    */
  def channel[F[_], A](
    bufferCapacity: BufferCapacity = Bounded(recommendedBufferChunkSize),
    maxBatchSize: Int = recommendedBufferChunkSize,
    producerType: ChannelType.ProducerSide = MultiProducer
  )(
    implicit F: Concurrent[F]
  ): F[(Producer[F, A], Iterant[F, A])] = {

    val channelF = ConcurrentChannel[F].withConfig[Option[Throwable], A](
      producerType = producerType
    )
    F.map(channelF) { channel =>
      val p: Producer[F, A] = channel
      val c = fromChannel(channel, bufferCapacity, maxBatchSize)
      (p, c)
    }
  }

  /** Builds a stream that on evaluation will produce equally spaced
    * values in some integer interval.
    *
    * @param from the start value of the stream
    * @param until the end value of the stream (exclusive from the stream)
    * @param step the increment value of the tail (must be positive or negative)
    *
    * @return the tail producing values `from, from + step, ...` up
    *         to, but excluding `until`
    */
  def range[F[_]](from: Int, until: Int, step: Int = 1)(implicit F: Applicative[F]): Iterant[F, Int] =
    NextBatch(Batch.range(from, until, step), F.pure(empty[F, Int]))

  /** Builds a stream that repeats the items provided in argument.
    *
    * It terminates either on error or if the source is empty.
    */
  def repeat[F[_], A](elems: A*)(implicit F: Sync[F]): Iterant[F, A] = elems match {
    case Seq() => Iterant.empty
    case Seq(elem) =>
      // trick to optimize recursion, see Optimisation section at:
      // https://japgolly.blogspot.com.by/2017/12/practical-awesome-recursion-ch-02.html
      var result: Iterant[F, A] = null
      result = Next[F, A](elem, F.delay(result))
      result
    case _ =>
      var result: Iterant[F, A] = null
      result = NextBatch(Batch(elems: _*), F.delay(result))
      result
  }

  /** Builds a stream that suspends provided thunk and evaluates
    * it indefinitely on-demand.
    *
    * The stream will only terminate if evaluation throws an exception
    *
    * Referentially transparent alternative to `Iterator.continually`
    *
    * Example: infinite sequence of random numbers
    * {{{
    *   import monix.eval.Coeval
    *   import scala.util.Random
    *
    *   val randomInts = Iterant[Coeval].repeatEval(Random.nextInt())
    * }}}
    *
    */
  def repeatEval[F[_], A](thunk: => A)(implicit F: Sync[F]): Iterant[F, A] =
    repeatEvalF(F.delay(thunk))

  /**
    * Builds a stream that evaluates provided effectful values indefinitely.
    *
    * The stream will only terminate if an error is raised in F context
    */
  def repeatEvalF[F[_], A](fa: F[A])(implicit F: Sync[F]): Iterant[F, A] =
    repeat[F, Unit](()).mapEval(_ => fa)

  /** Returns an empty stream. */
  def empty[F[_], A]: Iterant[F, A] =
    emptyRef.asInstanceOf[Iterant[F, A]]

  /**
    * Returns a stream that never emits any event and never completes.
    */
  def never[F[_], A](implicit F: Async[F]): Iterant[F, A] =
    Iterant.suspendS(F.never)

  /** $intervalAtFixedRateDesc
    *
    * @param period period between 2 successive emitted values
    * @param timer is the timer implementation used to generate
    *        delays and to fetch the current time
    */
  def intervalAtFixedRate[F[_]](period: FiniteDuration)(implicit F: Async[F], timer: Temporal[F]): Iterant[F, Long] =
    IterantIntervalAtFixedRate(Duration.Zero, period)

  /** $intervalAtFixedRateDesc
    *
    * This version of the `intervalAtFixedRate` allows specifying an
    * `initialDelay` before first value is emitted
    *
    * @param initialDelay initial delay before emitting the first value
    * @param period period between 2 successive emitted values
    * @param timer is the timer implementation used to generate
    *        delays and to fetch the current time
    */
  def intervalAtFixedRate[F[_]](initialDelay: FiniteDuration, period: FiniteDuration)(
    implicit
    F: Async[F],
    timer: Temporal[F]
  ): Iterant[F, Long] =
    IterantIntervalAtFixedRate(initialDelay, period)

  /** $intervalWithFixedDelayDesc
    *
    * Without having an initial delay specified, this overload
    * will immediately emit the first item, without any delays.
    *
    * @param delay the time to wait between 2 successive events
    * @param timer is the timer implementation used to generate
    *        delays and to fetch the current time
    */
  def intervalWithFixedDelay[F[_]](delay: FiniteDuration)(implicit F: Async[F], timer: Temporal[F]): Iterant[F, Long] =
    IterantIntervalWithFixedDelay(Duration.Zero, delay)

  /** $intervalWithFixedDelayDesc
    *
    * @param initialDelay is the delay to wait before emitting the first event
    * @param delay the time to wait between 2 successive events
    * @param timer is the timer implementation used to generate
    *        delays and to fetch the current time
    */
  def intervalWithFixedDelay[F[_]](initialDelay: FiniteDuration, delay: FiniteDuration)(
    implicit
    F: Async[F],
    timer: Temporal[F]
  ): Iterant[F, Long] =
    IterantIntervalWithFixedDelay(initialDelay, delay)

  /** Concatenates list of Iterants into a single stream
    */
  def concat[F[_], A](xs: Iterant[F, A]*)(implicit F: Sync[F]): Iterant[F, A] =
    xs.foldLeft(Iterant.empty[F, A])((acc, e) => IterantConcat.concat(acc, F.pure(e))(F))

  // ------------------------------------------------------------------
  // -- Data constructors

  /** Data constructor for building a [[Iterant.Next]] value.
    *
    * $NextDesc
    *
    * @param item $headParamDesc
    * @param rest $restParamDesc
    */
  def nextS[F[_], A](item: A, rest: F[Iterant[F, A]]): Iterant[F, A] =
    Next[F, A](item, rest)

  /** Data constructor for building a [[Iterant.NextCursor]] value.
    *
    * $NextCursorDesc
    *
    * @param items $cursorParamDesc
    * @param rest $restParamDesc
    */
  def nextCursorS[F[_], A](items: BatchCursor[A], rest: F[Iterant[F, A]]): Iterant[F, A] =
    NextCursor[F, A](items, rest)

  /** Data constructor for building a [[Iterant.NextBatch]] value.
    *
    * $NextBatchDesc
    *
    * @param items $generatorParamDesc
    * @param rest $restParamDesc
    */
  def nextBatchS[F[_], A](items: Batch[A], rest: F[Iterant[F, A]]): Iterant[F, A] =
    NextBatch[F, A](items, rest)

  /** Data constructor for building a [[Iterant.Halt]] value.
    *
    * $HaltDesc
    *
    * @param e $exParamDesc
    */
  def haltS[F[_], A](e: Option[Throwable]): Iterant[F, A] =
    Halt[F, A](e)

  /** Builds a stream state equivalent with [[Iterant.NextCursor]].
    *
    * $SuspendDesc
    *
    * @param rest $restParamDesc
    */
  def suspendS[F[_], A](rest: F[Iterant[F, A]]): Iterant[F, A] =
    Suspend[F, A](rest)

  /** Builds a stream state equivalent with [[Iterant.Scope]].
    *
    * $ScopeDesc
    *
    * @param acquire  $openParamDesc
    * @param use   $useParamDesc
    * @param release $closeParamDesc
    */
  def scopeS[F[_], A, B](
    acquire: F[A],
    use: A => F[Iterant[F, B]],
    release: (A, ExitCase[Throwable]) => F[Unit]
  ): Iterant[F, B] =
    Scope(acquire, use, release)

  /** Builds a stream state equivalent with [[Iterant.Concat]].
    *
    * $ConcatDesc
    *
    * @param lh $concatLhDesc
    * @param rh $concatRhDesc
    */
  def concatS[F[_], A](lh: F[Iterant[F, A]], rh: F[Iterant[F, A]]): Iterant[F, A] =
    Concat(lh, rh)

  /** $NextDesc
    *
    * @param item $headParamDesc
    * @param rest $restParamDesc
    */
  final case class Next[F[_], A](item: A, rest: F[Iterant[F, A]]) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)
  }

  /** $LastDesc
    *
    * @param item $lastParamDesc
    */
  final case class Last[F[_], A](item: A) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)
  }

  /** $NextCursorDesc
    *
    * @param cursor $cursorParamDesc
    * @param rest $restParamDesc
    */
  final case class NextCursor[F[_], A](cursor: BatchCursor[A], rest: F[Iterant[F, A]]) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)
  }

  /** $NextBatchDesc
    *
    * @param batch $generatorParamDesc
    * @param rest $restParamDesc
    */
  final case class NextBatch[F[_], A](batch: Batch[A], rest: F[Iterant[F, A]]) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)

    def toNextCursor(): NextCursor[F, A] =
      NextCursor(batch.cursor(), rest)
  }

  /** Builds a stream state equivalent with [[Iterant.NextCursor]].
    *
    * $SuspendDesc
    *
    * @param rest $restParamDesc
    */
  final case class Suspend[F[_], A](rest: F[Iterant[F, A]]) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)
  }

  /** $HaltDesc
    *
    * @param e $exParamDesc
    */
  final case class Halt[F[_], A](e: Option[Throwable]) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)
  }

  /** $ScopeDesc
    *
    * @param acquire  $openParamDesc
    * @param use   $useParamDesc
    * @param release $closeParamDesc
    */
  final case class Scope[F[_], A, B](
    acquire: F[A],
    use: A => F[Iterant[F, B]],
    release: (A, ExitCase[Throwable]) => F[Unit]
  ) extends Iterant[F, B] {

    def accept[R](visitor: Visitor[F, B, R]): R =
      visitor.visit(this)
  }

  /** $ConcatDesc
    *
    * @param lh $concatLhDesc
    * @param rh $concatRhDesc
    */
  final case class Concat[F[_], A](lh: F[Iterant[F, A]], rh: F[Iterant[F, A]]) extends Iterant[F, A] {

    def accept[R](visitor: Visitor[F, A, R]): R =
      visitor.visit(this)
  }

  /** Implements the
    * [[https://en.wikipedia.org/wiki/Visitor_pattern Visitor Pattern]]
    * for interpreting the `Iterant` data structure.
    *
    * This can be used as an alternative to pattern matching and is
    * used in the implementation of `Iterant` for performance reasons.
    *
    * WARN: this being a class instead of a recursive function, it means
    * that it often has to keep "shared state". Keeping shared state
    * is great for performance, but breaks referential transparency,
    * so use with care.
    */
  abstract class Visitor[F[_], A, R] extends (Iterant[F, A] => R) {
    /** Processes [[Iterant.Next]]. */
    def visit(ref: Next[F, A]): R

    /** Processes [[Iterant.NextBatch]]. */
    def visit(ref: NextBatch[F, A]): R

    /** Processes [[Iterant.NextCursor]]. */
    def visit(ref: NextCursor[F, A]): R

    /** Processes [[Iterant.Suspend]]. */
    def visit(ref: Suspend[F, A]): R

    /** Processes [[Iterant.Concat]]. */
    def visit(ref: Concat[F, A]): R

    /** Processes [[Iterant.Scope]]. */
    def visit[S](ref: Scope[F, S, A]): R

    /** Processes [[Iterant.Last]]. */
    def visit(ref: Last[F, A]): R

    /** Processes [[Iterant.Halt]]. */
    def visit(ref: Halt[F, A]): R

    /** Processes unhandled errors. */
    def fail(e: Throwable): R

    def apply(fa: Iterant[F, A]): R =
      try fa.accept(this)
      catch { case e if NonFatal(e) => fail(e) }
  }

  /**
    * Extension methods for deprecated methods.
    */
  implicit class Deprecated[F[_], A](val self: Iterant[F, A]) extends IterantDeprecated.Extensions[F, A]
}

private[tail] trait IterantInstances {
  /** Provides the `cats.effect.Sync` instance for [[Iterant]]. */
  implicit def catsSyncInstances[F[_]](implicit F: Sync[F]): CatsSyncInstances[F] =
    new CatsSyncInstances[F]()

  /** Provides the `cats.effect.Sync` instance for [[Iterant]]. */
  class CatsSyncInstances[F[_]](implicit F: Sync[F])
    extends StackSafeMonad[Iterant[F, *]] with MonadError[Iterant[F, *], Throwable] with Defer[Iterant[F, *]]
    with MonoidK[Iterant[F, *]] with CoflatMap[Iterant[F, *]] with FunctorFilter[Iterant[F, *]] {

    override def pure[A](a: A): Iterant[F, A] =
      Iterant.pure(a)

    override def defer[A](fa: => Iterant[F, A]): Iterant[F, A] =
      Iterant.suspend(fa)

    override def map[A, B](fa: Iterant[F, A])(f: (A) => B): Iterant[F, B] =
      fa.map(f)(F)

    override def flatMap[A, B](fa: Iterant[F, A])(f: (A) => Iterant[F, B]): Iterant[F, B] =
      fa.flatMap(f)

    override def map2[A, B, Z](fa: Iterant[F, A], fb: Iterant[F, B])(f: (A, B) => Z): Iterant[F, Z] =
      fa.flatMap(a => fb.map(b => f(a, b))(F))

    override def ap[A, B](ff: Iterant[F, (A) => B])(fa: Iterant[F, A]): Iterant[F, B] =
      ff.flatMap(f => fa.map(a => f(a))(F))

    override def tailRecM[A, B](a: A)(f: (A) => Iterant[F, Either[A, B]]): Iterant[F, B] =
      Iterant.tailRecM(a)(f)(F)

    override def empty[A]: Iterant[F, A] =
      Iterant.empty

    override def combineK[A](x: Iterant[F, A], y: Iterant[F, A]): Iterant[F, A] =
      IterantConcat.concat(x, F.pure(y))(F)

    override def coflatMap[A, B](fa: Iterant[F, A])(f: (Iterant[F, A]) => B): Iterant[F, B] =
      Iterant.pure[F, B](f(fa))

    override def coflatten[A](fa: Iterant[F, A]): Iterant[F, Iterant[F, A]] =
      Iterant.pure(fa)

    override def raiseError[A](e: Throwable): Iterant[F, A] =
      Iterant.raiseError(e)

    override def attempt[A](fa: Iterant[F, A]): Iterant[F, Either[Throwable, A]] =
      fa.attempt

    override def handleErrorWith[A](fa: Iterant[F, A])(f: (Throwable) => Iterant[F, A]): Iterant[F, A] =
      fa.onErrorHandleWith(f)

    override def handleError[A](fa: Iterant[F, A])(f: (Throwable) => A): Iterant[F, A] =
      fa.onErrorHandle(f)

    override def recover[A](fa: Iterant[F, A])(pf: PartialFunction[Throwable, A]): Iterant[F, A] =
      fa.onErrorRecover(pf)

    override def recoverWith[A](fa: Iterant[F, A])(pf: PartialFunction[Throwable, Iterant[F, A]]): Iterant[F, A] =
      fa.onErrorRecoverWith(pf)

    override def functor: Functor[Iterant[F, *]] = this

    override def mapFilter[A, B](fa: Iterant[F, A])(f: A => Option[B]): Iterant[F, B] =
      fa.map(f).collect { case Some(b) => b }

    override def collect[A, B](fa: Iterant[F, A])(f: PartialFunction[A, B]): Iterant[F, B] =
      fa.collect(f)

    override def filter[A](fa: Iterant[F, A])(f: A => Boolean): Iterant[F, A] =
      fa.filter(f)
  }
}
