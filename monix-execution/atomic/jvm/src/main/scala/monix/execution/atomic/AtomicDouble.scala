/*
 * Copyright (c) 2014-2021 by The Monix Project Developers.
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

package monix.execution.atomic

import monix.execution.atomic.PaddingStrategy.NoPadding
import scala.annotation.tailrec
import java.lang.Double.{doubleToLongBits, longBitsToDouble}
import monix.execution.atomic.internal.{BoxedLong, Factory}

/** Atomic references wrapping `Double` values.
  *
  * Note that the equality test in `compareAndSet` is value based,
  * since `Double` is a primitive.
  */
final class AtomicDouble private (val ref: BoxedLong) extends AtomicNumber[Double] {

  def get(): Double = longBitsToDouble(ref.volatileGet())
  def set(update: Double): Unit = ref.volatileSet(doubleToLongBits(update))
  def lazySet(update: Double): Unit = ref.lazySet(doubleToLongBits(update))

  def compareAndSet(expect: Double, update: Double): Boolean = {
    val expectLong = doubleToLongBits(expect)
    val updateLong = doubleToLongBits(update)
    ref.compareAndSet(expectLong, updateLong)
  }

  def getAndSet(update: Double): Double = {
    longBitsToDouble(ref.getAndSet(doubleToLongBits(update)))
  }

  @tailrec
  def increment(v: Int = 1): Unit = {
    val current = get()
    val update = incrementOp(current, v)
    if (!compareAndSet(current, update))
      increment(v)
  }

  @tailrec
  def add(v: Double): Unit = {
    val current = get()
    val update = plusOp(current, v)
    if (!compareAndSet(current, update))
      add(v)
  }

  @tailrec
  def incrementAndGet(v: Int = 1): Double = {
    val current = get()
    val update = incrementOp(current, v)
    if (!compareAndSet(current, update))
      incrementAndGet(v)
    else
      update
  }

  @tailrec
  def addAndGet(v: Double): Double = {
    val current = get()
    val update = plusOp(current, v)
    if (!compareAndSet(current, update))
      addAndGet(v)
    else
      update
  }

  @tailrec
  def getAndIncrement(v: Int = 1): Double = {
    val current = get()
    val update = incrementOp(current, v)
    if (!compareAndSet(current, update))
      getAndIncrement(v)
    else
      current
  }

  @tailrec
  def getAndAdd(v: Double): Double = {
    val current = get()
    val update = plusOp(current, v)
    if (!compareAndSet(current, update))
      getAndAdd(v)
    else
      current
  }

  @tailrec
  def subtract(v: Double): Unit = {
    val current = get()
    val update = minusOp(current, v)
    if (!compareAndSet(current, update))
      subtract(v)
  }

  @tailrec
  def subtractAndGet(v: Double): Double = {
    val current = get()
    val update = minusOp(current, v)
    if (!compareAndSet(current, update))
      subtractAndGet(v)
    else
      update
  }

  @tailrec
  def getAndSubtract(v: Double): Double = {
    val current = get()
    val update = minusOp(current, v)
    if (!compareAndSet(current, update))
      getAndSubtract(v)
    else
      current
  }

  def decrement(v: Int = 1): Unit = increment(-v)
  def decrementAndGet(v: Int = 1): Double = incrementAndGet(-v)
  def getAndDecrement(v: Int = 1): Double = getAndIncrement(-v)

  private[this] def plusOp(a: Double, b: Double): Double = a + b
  private[this] def minusOp(a: Double, b: Double): Double = a - b
  private[this] def incrementOp(a: Double, b: Int): Double = a + b
}

/** @define createDesc Constructs an [[AtomicDouble]] reference, allowing
  *         for fine-tuning of the created instance.
  *
  *         A [[PaddingStrategy]] can be provided in order to counter
  *         the "false sharing" problem.
  *
  *         Note that for ''Scala.js'' we aren't applying any padding,
  *         as it doesn't make much sense, since Javascript execution
  *         is single threaded, but this builder is provided for
  *         syntax compatibility anyway across the JVM and Javascript
  *         and we never know how Javascript engines will evolve.
  */
object AtomicDouble {
  /** Builds an [[AtomicDouble]] reference.
    *
    * @param initialValue is the initial value with which to initialize the atomic
    */
  def apply(initialValue: Double): AtomicDouble =
    withPadding(initialValue, NoPadding)

  /** $createDesc
    *
    * @param initialValue is the initial value with which to initialize the atomic
    * @param padding is the [[PaddingStrategy]] to apply
    */
  def withPadding(initialValue: Double, padding: PaddingStrategy): AtomicDouble =
    create(initialValue, padding, allowPlatformIntrinsics = true)

  /** $createDesc
    *
    * Also this builder on top Java 8 also allows for turning off the
    * Java 8 intrinsics, thus forcing usage of CAS-loops for
    * `getAndSet` and for `getAndAdd`.
    *
    * @param initialValue is the initial value with which to initialize the atomic
    * @param padding is the [[PaddingStrategy]] to apply
    * @param allowPlatformIntrinsics is a boolean parameter that specifies whether
    *        the instance is allowed to use the Java 8 optimized operations
    *        for `getAndSet` and for `getAndAdd`
    */
  def create(initialValue: Double, padding: PaddingStrategy, allowPlatformIntrinsics: Boolean): AtomicDouble = {
    new AtomicDouble(
      Factory.newBoxedLong(
        doubleToLongBits(initialValue),
        boxStrategyToPaddingStrategy(padding),
        true, // allowIntrinsics
        allowPlatformIntrinsics
      ))
  }

  /** $createDesc
    *
    * This builder guarantees to construct a safe atomic reference that
    * does not make use of `sun.misc.Unsafe`. On top of platforms that
    * don't support it, notably some versions of Android or on top of
    * the upcoming Java 9, this might be desirable.
    *
    * NOTE that explicit usage of this builder is not usually necessary
    * because [[create]] can auto-detect whether the underlying platform
    * supports `sun.misc.Unsafe` and if it does, then its usage is
    * recommended, because the "safe" atomic instances have overhead.
    *
    * @param initialValue is the initial value with which to initialize the atomic
    * @param padding is the [[PaddingStrategy]] to apply
    */
  def safe(initialValue: Double, padding: PaddingStrategy): AtomicDouble = {
    new AtomicDouble(
      Factory.newBoxedLong(
        doubleToLongBits(initialValue),
        boxStrategyToPaddingStrategy(padding),
        false, // allowUnsafe
        false // allowJava8Intrinsics
      ))
  }
}
