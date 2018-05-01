/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
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

package monix.eval.internal

import monix.eval.Task.{Async, Context, Error, Eval, FlatMap, FrameIndex, Map, Now, Suspend}
import monix.eval.{Callback, Task}
import monix.execution.internal.collection.ArrayStack
import monix.execution.misc.NonFatal
import monix.execution.{Cancelable, CancelableFuture, ExecutionModel, Scheduler}
import scala.concurrent.Promise


private[eval] object TaskRunLoop {
  type Current = Task[Any]
  type Bind = Any => Task[Any]
  type CallStack = ArrayStack[Bind]

  /** Starts or resumes evaluation of the run-loop from where it left
    * off. This is the complete run-loop.
    *
    * Used for continuing a run-loop after an async boundary
    * happens from [[startFuture]] and [[startLight]].
    */
  def startFull[A](
    source: Task[A],
    context: Context,
    cb: Callback[A],
    rcb: RestartCallback,
    bFirst: Bind,
    bRest: CallStack,
    frameIndex: FrameIndex): Unit = {

    val cba = cb.asInstanceOf[Callback[Any]]
    val sc = context.scheduler
    val em = sc.executionModel
    var current: Current = source
    var bFirstRef = bFirst
    var bRestRef = bRest
    // Values from Now, Always and Once are unboxed in this var, for code reuse
    var hasUnboxed: Boolean = false
    var unboxed: AnyRef = null
    var currentIndex = frameIndex

    do {
      if (currentIndex != 0) {
        current match {
          case FlatMap(fa, bindNext) =>
            if (bFirstRef ne null) {
              if (bRestRef eq null) bRestRef = new ArrayStack()
              bRestRef.push(bFirstRef)
            }
            bFirstRef = bindNext.asInstanceOf[Bind]
            current = fa

          case Now(value) =>
            unboxed = value.asInstanceOf[AnyRef]
            hasUnboxed = true

          case Eval(thunk) =>
            try {
              unboxed = thunk().asInstanceOf[AnyRef]
              hasUnboxed = true
              current = null
            } catch { case e if NonFatal(e) =>
              current = Error(e)
            }

          case bindNext @ Map(fa, _, _) =>
            if (bFirstRef ne null) {
              if (bRestRef eq null) bRestRef = new ArrayStack()
              bRestRef.push(bFirstRef)
            }
            bFirstRef = bindNext.asInstanceOf[Bind]
            current = fa

          case Suspend(thunk) =>
            // Try/catch described as statement, otherwise ObjectRef happens ;-)
            try { current = thunk() }
            catch { case ex if NonFatal(ex) => current = Error(ex) }

          case Error(error) =>
            findErrorHandler(bFirstRef, bRestRef) match {
              case null =>
                cba.onError(error)
                return
              case bind =>
                // Try/catch described as statement, otherwise ObjectRef happens ;-)
                try { current = bind.recover(error) }
                catch { case e if NonFatal(e) => current = Error(e) }
                currentIndex = em.nextFrameIndex(currentIndex)
                bFirstRef = null
            }

          case Async(register) =>
            executeAsyncTask(context, register, cba, rcb, bFirstRef, bRestRef, currentIndex)
            return
        }

        if (hasUnboxed) {
          popNextBind(bFirstRef, bRestRef) match {
            case null =>
              cba.onSuccess(unboxed)
              return
            case bind =>
              // Try/catch described as statement, otherwise ObjectRef happens ;-)
              try { current = bind(unboxed) }
              catch { case ex if NonFatal(ex) => current = Error(ex) }
              currentIndex = em.nextFrameIndex(currentIndex)
              hasUnboxed = false
              unboxed = null
              bFirstRef = null
          }
        }
      } else {
        // Force async boundary
        restartAsync(current, context, cba, rcb, bFirstRef, bRestRef)
        return
      }
    } while (true)
  }

  /** Internal utility, for forcing an asynchronous boundary in the
    * trampoline loop.
    */
  def restartAsync[A](
    source: Task[A],
    context: Context,
    cb: Callback[A],
    rcb: RestartCallback,
    bindCurrent: Bind,
    bindRest: CallStack): Unit = {

    context.scheduler.executeAsync { () =>
      // Checking for the cancellation status after the async boundary;
      // This is consistent with the behavior on `Async` tasks, i.e. check
      // is done *after* the evaluation and *before* signaling the result
      // or continuing the evaluation of the bind chain. It also makes sense
      // because it gives a chance to the caller to cancel and not wait for
      // another forced boundary to have actual cancellation happening.
      if (!context.shouldCancel) {
        // Resetting the frameRef, as a real asynchronous boundary happened
        context.frameRef.reset()
        // Using frameIndex = 1 to ensure at least one cycle gets executed
        startFull(source, context, cb, rcb, bindCurrent, bindRest, 1)
      }
    }
  }

  /** A run-loop that attempts to evaluate a `Task` without
    * initializing a `Task.Context`, falling back to
    * [[startFull]] when the first `Async` boundary is hit.
    *
    * Function gets invoked by `Task.runAsync(cb: Callback)`.
    */
  def startLight[A](
    source: Task[A],
    scheduler: Scheduler,
    opts: Task.Options,
    cb: Callback[A]): Cancelable = {

    var current = source.asInstanceOf[Task[Any]]
    var bFirst: Bind = null
    var bRest: CallStack = null
    // Values from Now, Always and Once are unboxed in this var, for code reuse
    var hasUnboxed: Boolean = false
    var unboxed: AnyRef = null
    // Keeps track of the current frame, used for forced async boundaries
    val em = scheduler.executionModel
    var frameIndex = frameStart(em)

    do {
      if (frameIndex != 0) {
        current match {
          case FlatMap(fa, bindNext) =>
            if (bFirst ne null) {
              if (bRest eq null) bRest = new ArrayStack()
              bRest.push(bFirst)
            }
            bFirst = bindNext.asInstanceOf[Bind]
            current = fa

          case Now(value) =>
            unboxed = value.asInstanceOf[AnyRef]
            hasUnboxed = true

          case Eval(thunk) =>
            try {
              unboxed = thunk().asInstanceOf[AnyRef]
              hasUnboxed = true
              current = null
            } catch { case e if NonFatal(e) =>
              current = Error(e)
            }

          case bindNext @ Map(fa, _, _) =>
            if (bFirst ne null) {
              if (bRest eq null) bRest = new ArrayStack()
              bRest.push(bFirst)
            }
            bFirst = bindNext.asInstanceOf[Bind]
            current = fa

          case Suspend(thunk) =>
            // Try/catch described as statement, otherwise ObjectRef happens ;-)
            try { current = thunk() }
            catch { case ex if NonFatal(ex) => current = Error(ex) }

          case Error(error) =>
            findErrorHandler(bFirst, bRest) match {
              case null =>
                cb.onError(error)
                return Cancelable.empty
              case bind =>
                // Try/catch described as statement, otherwise ObjectRef happens ;-)
                try { current = bind.recover(error) }
                catch { case e if NonFatal(e) => current = Error(e) }
                frameIndex = em.nextFrameIndex(frameIndex)
                bFirst = null
            }

          case Async(register) =>
            return goAsyncForLightCB(
              current,
              register,
              scheduler,
              opts,
              cb.asInstanceOf[Callback[Any]],
              bFirst, bRest, frameIndex,
              forceAsync = false)
        }

        if (hasUnboxed) {
          popNextBind(bFirst, bRest) match {
            case null =>
              cb.onSuccess(unboxed.asInstanceOf[A])
              return Cancelable.empty
            case bind =>
              // Try/catch described as statement, otherwise ObjectRef happens ;-)
              try { current = bind(unboxed) }
              catch { case ex if NonFatal(ex) => current = Error(ex) }
              frameIndex = em.nextFrameIndex(frameIndex)
              hasUnboxed = false
              unboxed = null
              bFirst = null
          }
        }
      }
      else {
        // Force async boundary
        return goAsyncForLightCB(
          current, null,
          scheduler, opts,
          cb.asInstanceOf[Callback[Any]],
          bFirst, bRest, frameIndex,
          forceAsync = true)
      }
    } while (true)
    // $COVERAGE-OFF$
    null
    // $COVERAGE-ON$
  }

  /** A run-loop that attempts to complete a `CancelableFuture`
    * synchronously falling back to [[startFull]] and actual
    * asynchronous execution in case of an asynchronous boundary.
    *
    * Function gets invoked by `Task.runAsync(implicit s: Scheduler)`.
    */
  def startFuture[A](source: Task[A], scheduler: Scheduler, opts: Task.Options): CancelableFuture[A] = {
    var current = source.asInstanceOf[Task[Any]]
    var bFirst: Bind = null
    var bRest: CallStack = null
    // Values from Now, Always and Once are unboxed in this var, for code reuse
    var hasUnboxed: Boolean = false
    var unboxed: AnyRef = null
    // Keeps track of the current frame, used for forced async boundaries
    val em = scheduler.executionModel
    var frameIndex = frameStart(em)

    do {
      if (frameIndex != 0) {
        current match {
          case FlatMap(fa, bindNext) =>
            if (bFirst ne null) {
              if (bRest eq null) bRest = new ArrayStack()
              bRest.push(bFirst)
            }
            bFirst = bindNext
            current = fa

          case Now(value) =>
            unboxed = value.asInstanceOf[AnyRef]
            hasUnboxed = true

          case Eval(thunk) =>
            try {
              unboxed = thunk().asInstanceOf[AnyRef]
              hasUnboxed = true
              current = null
            } catch {
              case e if NonFatal(e) =>
                current = Error(e)
            }

          case bindNext @ Map(fa, _, _) =>
            if (bFirst ne null) {
              if (bRest eq null) bRest = new ArrayStack()
              bRest.push(bFirst)
            }
            bFirst = bindNext
            current = fa

          case Suspend(thunk) =>
            // Try/catch described as statement to prevent ObjectRef ;-)
            try {
              current = thunk()
            }
            catch {
              case ex if NonFatal(ex) => current = Error(ex)
            }

          case Error(error) =>
            findErrorHandler(bFirst, bRest) match {
              case null =>
                return CancelableFuture.failed(error)
              case bind =>
                // Try/catch described as statement to prevent ObjectRef ;-)
                try { current = bind.recover(error) }
                catch { case e if NonFatal(e) => current = Error(e) }
                frameIndex = em.nextFrameIndex(frameIndex)
                bFirst = null
            }

          case Async(register) =>
            return goAsync4Future(
              current, register,
              scheduler, opts,
              bFirst, bRest,
              frameIndex,
              forceAsync = false)
        }

        if (hasUnboxed) {
          popNextBind(bFirst, bRest) match {
            case null =>
              return CancelableFuture.successful(unboxed.asInstanceOf[A])
            case bind =>
              // Try/catch described as statement to prevent ObjectRef ;-)
              try {
                current = bind(unboxed)
              }
              catch {
                case ex if NonFatal(ex) => current = Error(ex)
              }
              frameIndex = em.nextFrameIndex(frameIndex)
              hasUnboxed = false
              unboxed = null
              bFirst = null
          }
        }
      } else {
        // Force async boundary
        return goAsync4Future(
          current, null,
          scheduler, opts,
          bFirst, bRest,
          frameIndex,
          forceAsync = true)
      }
    } while (true)
    // $COVERAGE-OFF$
    null
    // $COVERAGE-ON$
  }

  private[internal] def executeAsyncTask(
    context: Context,
    register: (Context, Callback[Any]) => Unit,
    cb: Callback[Any],
    rcb: RestartCallback,
    bFirst: Bind,
    bRest: CallStack,
    nextFrame: FrameIndex): Unit = {

    // We are going to resume the frame index from where we left,
    // but only if no real asynchronous execution happened. So in order
    // to detect asynchronous execution, we are reading a thread-local
    // variable that's going to be reset in case of a thread jump.
    // Obviously this doesn't work for Javascript or for single-threaded
    // thread-pools, but that's OK, as it only means that in such instances
    // we can experience more async boundaries and everything is fine for
    // as long as the implementation of `Async` tasks are triggering
    // a `frameRef.reset` on async boundaries.
    context.frameRef := nextFrame

    // rcb reference might be null, so initializing
    val restartCallback = if (rcb != null) rcb else new RestartCallback(context, cb)
    restartCallback.prepare(bFirst, bRest)
    register(context, restartCallback)
  }

  /** Called when we hit the first async boundary in
    * [[startLight]].
    */
  private def goAsyncForLightCB(
    source: Current,
    register: (Context, Callback[Any]) => Unit,
    scheduler: Scheduler,
    opts: Task.Options,
    cb: Callback[Any],
    bFirst: Bind,
    bRest: CallStack,
    nextFrame: FrameIndex,
    forceAsync: Boolean): Cancelable = {

    val context = Context(scheduler, opts)
    if (register ne null)
      executeAsyncTask(context, register, cb, null, bFirst, bRest, 1)
    else if (forceAsync)
      restartAsync(source, context, cb, null, bFirst, bRest)
    else
      startFull(source, context, cb, null, bFirst, bRest, nextFrame)

    context.connection
  }

  /** Called when we hit the first async boundary in [[startFuture]]. */
  private def goAsync4Future[A](
    source: Current,
    register: (Context, Callback[Any]) => Unit,
    scheduler: Scheduler,
    opts: Task.Options,
    bFirst: Bind,
    bRest: CallStack,
    nextFrame: FrameIndex,
    forceAsync: Boolean): CancelableFuture[A] = {

    val p = Promise[A]()
    val cb = Callback.fromPromise(p)
    val fa = source.asInstanceOf[Task[A]]
    val context = Context(scheduler, opts)

    if (register ne null)
      executeAsyncTask(context, register, cb.asInstanceOf[Callback[Any]], null, bFirst, bRest, 1)
    else if (forceAsync)
      restartAsync(fa, context, cb, null, bFirst, bRest)
    else
      startFull(fa, context, cb, null, bFirst, bRest, nextFrame)

    CancelableFuture(p.future, context.connection)
  }

  private[internal] def findErrorHandler(bFirst: Bind, bRest: CallStack): StackFrame[Any, Task[Any]] = {
    bFirst match {
      case ref: StackFrame[Any, Task[Any]] @unchecked => ref
      case _ =>
        if (bRest eq null) null else {
          do {
            val ref = bRest.pop()
            if (ref eq null)
              return null
            else if (ref.isInstanceOf[StackFrame[_, _]])
              return ref.asInstanceOf[StackFrame[Any, Task[Any]]]
          } while (true)
          // $COVERAGE-OFF$
          null
          // $COVERAGE-ON$
        }
    }
  }

  private[internal] def popNextBind(bFirst: Bind, bRest: CallStack): Bind = {
    if ((bFirst ne null) && !bFirst.isInstanceOf[StackFrame.ErrorHandler[_, _]])
      return bFirst

    if (bRest eq null) return null
    do {
      val next = bRest.pop()
      if (next eq null) {
        return null
      } else if (!next.isInstanceOf[StackFrame.ErrorHandler[_, _]]) {
        return next
      }
    } while (true)
    // $COVERAGE-OFF$
    null
    // $COVERAGE-ON$
  }

  private def frameStart(em: ExecutionModel): FrameIndex =
    em.nextFrameIndex(0)

  private[internal] final class RestartCallback(context: Context, callback: Callback[Any])
    extends Callback[Any] {

    private[this] var canCall = false
    private[this] var bFirst: Bind = _
    private[this] var bRest: CallStack = _
    private[this] val runLoopIndex = context.frameRef

    def prepare(bindCurrent: Bind, bindRest: CallStack): Unit = {
      canCall = true
      this.bFirst = bindCurrent
      this.bRest = bindRest
    }

    def onSuccess(value: Any): Unit =
      if (canCall && !context.shouldCancel) {
        canCall = false
        startFull(Now(value), context, callback, this, bFirst, bRest, runLoopIndex())
      }

    def onError(ex: Throwable): Unit = {
      if (canCall && !context.shouldCancel) {
        canCall = false
        startFull(Error(ex), context, callback, this, bFirst, bRest, runLoopIndex())
      } else {
        // $COVERAGE-OFF$
        context.scheduler.reportFailure(ex)
        // $COVERAGE-ON$
      }
    }
  }
}
