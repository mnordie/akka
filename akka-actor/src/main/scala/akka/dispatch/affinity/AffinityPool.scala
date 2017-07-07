/**
 *  Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.dispatch.affinity

import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType.methodType
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit.MICROSECONDS
import java.util.concurrent._
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }
import java.util.concurrent.locks.{ Lock, LockSupport, ReentrantLock }

import akka.dispatch._
import akka.util.Helpers.Requiring
import com.typesafe.config.Config

import scala.annotation.tailrec
import java.lang.Integer.reverseBytes

import akka.annotation.InternalApi
import akka.annotation.ApiMayChange
import akka.util.ImmutableIntMap
import akka.util.OptionVal

import scala.collection.mutable
import scala.util.control.NonFatal

@InternalApi
@ApiMayChange
private[affinity] object AffinityPool {
  type PoolState = Int
  // PoolState: accepts new tasks and processes tasks that are enqueued
  final val Running = 0
  // PoolState: does not accept new tasks, processes tasks that are in the queue
  final val ShuttingDown = 1
  // PoolState: does not accept new tasks, does not process tasks in queue
  final val ShutDown = 2
  // PoolState: all threads have been stopped, does not process tasks and does not accept new ones
  final val Terminated = 3

  type WorkerState = Int
  // WorkerState: Worker not started
  final val NotStarted = 0
  // WorkerState: Worker executing task
  final val InExecution = 1
  // WorkerState: Worker idling
  final val Idle = 2

  // Method handle to JDK9+ onSpinWait method
  private val onSpinWaitMethodHandle =
    try
      OptionVal.Some(MethodHandles.lookup.findStatic(classOf[Thread], "onSpinWait", methodType(classOf[Unit])))
    catch {
      case NonFatal(_) ⇒ OptionVal.None
    }

  // Following are auxiliary class and trait definitions
  private final class IdleStrategy(val idleCpuLevel: Int) {

    private[this] val maxSpins = 1100 * idleCpuLevel - 1000
    private[this] val maxYields = 5 * idleCpuLevel
    private[this] val minParkPeriodNs = 1
    private[this] val maxParkPeriodNs = MICROSECONDS.toNanos(280 - 30 * idleCpuLevel)

    private sealed trait State
    private case object NotIdle extends State
    private case object Spinning extends State
    private case object Yielding extends State
    private case object Parking extends State

    private[this] var state: State = NotIdle
    private[this] var spins = 0L
    private[this] var yields = 0L
    private[this] var parkPeriodNs = 0L

    def idle(): Unit = {
      state match {
        case NotIdle ⇒
          state = Spinning
          spins += 1
        case Spinning ⇒
          onSpinWaitMethodHandle match {
            case OptionVal.Some(m) ⇒ m.invokeExact()
            case OptionVal.None    ⇒
          }
          spins += 1
          if (spins > maxSpins) {
            state = Yielding
            yields = 0
          }
        case Yielding ⇒
          yields += 1
          if (yields > maxYields) {
            state = Parking
            parkPeriodNs = minParkPeriodNs
          } else Thread.`yield`()
        case Parking ⇒
          LockSupport.parkNanos(parkPeriodNs)
          parkPeriodNs = Math.min(parkPeriodNs << 1, maxParkPeriodNs)
      }
    }

    def reset(): Unit = {
      spins = 0
      yields = 0
      state = NotIdle
    }
  }

  private final class BoundedAffinityTaskQueue(capacity: Int) extends AbstractBoundedNodeQueue[Runnable](capacity)
}

/**
 * An [[ExecutorService]] implementation which pins actor to particular threads
 * and guaranteed that an actor's [[Mailbox]] will e run on the thread it used
 * it used to run. In situations where we see a lot of cache ping pong, this
 * might lead to significant performance improvements.
 *
 * INTERNAL API
 */
@InternalApi
@ApiMayChange
private[akka] class AffinityPool(
  parallelism:                         Int,
  affinityGroupSize:                   Int,
  tf:                                  ThreadFactory,
  idleCpuLevel:                        Int,
  final val fairDistributionThreshold: Int,
  rejectionHandler:                    RejectionHandler)
  extends AbstractExecutorService {

  if (parallelism <= 0)
    throw new IllegalArgumentException("Size of pool cannot be less or equal to 0")

  import AffinityPool._

  // Held while starting/shutting down workers/pool in order to make
  // the operations linear and enforce atomicity. An example of that would be
  // adding a worker. We want the creation of the worker, addition
  // to the set and starting to worker to be an atomic action. Using
  // a concurrent set would not give us that
  private val bookKeepingLock = new ReentrantLock()

  // condition used for awaiting termination
  private val terminationCondition = bookKeepingLock.newCondition()

  // indicates the current state of the pool
  @volatile final private var poolState: PoolState = Running

  private final val workQueues = Array.fill(parallelism)(new BoundedAffinityTaskQueue(affinityGroupSize))
  private final val workers = mutable.Set[AffinityPoolWorker]()

  // maps a runnable to an index of a worker queue
  private[this] final val runnableToWorkerQueueIndex = new AtomicReference(ImmutableIntMap.empty)

  @inline private def locked[T](body: ⇒ T): T = {
    bookKeepingLock.lock()
    try {
      body
    } finally {
      bookKeepingLock.unlock()
    }
  }

  private def getQueueForRunnable(command: Runnable): BoundedAffinityTaskQueue = {
    val runnableHash = command.hashCode()

    def indexFor(h: Int): Int =
      Math.abs(reverseBytes(h * 0x9e3775cd) * 0x9e3775cd) % parallelism // In memory of Phil Bagwell

    val workQueueIndex =
      if (fairDistributionThreshold == 0)
        indexFor(runnableHash)
      else {
        @tailrec
        def updateAndOrGetIndex(): Int = {
          val prev = runnableToWorkerQueueIndex.get()
          if (prev.size > fairDistributionThreshold) indexFor(runnableHash)
          else {
            val existingIndex = prev.get(runnableHash)
            if (existingIndex >= 0) existingIndex
            else {
              val index = prev.size % parallelism
              val next = prev.updated(runnableHash, index)
              if (runnableToWorkerQueueIndex.compareAndSet(prev, next)) index // Successfully added key
              else updateAndOrGetIndex() // Try again
            }
          }
        }

        updateAndOrGetIndex()
      }

    workQueues(workQueueIndex)
  }

  //fires up initial workers
  locked {
    workQueues.foreach(q ⇒ addWorker(workers, q))
  }

  private def addWorker(workers: mutable.Set[AffinityPoolWorker], q: BoundedAffinityTaskQueue): Unit = {
    locked {
      val worker = new AffinityPoolWorker(q, new IdleStrategy(idleCpuLevel))
      workers.add(worker)
      worker.startWorker()
    }
  }

  /**
   * Each worker should go through that method while terminating.
   * In turn each worker is responsible for modifying the pool
   * state accordingly. For example if this is the last worker
   * and the queue is empty and we are in a ShuttingDown state
   * the worker can transition the pool to ShutDown and attempt
   * termination
   *
   * Furthermore, if this worker has experienced abrupt termination
   * due to an exception being thrown in user code, the worker is
   * responsible for adding one more worker to compensate for its
   * own termination
   *
   */
  private def onWorkerExit(w: AffinityPoolWorker, abruptTermination: Boolean): Unit =
    locked {
      workers.remove(w)
      if (workers.isEmpty && !abruptTermination && poolState >= ShuttingDown) {
        poolState = ShutDown // transition to shutdown and try to transition to termination
        attemptPoolTermination()
      }
      if (abruptTermination && poolState == Running)
        addWorker(workers, w.q)
    }

  override def execute(command: Runnable): Unit = {
    val queue = getQueueForRunnable(command) // Will throw NPE if command is null
    if (poolState != Running || !queue.add(command))
      rejectionHandler.reject(command, this)
  }

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    // recurse until pool is terminated or time out reached
    @tailrec
    def awaitTermination(nanos: Long): Boolean = {
      if (poolState == Terminated) true
      else if (nanos <= 0) false
      else awaitTermination(terminationCondition.awaitNanos(nanos))
    }

    locked {
      // need to hold the lock to avoid monitor exception
      awaitTermination(unit.toNanos(timeout))
    }
  }

  // WARNING: Only call while holding the bookKeepingLock
  private def attemptPoolTermination(): Unit =
    if (workers.isEmpty && poolState == ShutDown) {
      poolState = Terminated
      terminationCondition.signalAll()
    }

  override def shutdownNow(): util.List[Runnable] =
    locked {
      poolState = ShutDown
      workers.foreach(_.stop())
      attemptPoolTermination()
      // like in the FJ executor, we do not provide facility to obtain tasks that were in queue
      Collections.emptyList[Runnable]()
    }

  override def shutdown(): Unit =
    locked {
      poolState = ShuttingDown
      // interrupts only idle workers.. so others can process their queues
      workers.foreach(_.stopIfIdle())
      attemptPoolTermination()
    }

  override def isShutdown: Boolean = poolState >= ShutDown

  override def isTerminated: Boolean = poolState == Terminated

  private final class AffinityPoolWorker(val q: BoundedAffinityTaskQueue, val idleStrategy: IdleStrategy) extends Runnable {
    val thread: Thread = tf.newThread(this)
    @volatile private[this] var workerState: WorkerState = NotStarted

    def startWorker(): Unit = {
      workerState = Idle
      thread.start()
    }

    private[this] def runCommand(command: Runnable): Unit = {
      workerState = InExecution
      try
        command.run()
      finally
        workerState = Idle
    }

    override def run(): Unit = {

      /**
       * Determines whether the worker can keep running or not.
       * In order to continue polling for tasks three conditions
       * need to be satisfied:
       *
       * 1) pool state is less than Shutting down or queue
       * is not empty (e.g pool state is ShuttingDown but there are still messages to process)
       *
       * 2) the thread backing up this worker has not been interrupted
       *
       * 3) We are not in ShutDown state (in which we should not be processing any enqueued tasks)
       */
      def shouldKeepRunning =
        (poolState < ShuttingDown || !q.isEmpty) &&
          !Thread.interrupted() &&
          poolState != ShutDown

      var abruptTermination = true
      try {
        while (shouldKeepRunning) {
          val c = q.poll()
          if (c ne null) {
            runCommand(c)
            idleStrategy.reset()
          } else // if not wait for a bit
            idleStrategy.idle()
        }
        abruptTermination = false // if we have reached here, our termination is not due to an exception
      } finally {
        onWorkerExit(this, abruptTermination)
      }
    }

    def stop(): Unit =
      if (!thread.isInterrupted && workerState != NotStarted)
        thread.interrupt()

    def stopIfIdle(): Unit =
      if (workerState == Idle)
        stop()
  }
}

/**
 * INTERNAL API
 */
@InternalApi
@ApiMayChange
private[akka] final class AffinityPoolConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
  extends ExecutorServiceConfigurator(config, prerequisites) {

  private final val MaxFairDistributionThreshold = 2048

  private val poolSize = ThreadPoolConfig.scaledPoolSize(
    config.getInt("parallelism-min"),
    config.getDouble("parallelism-factor"),
    config.getInt("parallelism-max"))
  private val taskQueueSize = config.getInt("task-queue-size")

  private val idleCpuLevel = config.getInt("idle-cpu-level").requiring(level ⇒
    1 <= level && level <= 10, "idle-cpu-level must be between 1 and 10")

  private val fairDistributionThreshold = config.getInt("fair-work-distribution-threshold").requiring(thr ⇒
    0 <= thr && thr <= MaxFairDistributionThreshold, s"fair-work-distribution-threshold must be between 0 and $MaxFairDistributionThreshold")

  private val rejectionHandlerFCQN = config.getString("rejection-handler-factory")

  private val rejectionHandlerFactory = prerequisites.dynamicAccess
    .createInstanceFor[RejectionHandlerFactory](rejectionHandlerFCQN, Nil).recover({
      case exception ⇒ throw new IllegalArgumentException(
        s"Cannot instantiate RejectionHandlerFactory (rejection-handler-factory = $rejectionHandlerFCQN), make sure it has an accessible empty constructor",
        exception)
    }).get

  override def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
    new ExecutorServiceFactory {
      override def createExecutorService: ExecutorService =
        new AffinityPool(poolSize, taskQueueSize, threadFactory, idleCpuLevel, fairDistributionThreshold, rejectionHandlerFactory.create())
    }
}

trait RejectionHandler {
  def reject(command: Runnable, service: ExecutorService)
}

trait RejectionHandlerFactory {
  def create(): RejectionHandler
}

/**
 * INTERNAL API
 */
@InternalApi
@ApiMayChange
private[akka] final class DefaultRejectionHandlerFactory extends RejectionHandlerFactory {
  private class DefaultRejectionHandler extends RejectionHandler {
    override def reject(command: Runnable, service: ExecutorService): Unit =
      throw new RejectedExecutionException(s"Task $command rejected from $service")
  }
  override def create(): RejectionHandler = new DefaultRejectionHandler()
}

