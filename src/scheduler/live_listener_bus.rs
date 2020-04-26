use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::scheduler::listener::ListenerEvent;
use crate::{Error, Result};
use parking_lot::{Mutex, RwLock};

trait AsyncEventQueue: Send + Sync {
    fn post(&mut self, event: Arc<dyn ListenerEvent>);
    fn start(&mut self);
    fn stop(&mut self);
}

type QueueBuffer = Option<Arc<Mutex<Vec<Arc<dyn ListenerEvent>>>>>;

/// Asynchronously passes SparkListenerEvents to registered SparkListeners.
///
/// Until `start()` is called, all posted events are only buffered. Only after this listener bus
/// has started will events be actually propagated to all attached listeners. This listener bus
/// is stopped when `stop()` is called, and it will drop further events after stopping.
#[derive(Clone)]
pub(in crate::scheduler) struct LiveListenerBus {
    /// Indicate if `start()` is called
    started: Arc<AtomicBool>,
    /// Indicate if `stop()` is called
    stopped: Arc<AtomicBool>,
    queued_events: QueueBuffer,
    queues: Arc<RwLock<Vec<Box<dyn AsyncEventQueue>>>>,
}

impl Default for LiveListenerBus {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveListenerBus {
    pub fn new() -> Self {
        LiveListenerBus {
            started: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            queued_events: Some(Arc::new(Mutex::new(vec![]))),
            queues: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Post an event to all queues.
    pub fn post(&self, event: Box<dyn ListenerEvent>) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }

        //TODO: self.metrics.num_events_posted.inc()

        match self.queued_events {
            None => {
                // If the event buffer is null, it means the bus has been started and we can avoid
                // synchronization and post events directly to the queues. This should be the most
                // common case during the life of the bus.
                self.post_to_queues(event);
            }
            Some(ref queue) => {
                // Otherwise, need to synchronize to check whether the bus is started, to make sure the thread
                // calling start() picks up the new event.
                if !self.started.load(Ordering::SeqCst) {
                    queue.lock().push(Arc::from(event));
                } else {
                    // If the bus was already started when the check above was made, just post directly to the queues.
                    self.post_to_queues(event);
                }
            }
        }
    }

    fn post_to_queues(&self, event: Box<dyn ListenerEvent>) {
        let event: Arc<dyn ListenerEvent> = Arc::from(event);
        for queue in &mut *self.queues.write() {
            queue.post(event.clone());
        }
    }

    /// Start sending events to attached listeners.
    ///
    /// This first sends out all buffered events posted before this listener bus has started, then
    /// listens for any additional events asynchronously while the listener bus is still running.
    /// This should only be called once.
    fn start(&mut self) -> Result<()> {
        if !self.started.compare_and_swap(false, true, Ordering::SeqCst) {
            return Err(Error::Other);
        }

        let mut queues = self.queues.write();
        {
            let queued_events = self
                .queued_events
                .as_ref()
                .ok_or_else(|| /* Cannot be some if it was already started */ Error::Other)?
                .lock();
            for queue in queues.iter_mut() {
                queue.start();
                queued_events
                    .iter()
                    .for_each(|event| queue.post(event.clone()));
            }
        }
        self.queued_events = None;
        // TODO: metricsSystem.registerSource(metrics)
        Ok(())
    }

    /// Stop the listener bus. It will wait until the queued events have been processed, but drop the
    /// new events after stopping.
    fn stop(&mut self) -> Result<()> {
        if !self.started.load(Ordering::SeqCst) {
            return Err(Error::Other);
        }

        if !self.stopped.compare_and_swap(false, true, Ordering::SeqCst) {
            return Ok(());
        }

        let mut queues = self.queues.write();
        for queue in queues.iter_mut() {
            queue.stop();
        }
        queues.clear();
        Ok(())
    }
}

/*
private[spark] class LiveListenerBus(conf: SparkConf) {

  import LiveListenerBus._

  private var sparkContext: SparkContext = _

  private[spark] val metrics = new LiveListenerBusMetrics(conf)

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  /** Add a listener to queue shared by all non-internal listeners. */
  def addToSharedQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, SHARED_QUEUE)
  }

  /** Add a listener to the executor management queue. */
  def addToManagementQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, EXECUTOR_MANAGEMENT_QUEUE)
  }

  /** Add a listener to the application status queue. */
  def addToStatusQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, APP_STATUS_QUEUE)
  }

  /** Add a listener to the event log queue. */
  def addToEventLogQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, EVENT_LOG_QUEUE)
  }

  /**
   * Add a listener to a specific queue, creating a new queue if needed. Queues are independent
   * of each other (each one uses a separate thread for delivering events), allowing slower
   * listeners to be somewhat isolated from others.
   */
  private[spark] def addToQueue(
      listener: SparkListenerInterface,
      queue: String): Unit = synchronized {
    if (stopped.get()) {
      throw new IllegalStateException("LiveListenerBus is stopped.")
    }

    queues.asScala.find(_.name == queue) match {
      case Some(queue) =>
        queue.addListener(listener)

      case None =>
        val newQueue = new AsyncEventQueue(queue, conf, metrics, this)
        newQueue.addListener(listener)
        if (started.get()) {
          newQueue.start(sparkContext)
        }
        queues.add(newQueue)
    }
  }

  def removeListener(listener: SparkListenerInterface): Unit = synchronized {
    // Remove listener from all queues it was added to, and stop queues that have become empty.
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val deadline = System.currentTimeMillis + timeoutMillis
    queues.asScala.foreach { queue =>
      if (!queue.waitUntilEmpty(deadline)) {
        throw new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
      }
    }
  }

  // For testing only.
  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag](): Seq[T] = {
    queues.asScala.flatMap { queue => queue.findListenersByClass[T]() }
  }

  // For testing only.
  private[spark] def listeners: JList[SparkListenerInterface] = {
    queues.asScala.flatMap(_.listeners.asScala).asJava
  }

  // For testing only.
  private[scheduler] def activeQueues(): Set[String] = {
    queues.asScala.map(_.name).toSet
  }

  // For testing only.
  private[scheduler] def getQueueCapacity(name: String): Option[Int] = {
    queues.asScala.find(_.name == name).map(_.capacity)
  }
}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  private[scheduler] val SHARED_QUEUE = "shared"

  private[scheduler] val APP_STATUS_QUEUE = "appStatus"

  private[scheduler] val EXECUTOR_MANAGEMENT_QUEUE = "executorManagement"

  private[scheduler] val EVENT_LOG_QUEUE = "eventLog"
}

private[spark] class LiveListenerBusMetrics(conf: SparkConf)
  extends Source with Logging {

  override val sourceName: String = "LiveListenerBus"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * The total number of events posted to the LiveListenerBus. This is a count of the total number
   * of events which have been produced by the application and sent to the listener bus, NOT a
   * count of the number of events which have been processed and delivered to listeners (or dropped
   * without being delivered).
   */
  val numEventsPosted: Counter = metricRegistry.counter(MetricRegistry.name("numEventsPosted"))

  // Guarded by synchronization.
  private val perListenerClassTimers = mutable.Map[String, Timer]()

  /**
   * Returns a timer tracking the processing time of the given listener class.
   * events processed by that listener. This method is thread-safe.
   */
  def getTimerForListenerClass(cls: Class[_ <: SparkListenerInterface]): Option[Timer] = {
    synchronized {
      val className = cls.getName
      val maxTimed = conf.get(LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED)
      perListenerClassTimers.get(className).orElse {
        if (perListenerClassTimers.size == maxTimed) {
          logError(s"Not measuring processing time for listener class $className because a " +
            s"maximum of $maxTimed listener classes are already timed.")
          None
        } else {
          perListenerClassTimers(className) =
            metricRegistry.timer(MetricRegistry.name("listenerProcessingTime", className))
          perListenerClassTimers.get(className)
        }
      }
    }
  }

}
*/
