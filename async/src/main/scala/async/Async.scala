package async

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future.never.recoverWith
import scala.util.{Failure, Success, Try}

object Async extends AsyncInterface {

  /**
   * Transforms a successful asynchronous `Int` computation
   * into a `Boolean` indicating whether the number was even or not.
   * In case the given `Future` value failed, this method
   * should return a failed `Future` with the same error.
   */
  def transformSuccess(eventuallyX: Future[Int]): Future[Boolean] = {
    eventuallyX map { x =>
      isEven(x)
    } recoverWith {
      case t: Throwable => println("Error: ")
        Future.failed(t)
    }
  }

  val isEven: (Int => Boolean) = (x: Int) => (x % 2) == 0

  /**
   * Transforms a failed asynchronous `Int` computation into a
   * successful one returning `-1`.
   * Any non-fatal failure should be recovered.
   * In case the given `Future` value was successful, this method
   * should return a successful `Future` with the same value.
   */
  def recoverFailure(eventuallyX: Future[Int]): Future[Int] = {
    eventuallyX.flatMap {
      case x => Future.successful(x)
    } recoverWith {
      case t: Throwable => println("Error: ")
        Future.successful(-1)
    }
  }

  /**
   * Perform two asynchronous computation, one after the other. `makeAsyncComputation2`
   * should start ''after'' the `Future` returned by `makeAsyncComputation1` has
   * completed.
   * In case the first asynchronous computation failed, the second one should not even
   * be started.
   * The returned `Future` value should contain the successful result of the first and
   * second asynchronous computations, paired together.
   */
  def sequenceComputations[A, B](
    makeAsyncComputation1: () => Future[A],
    makeAsyncComputation2: () => Future[B]
  ): Future[(A, B)] =  {
    for {
      a <- makeAsyncComputation1()
      b <- makeAsyncComputation2()
    } yield (a, b)
  }

  /**
   * Concurrently perform two asynchronous computations and pair their successful
   * result together.
   * The two computations should be started independently of each other.
   * If one of them fails, this method should return the failure.
   */
  def concurrentComputations[A, B](
      makeAsyncComputation1: () => Future[A],
      makeAsyncComputation2: () => Future[B]
  ): Future[(A, B)] = {

    val comp1 = makeAsyncComputation1()
    val comp2 = makeAsyncComputation2()

    val result = for {
      c1 <- comp1
      c2 <- comp2
    } yield (c1, c2)

    result recoverWith {
      case t: Throwable => Future.failed(t)
    }
  }


  /**
   * Attempt to perform an asynchronous computation.
   * In case of failure this method should try again to make
   * the asynchronous computation so that at most `maxAttempts`
   * are eventually performed.
   */
  def insist[A](makeAsyncComputation: () => Future[A], maxAttempts: Int): Future[A] =  {
    val result = for {
      c1 <- makeAsyncComputation()
    } yield c1
    result recoverWith {
      case _ if maxAttempts > 1 => insist(makeAsyncComputation, maxAttempts - 1)
      case t: Throwable => Future.failed(t)
    }
  }


  /**
   * Turns a callback-based API into a Future-based API
   * @return A `FutureBasedApi` that forwards calls to `computeIntAsync` to the `callbackBasedApi`
   *         and returns its result in a `Future` value
   *
   * Hint: Use a `Promise`
   */
  def futurize(callbackBasedApi: CallbackBasedApi): FutureBasedApi =  {
    val futureBasedApi: FutureBasedApi = new FutureBasedApi {
      val p = Promise[Int]
      override def computeIntAsync(): Future[Int] = {
        callbackBasedApi.computeIntAsync {
          case Success(x) => p.success(x)
          case Failure(e) => p.failure(e)
        }
        p.future
      }
    }
    futureBasedApi
  }

}

/**
  * Dummy example of a callback-based API
  */
trait CallbackBasedApi {
  def computeIntAsync(continuation: Try[Int] => Unit): Unit
}

/**
  * API similar to [[CallbackBasedApi]], but based on `Future` instead
  */
trait FutureBasedApi {
  def computeIntAsync(): Future[Int]
}
