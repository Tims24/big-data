import java.io.File
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.schedulers.Schedulers
import akka.actor.AbstractActor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.japi.pf.ReceiveBuilder
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

fun measure(block: () -> Unit) {
    val nanotime: Long = measureNanoTime(block)
    val millis = TimeUnit.NANOSECONDS.toMillis(nanotime)
    println("$millis ms")
}

fun main() {
    println("Choose the method")
    when(readLine()) {
        "akka" -> AkkaFactorsCounter().count()
        "naive" -> measure {println("Naive: ${naive()}") }
        "multithread" -> measure {println("Multithread: ${multithread()}") }
        "rx" -> measure { println("RXJava: ${rx()}") }
        else -> {print("No such method")}
    }
}

private fun getLines(fileName: String = "numbers.txt"): List<String> {
    return File(fileName).readLines()
}
private fun naive(): Long {
    var count: Long = 0
    val lines = getLines()

    lines.forEach { count += factorize(BigInteger(it)) }

    return count
}
private fun multithread(): Long {
    val numThreads = Runtime.getRuntime().availableProcessors()
    val numbers = getLines().map { it.toBigInteger() }
    val result = AtomicLong(0)
    val lock = ReentrantLock()
    var curIndex = 0
    var number: BigInteger

    val threads = List(numThreads) {
        Thread {
            var threadResult: Long = 0
            while (curIndex < numbers.size) {
                try {
                    lock.lock()
                    number = numbers[curIndex]
                    curIndex++
                } finally {
                    lock.unlock()
                }
                threadResult += factorize(number)
            }
            result.addAndGet(threadResult)
        }
    }

    for (thread in threads)
        thread.start()

    for (thread in threads)
        thread.join()

    return result.toLong()
}
private fun rx(): Long {
    val numbers = getLines().map { it.toBigInteger() }

    return Flowable.fromIterable(numbers).
    onBackpressureBuffer().
    parallel().
    runOn(Schedulers.computation()).
    map { number -> factorize(number) }.
    sequential().
    reduceWith({ 0 }, { acc: Long, item: Long -> acc + item }).
    blockingGet()
}
data class CountFactors(val countFactors: Long)

class MasterActor : AbstractActor() {
    private var numberOfWorkers = 6
    private var finished = 0

    private var workers = Array<ActorRef>(numberOfWorkers) {
        context.actorOf(Props.create(SlaveActor::class.java))
    }

    private var startTime = 0L
    private var count = 0L

    override fun createReceive() =
        ReceiveBuilder().
        match(File::class.java) { file ->
            val numbers = file.readLines().map { BigInteger(it) }
            numberOfWorkers = numbers.size
            workers = Array(numberOfWorkers) { context.actorOf(Props.create(SlaveActor::class.java)) }
            startTime = System.currentTimeMillis()
            for (i in 0 until numberOfWorkers) {
                workers[i].tell(numbers[i], self)
            }
        }.match(CountFactors::class.java) {
            count += it.countFactors
            finished++
            if (finished == numberOfWorkers) {
                val endTime = System.currentTimeMillis()
                val elapsed = endTime - startTime

                println("Akka result: $count")
                println("$elapsed ms")
                context.system.terminate()
            }
        }.build()!!
}
class SlaveActor : AbstractActor() {
    override fun createReceive() =
        ReceiveBuilder().
        match(BigInteger::class.java) {
            val countFactors = factorize(it)
            sender.tell(CountFactors(countFactors), self)
        }.matchAny {
            println("Something went wrong")
        }.build()!!
}
class AkkaFactorsCounter {
    fun count() {


        val file = File("numbers.txt")

        val actorSystem = ActorSystem.create("factorization")
        val actorRef = actorSystem.actorOf(Props.create(MasterActor::class.java))
        actorRef.tell(file, actorRef)
    }
}

private fun factorize(num: BigInteger): Long {
    var number = num
    var count: Long = 0
    var factor = BigInteger.valueOf(2)

    while (number.remainder(factor) == BigInteger.ZERO) {
        count++
        number = number.divide(factor)
    }

    factor++

    while (factor * factor <= number) {
        if (number.remainder(factor) == BigInteger.ZERO) {
            count++
            number = number.divide(factor)
        } else {
            factor += BigInteger.valueOf(2)
        }
    }

    count++

    return count
}