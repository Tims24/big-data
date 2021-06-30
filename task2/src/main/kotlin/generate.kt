import kotlin.collections.ArrayList
import java.math.BigInteger
import java.util.Random
import java.io.File

fun main() {
    writeNumbersToFile()
}

private fun generateNumbers(qty: Int = 2000, bits: Int = 48): ArrayList<BigInteger> {
    val random = Random()
    val numbers = arrayListOf<BigInteger>()

    for (i in 1..qty) {
        numbers.add(BigInteger(bits, random))
    }

    return numbers
}

private fun writeNumbersToFile(fileName: String = "numbers.txt") {
    File(fileName).writeText(generateNumbers().joinToString("\n"))
}