object Main {
    def main(args: Array[String]): Unit = {
        if (args.length != 1) {
            println("Usage:\nspark-submit --master yarn --class Main <jar name>.jar <timeout in minutes>")
            return
        }
        var timeout: Long = 0
        try timeout = args(0).toLong * 60000
        catch {
            case _: NumberFormatException =>
                println("Cannot parse number of minutes")
                return
        }
        if (timeout <= 0) {
            println("Number of minutes must be positive")
            return
        }
        Stream.start(timeout)
    }
}
