object Main {
    def main(args: Array[String]): Unit = {
        if (args.length != 1) {
            println("Usage:\nspark-submit --master yarn --deploy-mode cluster " +
              "--class Main <jar name>.jar <timeout in minutes>")
            return
        }
        var timeout: Long = 0
        try timeout = args(0).toLong * 60000
        catch {
            case _: NumberFormatException =>
                println("Cannot parse number of minutes")
                return
        }
        if (timeout < 0) {
            println("Number of minutes must be non-negative")
            return
        } else if (timeout == 0) {
            println("No timeout")
        }
        Stream.start(timeout)
    }
}
