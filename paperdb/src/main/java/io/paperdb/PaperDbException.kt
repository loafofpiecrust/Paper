package io.paperdb

class PaperDbException : RuntimeException {
    constructor(detailMessage: String) : super(detailMessage) {}

    constructor(detailMessage: String, throwable: Throwable) : super(detailMessage, throwable) {}
}
