package io.paperdb

internal class PaperTable<T> {

    // Serialized content
    var mContent: T

    constructor(content: T) {
        mContent = content
    }
}
