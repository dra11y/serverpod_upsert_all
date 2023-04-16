extension ChunkedList<T> on List<T> {
  List<List<T>> chunked(int batchSize) {
    int numberOfChunks = (length / batchSize).ceil();
    List<List<T>> chunks = List<List<T>>.generate(numberOfChunks,
        (index) => List<T>.from(skip(index * batchSize).take(batchSize)));
    return chunks;
  }
}
