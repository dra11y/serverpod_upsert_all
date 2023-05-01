import 'dart:typed_data';

import 'package:serverpod/serverpod.dart';

T? formatTableRow<T extends TableRow>(SerializationManager serializationManager,
    String tableName, Map<String, dynamic>? rawRow) {
  var data = <String, dynamic>{};

  for (var columnName in rawRow!.keys) {
    var value = rawRow[columnName];

    if (value is DateTime) {
      data[columnName] = value.toIso8601String();
    } else if (value is Uint8List) {
      var byteData = ByteData.view(
        value.buffer,
        value.offsetInBytes,
        value.length,
      );
      data[columnName] = byteData.base64encodedString();
    } else {
      data[columnName] = value;
    }
  }

  return serializationManager.deserialize<T>(data);
}
