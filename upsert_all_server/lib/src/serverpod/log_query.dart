// Currently internal in Serverpod, so recreating it here for now.
import 'dart:typed_data';

import 'package:serverpod/protocol.dart';
import 'package:serverpod/serverpod.dart';

void logQuery(
  Session session,
  String query,
  DateTime startTime, {
  int? numRowsAffected,
  exception,
  StackTrace? trace,
}) {
  // Check if this query should be logged.
  var logSettings = session.serverpod.logManager.getLogSettingsForSession(
    session,
  );
  var duration =
      DateTime.now().difference(startTime).inMicroseconds / 1000000.0;
  var slow = duration >= logSettings.slowQueryDuration;
  var shouldLog = session.serverpod.logManager.shouldLogQuery(
    session: session,
    slow: slow,
    failed: exception != null,
  );

  if (!shouldLog) {
    return;
  }

  // Use the current stack trace if there is no exception.
  trace ??= StackTrace.current;

  // Log the query.
  var entry = QueryLogEntry(
    sessionLogId: session.sessionLogs.temporarySessionId,
    serverId: session.server.serverId,
    query: query,
    duration: duration,
    numRows: numRowsAffected,
    error: exception?.toString(),
    stackTrace: trace.toString(),
    slow: slow,
    order: session.sessionLogs.currentLogOrderId,
  );
  session.serverpod.logManager.logQuery(session, entry);
  session.sessionLogs.currentLogOrderId += 1;
  session.sessionLogs.numQueries += 1;
}

T? _formatTableRow<T extends TableRow>(
    SerializationManager serializationManager,
    String tableName,
    Map<String, dynamic>? rawRow) {
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
