// ignore_for_file: invalid_use_of_internal_member

import 'package:serverpod/protocol.dart';
import 'package:serverpod/serverpod.dart';

// Currently internal in Serverpod, so recreating it here for now.
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
