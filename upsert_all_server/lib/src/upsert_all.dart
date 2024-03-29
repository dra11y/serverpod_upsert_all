import 'dart:convert';

import 'package:collection/collection.dart';
import 'package:serverpod/serverpod.dart';

import 'extensions/extensions.dart';
import 'generated/protocol.dart';
import 'serverpod_internals/serverpod_internals.dart';
import 'upsert_return_types.dart';

extension IterableColumnExtension<T extends Column> on Iterable<T> {
  Iterable<String> get names => map((column) => column.columnName);
  Iterable<String> get quotedNames => map((column) => '"${column.columnName}"');
  String get joinedQuotedNames => quotedNames.join(', ');
}

Future<void> resetIdSequence({
  required Session session,
  required String tableName,
  Transaction? transaction,
}) async {
  final databaseConnection = await session.db.databaseConnection;

  final context = transaction != null
      ? transaction.postgresContext
      : databaseConnection.postgresConnection;

  /// Reset the sequence to the highest id to avoid weird id INSERT conflicts.
  /// Comment on SO: "inserting with ids is not updating the incrementor for the column"
  /// https://stackoverflow.com/questions/44708548/postgres-complains-id-already-exists-after-insert-of-initial-data
  await context.execute(
      "SELECT setval('${tableName}_id_seq', max(id)) FROM $tableName;");
}

Future<Map<UpsertReturnType, List<T>>> upsertAll<T extends TableRow>(
  Session session, {
  required Iterable<T> rows,
  required Set<Column> onConflict,
  int batchSize = 100,
  bool doUpdate = true,
  Set<Column> updateOnly = const {},
  Set<Column> ignoreColumns = const {},
  Set<Column> nonUpdatableColumns = const {},
  Set<UpsertReturnType> returning = UpsertReturnTypes.changes,
  Transaction? transaction,
  String createdAtName = 'createdAt',
  String updatedAtName = 'updatedAt',
}) async {
  // Do nothing if passed an empty list.
  if (rows.isEmpty) return {};

  assert(onConflict.isNotEmpty, 'onConflict columns is required.');

  final table = session.serverpod.serializationManager.getTableForType(T);

  assert(table is Table, '''
You need to specify a template type that is a subclass of TableRow.
E.g. myRows = await session.db.find<MyTableClass>(where: ...);
Current type was $T''');

  if (table == null) return {};

  final tableName = table.tableName;

  // Convert all rows to JSON.
  List<Map> dataList = rows.map((row) => row.toJsonForDatabase()).toList();

  // We should IGNORE the `id` column (UNLESS it is in the ON CONFLICT columns).
  // We should ALWAYS IGNORE the timestamp columns when comparing,
  // because they are most likely instantiated in dart and will be different.
  ignoreColumns = ignoreColumns.union({
    ColumnInt('id'),
    ColumnDateTime(createdAtName),
    ColumnDateTime(updatedAtName),
  });

  // It does not make sense to update the ON CONFLICT columns.
  // We should NEVER UPDATE the created at column.
  nonUpdatableColumns = nonUpdatableColumns.union({
    ...onConflict,
    ColumnDateTime(createdAtName),
  });

  // Get all columns in the table that are present in the JSON.
  // It only makes sense to include the `id` column if it is in the ON CONFLICT columns.
  Set<Column> columns = table.columns
      .where((column) => (column.columnName != 'id' ||
          column.columnName == 'id' && onConflict.names.contains('id')))
      .where((column) =>
          dataList.any((data) => data.containsKey(column.columnName)))
      .toSet();

  final jsonColumns = columns.whereType<ColumnSerializable>();

  Set<Column> updateColumns = columns
      .whereNot((col) => nonUpdatableColumns.names.contains(col.columnName))
      .where((col) =>
          updateOnly.isEmpty || updateOnly.names.contains(col.columnName))
      .toSet();

  final updateSet = updateColumns.quotedNames
      .map((col) => '$col = EXCLUDED.$col')
      .join(',\n        ');

  final updateWhereNames =
      {...updateColumns.quotedNames}.difference({...ignoreColumns.quotedNames});

  final updateWhere = updateWhereNames.map((col) {
    final toJsonB = jsonColumns.quotedNames.contains(col) ? '::jsonb' : '';
    return '$tableName.$col$toJsonB IS DISTINCT FROM EXCLUDED.$col$toJsonB';
  }).join('\n        OR ');

  final batches = dataList.chunked(batchSize);
  Map<UpsertReturnType, List<T>> resultsMap = {};

  for (int batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    final batch = batches[batchIndex];

    await resetIdSequence(
        session: session, tableName: tableName, transaction: transaction);

    print(
        "upsertAll $tableName batch ${batchIndex + 1} of ${batches.length}, size = $batchSize, start = ${batchIndex * batchSize}, length = ${batch.length}");

    final Iterable<MapEntry<String, dynamic>> substitutionValuesEntries = batch
        .mapIndexed((index, row) => columns.map((column) {
              final columnName = column.columnName;
              // final convertedValue =
              //     DatabasePoolManager.encoder.convert(row[columnName]);
              // return MapEntry('$columnName$index', convertedValue);
              final value = column.databaseType == 'json'
                  // ? '\'${jsonEncode((row[columnName] as SerializableEntity).allToJson())}\':jsonb'
                  ? jsonEncode(row[columnName])
                  : row[columnName];
              return MapEntry('$columnName$index', value);
            }))
        .expand((entries) => entries);

    final Map<String, dynamic> substitutionValues =
        Map.fromEntries(substitutionValuesEntries);

    final valuesList = '\n        (' +
        batch
            .mapIndexed((index, row) =>
                columns.names.map((columnName) => '@$columnName$index'))
            .map((row) => row.join(', '))
            .join('),\n        (') +
        ')';

    final Iterable<MapEntry<String, dynamic>>
        onConflictSubstitutionValuesEntries = batch
            .mapIndexed((index, row) => onConflict.names.map((columnName) {
                  final value = row[columnName] is SerializableEntity
                      ? '\'${jsonEncode((row[columnName] as SerializableEntity).allToJson())}\':jsonb'
                      : row[columnName];
                  return MapEntry('$columnName$index', value);
                }))
            .expand((entries) => entries);

    final Map<String, dynamic> onConflictSubstitutionValues =
        Map.fromEntries(onConflictSubstitutionValuesEntries);

    final onConflictValues = ' (' +
        batch
            .mapIndexed((index, row) =>
                onConflict.names.map((columnName) => '@$columnName$index'))
            .map((row) => row.join(', '))
            .join('), (') +
        ') ';

    final query = """
      WITH changed AS (
        INSERT INTO $tableName (${columns.joinedQuotedNames})
        VALUES $valuesList
        ON CONFLICT (${onConflict.joinedQuotedNames})
        """ +
        ((updateColumns.isNotEmpty && doUpdate)
            ? """
        DO UPDATE SET
          $updateSet
        WHERE $updateWhere
        """
            : "        DO NOTHING\n") +
        """RETURNING *, CASE xmax
            WHEN 0 THEN ${UpsertReturnType.inserted}
            ELSE ${UpsertReturnType.updated}
          END AS "returnType"
      )
      SELECT * FROM changed
      UNION ALL
      SELECT *, ${UpsertReturnType.unchanged} AS "returnType"
      FROM $tableName
      WHERE id NOT IN (SELECT id FROM changed)
      AND (${onConflict.joinedQuotedNames}) IN
      (
        $onConflictValues
      );
    """;

    // print('==================================');
    // print(query);

    final startTime = DateTime.now();

    try {
      final databaseConnection = await session.db.databaseConnection;

      final context = transaction != null
          ? transaction.postgresContext
          : databaseConnection.postgresConnection;

      final batchResults = await context.mappedResultsQuery(
        query,
        allowReuse: false,
        timeoutInSeconds: 60,
        substitutionValues: {
          ...substitutionValues,
          ...onConflictSubstitutionValues,
        },
      );

      // print('batchResults.length = ${batchResults.length}');
      // print('batchResults = $batchResults');

      for (final rawRow in batchResults) {
        final value = rawRow.values.first;
        final returnType = UpsertReturnType.fromJson(value['returnType'])!;
        T? row;
        try {
          row = formatTableRow<T>(
              session.serverpod.serializationManager, tableName, value);
        } catch (error) {
          print('tableName = $tableName');
          print('error value = $value');
          print(error);
        }

        if (row == null) continue;
        resultsMap[returnType] = [
          ...resultsMap[returnType] ?? [],
          row,
        ];
      }
    } catch (e, trace) {
      print('==========\n\n\nquery = $query\n\n\n==========\n\n\n');
      print('==========\n\n\nsubstitutionValues = ${{
        ...substitutionValues,
        ...onConflictSubstitutionValues,
      }}\n\n\n==========\n\n\n');

      logQuery(session, query, startTime, exception: e, trace: trace);
      rethrow;
    }

    /// "The count is the number of rows inserted or updated."
    /// Thus, count of unchanged rows is not included in `numRowsAffected`.
    /// Documented under "Outputs": https://www.postgresql.org/docs/current/sql-insert.html#id-1.9.3.152.7
    logQuery(session, query, startTime,
        numRowsAffected: resultsMap.changed.length);
  }

  await resetIdSequence(
      session: session, tableName: tableName, transaction: transaction);

  print(
      '\tresults: ${resultsMap.keys.map((resultType) => '${resultsMap[resultType]!.length} ${resultType.name}').join(', ')}');

  // final inserted = resultsMap[UpsertReturnType.inserted];
  // final updated = resultsMap[UpsertReturnType.updated];
  // if (inserted != null && inserted.isNotEmpty) {
  //   print('inserted ids: ${inserted.map((r) => r.id!).join(', ')}');
  // }
  // if (updated != null && updated.isNotEmpty) {
  //   print('updated ids: ${updated.map((r) => r.id!).join(', ')}');
  // }

  return resultsMap;
}
