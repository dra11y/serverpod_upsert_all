import 'package:serverpod/serverpod.dart';

import 'generated/protocol.dart';
import 'extensions/extensions.dart';
import 'serverpod_internals/serverpod_internals.dart';
import 'upsert_return_types.dart';

Future<Map<UpsertReturnType, List<T>>> upsertAll<T extends TableRow>(
  Session session, {
  required Iterable<T> rows,
  int batchSize = 100,
  required Set<Column> uniqueBy,
  List<Column> excludedCriteriaColumns = const [],
  List<Column> nonUpdatableColumns = const [],
  Set<UpsertReturnType> returning = UpsertReturnTypes.changes,
  Transaction? transaction,
}) async {
  // Do nothing if passed an empty list.
  if (rows.isEmpty) return {};

  // Make sure ON CONFLICT column(s) specified.
  assert(uniqueBy.isNotEmpty);

  if (excludedCriteriaColumns.isEmpty) {
    excludedCriteriaColumns = [
      ColumnInt('id'),
      ColumnDateTime('createdAt'),
      ColumnDateTime('updatedAt'),
    ];
  }

  if (nonUpdatableColumns.isEmpty) {
    nonUpdatableColumns = [ColumnInt('id'), ColumnDateTime('createdAt')];
  }

  var table = session.serverpod.serializationManager.getTableForType(T);
  assert(table is Table, '''
You need to specify a template type that is a subclass of TableRow.
E.g. myRows = await session.db.find<MyTableClass>(where: ...);
Current type was $T''');
  if (table == null) return {};

  var startTime = DateTime.now();

  // Convert all rows to JSON.
  List<Map> dataList = rows.map((row) => row.toJsonForDatabase()).toList();

  // Get all columns in the table that are present in the JSON.
  // It only makes sense to include the `id` column if it is in the ON CONFLICT columns.
  List<Column> columns = table.columns
      .where((column) =>
          uniqueBy.contains(ColumnInt('id')) || column.columnName != 'id')
      .where((column) =>
          dataList.any((data) => data.containsKey(column.columnName)))
      .toList();
  Map<String, String> columnTypes = Map.fromEntries(columns
      .map((column) => MapEntry(column.columnName, column.databaseType)));
  List<String> columnsList =
      columns.map((column) => column.columnName).toList();
  List<String> quotedColumnsList =
      columnsList.map((columnName) => '"$columnName"').toList();
  final onConflictColumnsList =
      uniqueBy.map((column) => '"${column.columnName}"').toList();
  final excludedCriteriaColumnsList = excludedCriteriaColumns
      .map((column) => '"${column.columnName}"')
      .toList();
  final nonUpdatableColumnsList =
      nonUpdatableColumns.map((column) => '"${column.columnName}"').toList();

  final tableName = table.tableName;
  final insertColumns =
      quotedColumnsList.where((column) => column != '"id"').join(', ');
  final updateSetList = quotedColumnsList
      .where((column) => !nonUpdatableColumnsList.contains(column))
      .map((column) => '$column = input_values.$column')
      .join(',\n    ');
  final quotedOnConflicts = onConflictColumnsList.join(', ');
  final updateWhere = onConflictColumnsList
      .map((column) => '$tableName.$column = input_values.$column')
      .join(' AND ');
  final distinctOrConditionsList = quotedColumnsList
      .where((column) =>
          !excludedCriteriaColumnsList.contains(column) &&
          !onConflictColumnsList.contains(column))
      .map((column) =>
          '$tableName.$column IS DISTINCT FROM input_values.$column');

  final distinctOrConditions = distinctOrConditionsList.isNotEmpty
      ? 'AND (${distinctOrConditionsList.join(' OR ')})'
      : '';

  var batches = dataList.chunked(batchSize);
  Map<UpsertReturnType, List<T>> resultsMap = {};

  final selectResultsUnion = [
    if (returning.contains(UpsertReturnType.inserted))
      "SELECT id, *, ${UpsertReturnType.inserted.index} AS \"returnType\" FROM inserted_rows",
    if (returning.contains(UpsertReturnType.updated))
      "SELECT id, *, ${UpsertReturnType.updated.index} AS \"returnType\" FROM updated_rows",
    if (returning.contains(UpsertReturnType.unchanged))
      "SELECT id, *, ${UpsertReturnType.unchanged.index} AS \"returnType\" FROM unchanged_rows",
  ].join(' UNION ALL ');

  var index = 0;
  for (var batch in batches) {
    print("Batch $index of ${batches.length}");
    index++;

    var valuesList = batch
        .asMap()
        .map((index, data) {
          final rowValues = [];
          for (var column in columns) {
            final columnName = column.columnName;
            // final isJson = column is ColumnSerializable;
            // double-quote JSON values, but not other values.
            // var convertedValue = isJson
            //     ? '"${data[columnName]}"'
            //     : DatabasePoolManager.encoder.convert(data[columnName]);
            var convertedValue =
                DatabasePoolManager.encoder.convert(data[columnName]);
            if (index == 0) {
              print('columnName $columnName type = ${columnTypes[columnName]}');
              convertedValue += '::${columnTypes[columnName]}';
            }
            rowValues.add(convertedValue);
          }
          return MapEntry(index, rowValues);
        })
        .values
        .toList();

    final inputValues =
        valuesList.map((values) => values.join(', ')).join('),\n    (');

    final query = """WITH input_values ($insertColumns) AS (
        VALUES ($inputValues)
      ), updated_rows AS (
        UPDATE $tableName
          SET $updateSetList FROM input_values
        WHERE $updateWhere
          $distinctOrConditions
        RETURNING $tableName.*
      ), unchanged_rows AS (
        SELECT * FROM $tableName
          WHERE ($quotedOnConflicts) IN
            (SELECT $quotedOnConflicts FROM input_values)
        EXCEPT SELECT * FROM updated_rows
      ), inserted_rows AS (
        INSERT INTO $tableName ($insertColumns)
        SELECT $insertColumns FROM input_values
        ON CONFLICT ($quotedOnConflicts) DO NOTHING
        RETURNING $tableName.*
      ), results AS (
        $selectResultsUnion
      )
        SELECT * FROM results;
    """;

    try {
      var databaseConnection = await session.db.databaseConnection;

      var context = transaction != null
          ? transaction.postgresContext
          : databaseConnection.postgresConnection;

      print('query = $query');

      var result = await context.mappedResultsQuery(
        query,
        allowReuse: false,
        timeoutInSeconds: 60,
        substitutionValues: {},
      );

      for (var rawRow in result) {
        final value = rawRow.values.first;
        final returnType = UpsertReturnType.fromJson(value['returnType'])!;
        final row = formatTableRow<T>(
            session.serverpod.serializationManager, tableName, value);
        if (row == null) continue;
        resultsMap[returnType] = [
          ...resultsMap[returnType] ?? [],
          row,
        ];
      }
    } catch (e, trace) {
      logQuery(session, query, startTime, exception: e, trace: trace);
      rethrow;
    }

    logQuery(session, query, startTime, numRowsAffected: resultsMap.length);
  }

  return resultsMap;
}
