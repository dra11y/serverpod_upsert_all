import 'package:serverpod/serverpod.dart';

import 'generated/protocol.dart';
import 'extensions/extensions.dart';
import 'serverpod_internals/serverpod_internals.dart';
import 'upsert_return_types.dart';

Future<Map<UpsertReturnType, List<T>>> upsertAll<T extends TableRow>(
  Session session, {
  required Iterable<T> rows,
  required Set<Column> uniqueBy,
  int batchSize = 100,
  Set<Column> updateOnly = const {},
  Set<Column> ignoreColumns = const {},
  Set<Column> nonUpdatableColumns = const {},
  Set<UpsertReturnType> returning = UpsertReturnTypes.changes,
  Transaction? transaction,
}) async {
  // Do nothing if passed an empty list.
  if (rows.isEmpty) return {};

  // Make sure ON CONFLICT column(s) specified.
  assert(uniqueBy.isNotEmpty);

  ignoreColumns = ignoreColumns.union({
    ColumnInt('id'),
    ColumnDateTime('createdAt'),
    ColumnDateTime('updatedAt'),
  });

  final ignoreColumnNames = ignoreColumns.map((col) => col.columnName).toSet();

  nonUpdatableColumns = nonUpdatableColumns.union({
    ColumnInt('id'),
    ColumnDateTime('createdAt'),
  });

  final nonUpdatableColumnNames =
      nonUpdatableColumns.map((col) => col.columnName).toSet();

  var table = session.serverpod.serializationManager.getTableForType(T);
  assert(table is Table, '''
You need to specify a template type that is a subclass of TableRow.
E.g. myRows = await session.db.find<MyTableClass>(where: ...);
Current type was $T''');
  if (table == null) return {};

  var startTime = DateTime.now();

  // Convert all rows to JSON.
  List<Map> dataList = rows.map((row) => row.toJsonForDatabase()).toList();

  final updateOnlyNames = updateOnly.map((col) => col.columnName).toSet();

  // Get all columns in the table that are present in the JSON.
  // It only makes sense to include the `id` column if it is in the ON CONFLICT columns.
  Set<Column> columns = table.columns
      .where((column) =>
          uniqueBy.contains(ColumnInt('id')) || column.columnName != 'id')
      .where((column) =>
          dataList.any((data) => data.containsKey(column.columnName)))
      .toSet();

  final uniqueByNames = uniqueBy.map((col) => col.columnName).toSet();

  Set<Column> updateColumns = columns
      .where((col) =>
          !nonUpdatableColumnNames.contains(col.columnName) &&
          !uniqueByNames.contains(col.columnName))
      .where((col) =>
          updateOnly.isEmpty || updateOnlyNames.contains(col.columnName))
      .toSet();

  Set<String> updateColumnNames =
      updateColumns.map((col) => col.columnName).toSet();

  Set<String> quotedUpdateColumnNames =
      updateColumnNames.map((colName) => '"$colName"').toSet();

  Map<String, String> columnTypes = Map.fromEntries(columns
      .map((column) => MapEntry(column.columnName, column.databaseType)));
  Set<String> columnNames = columns.map((column) => column.columnName).toSet();
  // Set<String> quotedColumns =
  //     columnNames.map((columnName) => '"$columnName"').toSet();
  final onConflictColumns = uniqueBy.map((column) => column.columnName).toSet();
  final quotedOnConflictColumns =
      onConflictColumns.map((colName) => '"$colName"').toSet();
  // final checkColumnsForSkipUpdate = columnNames
  //     .where((colName) => !ignoreColumnNames.contains(colName))
  //     .toSet();

  final skipUpdate = updateColumns.isEmpty;
  // final quotedIgnoreColumns =
  //     ignoreColumns.map((column) => '"${column.columnName}"').toSet();
  // final quotedNonUpdatableColumns =
  //     nonUpdatableColumns.map((column) => '"${column.columnName}"').toSet();

  final tableName = table.tableName;
  final insertColumns = columnNames
      .where((column) => column != 'id')
      .map((col) => '"$col"')
      .join(', ');
  final insertColumnsWithTypes = columnNames
      .where((column) => column != 'id')
      .map((col) =>
          columnTypes[col] != null ? '"$col"::${columnTypes[col]!}' : '"$col"')
      .join(', ');
  // final insertColumnsForCompareWithJsonB =
  //     insertColumnsWithTypes.replaceAll(RegExp('(?<!jsonb)::json'), '::jsonb');
  final updateSetList = quotedUpdateColumnNames
      .where((colName) => !nonUpdatableColumnNames.contains(colName))
      .map((colName) => '$colName = input_values.$colName')
      .join(',\n    ');
  final quotedOnConflicts = quotedOnConflictColumns.join(', ');
  final updateWhere = quotedOnConflictColumns
      .map((column) =>
          '$tableName.$column IS NOT DISTINCT FROM input_values.$column')
      .join(' AND ');
  final updateWhereNotExistsInserted = quotedOnConflictColumns
      .map((column) =>
          '$tableName.$column IS NOT DISTINCT FROM inserted_rows.$column')
      .join(' AND ');
  // final returningColumns = columnNames
  //     .where((column) => column != 'id')
  //     .map((col) => '$tableName."$col"')
  //     .join(', ');

  final distinctOrConditionsList = columnNames
      .where(
        (colName) =>
            !ignoreColumnNames.contains(colName) &&
            !onConflictColumns.contains(colName) &&
            (updateOnly.isEmpty || updateOnlyNames.contains(colName)),
      )
      .map((colName) =>
          '$tableName."$colName" IS DISTINCT FROM input_values."$colName"');

  final distinctOrConditions = distinctOrConditionsList.isNotEmpty
      ? 'AND (${distinctOrConditionsList.join(' OR ')})'
      : '';

  var batches = dataList.chunked(batchSize);
  Map<UpsertReturnType, List<T>> resultsMap = {};

  final selectResultsUnion = [
    if (returning.contains(UpsertReturnType.inserted))
      // "SELECT id, $insertColumnsWithTypes, ${UpsertReturnType.inserted.index} AS \"returnType\" FROM inserted_rows",
      "SELECT inserted_rows.*, ${UpsertReturnType.inserted.index} AS \"returnType\" FROM inserted_rows",
    if (!skipUpdate && returning.contains(UpsertReturnType.updated))
      // "SELECT id, $insertColumnsWithTypes, ${UpsertReturnType.updated.index} AS \"returnType\" FROM updated_rows",
      "SELECT updated_rows.*, ${UpsertReturnType.updated.index} AS \"returnType\" FROM updated_rows",
    if (returning.contains(UpsertReturnType.unchanged))
      // "SELECT id, $insertColumnsWithTypes, ${UpsertReturnType.unchanged.index} AS \"returnType\" FROM unchanged_rows",
      "SELECT unchanged_rows.*, ${UpsertReturnType.unchanged.index} AS \"returnType\" FROM unchanged_rows",
  ].join(' UNION ALL ');

  var index = 0;
  for (var batch in batches) {
    print(
        "upsertAll $tableName batch ${index + 1} of ${batches.length}, size = $batchSize, start = ${index * batchSize}, length = ${batch.length}");
    index++;

    var valuesList = batch
        .asMap()
        .map((index, data) {
          final rowValues = [];
          for (var column in columns) {
            final columnName = column.columnName;
            var convertedValue =
                DatabasePoolManager.encoder.convert(data[columnName]);
            if (index == 0) {
              // print('columnName $columnName type = ${columnTypes[columnName]}');
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

    final query = """
      WITH input_values ($insertColumns) AS (
        VALUES ($inputValues)
      ), inserted_rows AS (
        INSERT INTO $tableName ($insertColumns)
        SELECT $insertColumnsWithTypes FROM input_values
        ON CONFLICT ($quotedOnConflicts) DO NOTHING
        RETURNING $tableName.*
      ), ${skipUpdate ? '' : '''updated_rows AS (
        UPDATE $tableName
          SET $updateSetList FROM input_values
        WHERE $updateWhere
          $distinctOrConditions
          AND NOT EXISTS (
            SELECT 1 FROM inserted_rows WHERE $updateWhereNotExistsInserted
          )
        RETURNING $tableName.*
      ),'''} changed_ids AS (
          SELECT id FROM inserted_rows
          ${skipUpdate ? '' : 'UNION ALL SELECT id FROM updated_rows'}
      ), unchanged_rows AS (
        SELECT $tableName.* FROM $tableName
          WHERE ($quotedOnConflicts) IN
            (SELECT $quotedOnConflicts FROM input_values)
          AND $tableName.id NOT IN (
            SELECT id FROM changed_ids
          )
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

      print('==========\n\n\nquery = $query\n\n\n==========\n\n\n');

      var result = await context.mappedResultsQuery(
        query,
        allowReuse: false,
        timeoutInSeconds: 60,
        substitutionValues: {},
      );

      // print('result = $result');

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

    /// "The count is the number of rows inserted or updated."
    /// Thus, count of unchanged rows is not included in `numRowsAffected`.
    /// Documented under "Outputs": https://www.postgresql.org/docs/current/sql-insert.html#id-1.9.3.152.7
    logQuery(session, query, startTime,
        numRowsAffected: resultsMap.changed.length);
  }

  print(
      '\tresults: ${resultsMap.keys.map((type) => '${resultsMap[type]!.length} ${type.name}').join(', ')}');

  return resultsMap;
}
