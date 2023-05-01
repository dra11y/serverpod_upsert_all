import 'package:serverpod/serverpod.dart';

extension DatabaseTypeExtension<E extends Enum> on Column {
  /// Get the qgsql type (string) that represents this [TypeDefinition] in the database.
  String get databaseType {
    if (this is ColumnEnum) {
      return 'integer';
    }

    // TODO: add all supported types here
    switch (runtimeType) {
      case ColumnInt:
        return 'integer';
      case ColumnDouble:
        return 'double precision';
      case ColumnBool:
        return 'boolean';
      case ColumnDateTime:
        return 'timestamp without time zone';
      case ColumnByteData:
        return 'bytea';
      case ColumnUuid:
        return 'uuid';
      case ColumnString:
        return 'text';
      case ColumnDuration:
        return 'bigint';
      case ColumnSerializable:
        return 'json';
      default:
        print('$columnName IS UNKNOWN!');
        return 'unknown';
    }
  }
}
