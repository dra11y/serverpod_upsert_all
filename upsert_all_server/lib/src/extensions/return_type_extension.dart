import 'package:serverpod/serverpod.dart';

import '../generated/upsert_return_type.dart';

extension ReturnTypeExtensions<T extends TableRow>
    on Map<UpsertReturnType, List<T>> {
  List<T> get inserted => this[UpsertReturnType.inserted] ?? [];
  List<T> get updated => this[UpsertReturnType.updated] ?? [];
  List<T> get unchanged => this[UpsertReturnType.unchanged] ?? [];
  List<T> get changed => inserted + updated;
  List<T> get all => values.expand((element) => element).toList();
}
