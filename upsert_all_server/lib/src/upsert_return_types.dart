import 'generated/protocol.dart';

class UpsertReturnTypes {
  static const Set<UpsertReturnType> changes = {
    UpsertReturnType.inserted,
    UpsertReturnType.updated,
  };

  static const Set<UpsertReturnType> all = {
    UpsertReturnType.inserted,
    UpsertReturnType.updated,
    UpsertReturnType.unchanged,
  };
}
