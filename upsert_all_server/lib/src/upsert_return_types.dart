import 'generated/protocol.dart';

class UpsertReturnTypes {
  static const Set<UpsertReturnType> changeTypes = {
    UpsertReturnType.inserted,
    UpsertReturnType.updated,
  };
  static const Set<UpsertReturnType> allTypes = {
    UpsertReturnType.inserted,
    UpsertReturnType.updated,
    UpsertReturnType.unchanged,
  };
}
