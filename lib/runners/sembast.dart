import 'dart:io';

import 'package:hive_benchmark/runners/runner.dart';
import 'package:path_provider/path_provider.dart';
import 'package:sembast/sembast.dart';
import 'package:sembast/sembast_io.dart';
import 'package:path/path.dart' as path;

class SembastRunner implements BenchmarkRunner {
  Database _db;

  @override
  Future<int> batchDeleteInt(List<String> keys) async {
    final s = Stopwatch()..start();
    var store = StoreRef<String, int>.main();

    await _db.transaction((txn) async {
      keys.forEach((key) async {
        await store.record(key).delete(_db);
      });
    });

    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchDeleteString(List<String> keys) async {
    final s = Stopwatch()..start();
    var store = StoreRef<String, String>.main();

    await _db.transaction((txn) async {
      keys.forEach((key) async {
        await store.record(key).delete(_db);
      });
    });

    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchReadInt(List<String> keys) async {
    final s = Stopwatch()..start();
    var store = StoreRef<String, int>.main();

    await store.records(keys).get(_db);

    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchReadString(List<String> keys) async {
    final s = Stopwatch()..start();
    var store = StoreRef<String, String>.main();

    await store.records(keys).get(_db);

    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchWriteInt(Map<String, int> entries) async {
    final s = Stopwatch()..start();
    var store = StoreRef<String, int>.main();

    await _db.transaction((txn) async {
      entries.forEach((key, val) async {
        await store.record(key).put(_db, val);
      });
    });

    return s.elapsedMilliseconds;
  }

  @override
  Future<int> batchWriteString(Map<String, String> entries) async {
    final s = Stopwatch()..start();
    var store = StoreRef<String, String>.main();

    await _db.transaction((txn) async {
      entries.forEach((key, val) async {
        await store.record(key).put(_db, val);
      });
    });

    return s.elapsedMilliseconds;
  }

  @override
  String get name => 'Sembast';

  @override
  Future<void> setUp() async {
    var dir = await getApplicationDocumentsDirectory();
    var homePath = path.join(dir.path, 'sembast');
    if (await Directory(homePath).exists()) {
      await Directory(homePath).delete(recursive: true);
    }
    await Directory(homePath).create();
    _db = await dbFactory.openDatabase('$homePath/sembast.db');
  }

  @override
  Future<void> tearDown() async {
    await _db.close();
  }
}

DatabaseFactory dbFactory = databaseFactoryIo;
