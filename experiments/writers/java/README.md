# Java Parquet writer

Writes a dataset with the default writer properties of parquet-java (parquet-hadoop 1.17.0).

## Requirements
- **Java 21** (JDK). [pom.xml](pom.xml) compiles for Java 21 (`maven.compiler.source/target = 21`).
- Maven 3.9 or newer.

Tested with OpenJDK 21.0.12 and Maven 3.9.16.

## Compile
```bash
mvn package
```

This builds a self-contained jar with all dependencies, `target/parquet-writer-java-1.0.0-jar-with-dependencies.jar`. To run it you only need a Java 21 runtime, or newer.

## Run
```bash
java -Djava.io.tmpdir=/path/to/new/tmp -jar target/parquet-writer-java-1.0.0-jar-with-dependencies.jar "/path/to/parquet_files_dir/*" /path/to/new_file.parquet <dataset-name> /path/to/default_writers.db
```

The run is recorded in the `writer_runs` table of the SQLite database. See [experiments/README.md](../../README.md#default-configurations-of-the-parquet-implementations). The jar bundles `sqlite-jdbc`, including its native SQLite library.

`java.io.tmpdir` should point to a disk with enough free space for large datasets.
