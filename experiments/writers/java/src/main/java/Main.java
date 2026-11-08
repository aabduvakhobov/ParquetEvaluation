import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;


public class Main {

    private static final String WRITER_NAME = "java";

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: java -jar parquet-writer-java.jar <glob-path> <output.parquet> <dataset-name> <sqlite-database>");
            System.exit(1);
        }

        String globPath = args[0];
        String outputParquet = args[1];
        String datasetName = args[2];
        String sqlitePath = args[3];

        List<Path> files = collectFiles(globPath);
        if (files.isEmpty()) {
            System.err.println("No files found for pattern: " + globPath);
            System.exit(2);
        }
        System.out.println(files.get(0));
        Configuration conf = new Configuration();
        MessageType schema = readSchema(files.get(0), conf);
        GroupWriteSupport.setSchema(schema, conf);

        long totalWriteTimeNanos = 0;
        long totalReadAndWriteTimeNanos = System.nanoTime();

        ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(outputParquet))
                .withConf(conf)
                .withType(schema)
                .build();
        try {
            for (Path inputFile : files) {
                System.out.println(inputFile.toString());
                try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(inputFile.toUri()) )
                        .withConf(conf)
                        .build()) {
                    Group group;
                    long writeStartNanos = System.nanoTime();
                    while ((group = reader.read()) != null) {
                        writer.write(group);
                    }
                    long writeElapsedNanos = System.nanoTime() - writeStartNanos;
                    totalWriteTimeNanos += writeElapsedNanos;
                }
            }
        } finally {
            long closeStartNanos = System.nanoTime();
            writer.close();
            long closeElapsedNanos = System.nanoTime() - closeStartNanos;
            totalWriteTimeNanos += closeElapsedNanos;
        }

        long totalReadAndWriteElapsedNanos = System.nanoTime() - totalReadAndWriteTimeNanos;
        double totalReadAndWriteTimeSeconds = totalReadAndWriteElapsedNanos / 1_000_000_000.0;
        double overallWriteTimeSeconds = totalWriteTimeNanos / 1_000_000_000.0;

        System.out.println("Wrote " + files.size() + " input files to " + outputParquet);
        System.out.println("Total read + write time: " + String.format("%.6f", totalReadAndWriteTimeSeconds) + " seconds");
        System.out.println("Writer-only time: " + String.format("%.6f", overallWriteTimeSeconds) + " seconds");

        // Recorded after the timers stopped, so it does not affect the measured times.
        long fileSizeInBytes = Files.size(Paths.get(outputParquet));
        try {
            recordRun(sqlitePath, datasetName, totalReadAndWriteTimeSeconds, fileSizeInBytes);
        } catch (SQLException e) {
            System.err.println("Failed to record run in " + sqlitePath + ": " + e.getMessage());
            System.exit(1);
        }
        System.out.println("Recorded run in " + sqlitePath + " (table writer_runs)");
    }


    /** Appends one row to the writer_runs table shared by the C++, Go and Java writers, creating it if needed. */
    private static void recordRun(String sqlitePath, String datasetName, double totalWriteTime, long fileSizeInBytes)
            throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA busy_timeout = 60000");
                statement.execute("CREATE TABLE IF NOT EXISTS writer_runs ("
                        + "dataset_name TEXT, writer TEXT, total_write_time REAL, file_size_in_bytes INTEGER)");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO writer_runs (dataset_name, writer, total_write_time, file_size_in_bytes) VALUES (?, ?, ?, ?)")) {
                insert.setString(1, datasetName);
                insert.setString(2, WRITER_NAME);
                insert.setDouble(3, totalWriteTime);
                insert.setLong(4, fileSizeInBytes);
                insert.executeUpdate();
            }
        }
    }


    private static MessageType readSchema(Path file, Configuration conf) throws IOException {
        InputFile inputFile = new LocalInputFile(file);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                return reader.getFooter().getFileMetaData().getSchema();
            }
    }


    private static List<Path> collectFiles(String globPattern) throws IOException {
        Path inputPath = Paths.get(globPattern);
        if (Files.isDirectory(inputPath)) {
            List<Path> matchedFiles = new ArrayList<>();
            collectFilesFromDirectory(inputPath, path -> true, matchedFiles);
            matchedFiles.sort(Comparator.comparing(Path::getFileName, Comparator.naturalOrder())
                    .thenComparing(Path::toString));
            return matchedFiles;
        }

        boolean containsGlob = globPattern.contains("*") || globPattern.contains("?") || globPattern.contains("[") || globPattern.contains("]");
        if (!containsGlob && Files.isRegularFile(inputPath)) {
            return List.of(inputPath);
        }

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
        Path basePath = inputPath.getParent();
        if (basePath == null) {
            basePath = Paths.get(".");
        }
        if (!Files.exists(basePath)) {
            basePath = Paths.get(".");
        }

        List<Path> matchedFiles = new ArrayList<>();
        collectFilesFromDirectory(basePath, matcher::matches, matchedFiles);
        matchedFiles.sort(Comparator.comparing(Path::getFileName, Comparator.naturalOrder())
                .thenComparing(Path::toString));

        return matchedFiles;
    }

    private static void collectFilesFromDirectory(Path directory, java.util.function.Predicate<Path> predicate, List<Path> matchedFiles) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    collectFilesFromDirectory(entry, predicate, matchedFiles);
                } else if (Files.isRegularFile(entry) && predicate.test(entry)) {
                    matchedFiles.add(entry);
                }
            }
        } catch (AccessDeniedException ignored) {
            // Skip unreadable directories when walking the filesystem.
        }
    }
}
