package com.grand.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.UUID;

public class IceBergOnMinioWithJavaExample {

    public static final String tableLocation = "s3a://my-warehouse";

    public static Configuration getHadoopConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", "jiandaoyun");
        conf.set("fs.s3a.secret.key", "jiandaoyun");
        conf.set("fs.s3a.endpoint", "http://172.24.34.176:9100");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"); // 明确指定文件系统实现
        return conf;
    }

    // 创建HadoopCatalog（正确用法）
    public static Catalog createCatalog() {
        Configuration hadoopConf = getHadoopConfig();
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, tableLocation );
        return catalog;
    }

    public static void main(String[] args) throws IOException {
        createDataList();
//        dropTable();
    }

    public static void createDataList() throws IOException {
        Catalog catalog = createCatalog();
        Namespace namespace = Namespace.of("demo");
        TableIdentifier tableId = TableIdentifier.of(namespace, "users");
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );
        boolean isExist = catalog.tableExists(tableId);
        Table table;
        if (!isExist) {
            table = catalog.createTable(tableId, schema);
        } else {
            table = catalog.loadTable(tableId);
        }
        String filePath = tableLocation + "/data/data-" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filePath);
        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(schema);
        FileAppender<Record> appender = appenderFactory.newAppender(outputFile, FileFormat.PARQUET);
        // 6. 构造并写入数据（示例写入两条记录）
        try {
            GenericRecord record1 = GenericRecord.create(schema);
            record1.setField("id", 1);
            record1.setField("name", "Alice");
            appender.add(record1);

            GenericRecord record2 = GenericRecord.create(schema);
            record2.setField("id", 2);
            record2.setField("name", "Bob");
            appender.add(record2);
        } finally {
            appender.close();
        }

        DataFile dataFile = DataFiles.builder(table.spec())
            .withPath(filePath)
            .withFileSizeInBytes(table.io().newInputFile(filePath).getLength())
            .withRecordCount(2)
            .build();
        table.newAppend()
            .appendFile(dataFile)
            .commit();
    }

    public static void dropTable() {
        Catalog catalog = createCatalog();
        Namespace namespace = Namespace.of("demo");
        TableIdentifier tableId = TableIdentifier.of(namespace, "users");
        boolean b = catalog.dropTable(tableId);
        System.out.println(b);
    }
}
