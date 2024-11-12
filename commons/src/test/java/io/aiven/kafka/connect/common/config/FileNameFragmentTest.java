package io.aiven.kafka.connect.common.config;

public class FileNameFragmentTest {

    public enum FileNameArgs {
    GROUP_FILE(FileNameFragment.GROUP_FILE), FILE_COMPRESSION_TYPE_CONFIG(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG),
    FILE_MAX_RECORDS (FileNameFragment.FILE_MAX_RECORDS),
    FILE_NAME_TIMESTAMP_TIMEZONE (FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE),
    FILE_NAME_TIMESTAMP_SOURCE (FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE),
    FILE_NAME_TEMPLATE_CONFIG (FileNameFragment.FILE_NAME_TEMPLATE_CONFIG),
    DEFAULT_FILENAME_TEMPLATE (FileNameFragment.DEFAULT_FILENAME_TEMPLATE);
    String key;
    FileNameArgs(String key){
        this.key=key;
    }

    public String key() {
        return key;
    }
}

}
