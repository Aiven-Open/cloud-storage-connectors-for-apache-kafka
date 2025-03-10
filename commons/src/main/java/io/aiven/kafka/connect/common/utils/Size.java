package io.aiven.kafka.connect.common.utils;

public final class Size {
    public static final long KB = 1024;
    public static final long MB = KB * 1024;
    public static final long GB = MB * 1024;
    public static final long TB = GB * 1024L;

    public static long ofKB(final int kb) {
        return kb * KB;
    }

    public static long ofMB(final int mb) {
        return mb * MB;
    }
    public static long ofGB(final int gb) {
        return gb * GB;
    }
    public static long ofTB(final int tb) {
        return tb * TB;
    }

    public static long toKB(final long size) {
        return size * KB;
    }

    public static int toMB(final long size) {
        return (int) (size / MB);
    }

    public static int toGB(final long size) {
        return (int) (size / GB);
    }

    public static int toTB(final long size) {
        return (int) (size / TB);
    }

}
