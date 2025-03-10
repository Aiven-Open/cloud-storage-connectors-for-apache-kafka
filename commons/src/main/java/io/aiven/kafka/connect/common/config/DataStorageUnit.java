package io.aiven.kafka.connect.common.config;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Definitions and conversions between IEC 80000-13:2025 units.
 *
 * @see <a href='https://en.wikipedia.org/wiki/Binary_prefix'>Binary Prefixes</a>
 * @see <a href='https://iec.ch/prefixes-binary-multiples'>Prefixes for binary multiples</a>
 */
public enum DataStorageUnit {
    BYTES("B") {
        public long toBytes(long d) {
            return d;
        }

        public long toKibibytes(long d) {
            return (d / 1024L);
        }

        public long toMebibytes(long d) {
            return (d / (1024L * 1024));
        }

        public long toGibibytes(long d) {
            return (d / (1024L * 1024 * 1024));
        }

        public long convert(long source, DataStorageUnit sourceUnit) {
            return sourceUnit.toBytes(source);
        }
    },
    KIBIBYTES("KiB") {
        public long toBytes(long d) {
            return x(d, 1024L, (MAX / 1024L));
        }

        public long toKibibytes(long d) {
            return d;
        }

        public long toMebibytes(long d) {
            return (d / 1024L);
        }

        public long toGibibytes(long d) {
            return (d / (1024L * 1024));
        }

        public long convert(long source, DataStorageUnit sourceUnit) {
            return sourceUnit.toKibibytes(source);
        }
    },
    MEBIBYTES("MiB") {
        public long toBytes(long d) {
            return x(d, (1024L * 1024), MAX / (1024L * 1024));
        }

        public long toKibibytes(long d) {
            return x(d, 1024L, (MAX / 1024L));
        }

        public long toMebibytes(long d) {
            return d;
        }

        public long toGibibytes(long d) {
            return (d / 1024L);
        }

        public long convert(long source, DataStorageUnit sourceUnit) {
            return sourceUnit.toMebibytes(source);
        }
    },
    GIBIBYTES("GiB") {
        public long toBytes(long d) {
            return x(d, (1024L * 1024 * 1024), MAX / (1024L * 1024 * 1024));
        }

        public long toKibibytes(long d) {
            return x(d, (1024L * 1024), MAX / (1024L * 1024));
        }

        public long toMebibytes(long d) {
            return x(d, 1024L, (MAX / 1024L));
        }

        public long toGibibytes(long d) {
            return d;
        }

        public long convert(long source, DataStorageUnit sourceUnit) {
            return sourceUnit.toGibibytes(source);
        }
    };

    /**
     * Scale d by m, checking for overflow. This has a short name to make above code more readable.
     */
    static long x(long d, long m, long over) {
        assert (over > 0) && (over < (MAX - 1L)) && (over == (MAX / m));

        if (d > over)
            return Long.MAX_VALUE;
        return Math.multiplyExact(d, m);
    }

    /**
     * @param symbol the unit symbol
     * @return the memory unit corresponding to the given symbol
     */
    public static DataStorageUnit fromSymbol(String symbol) {
        for (DataStorageUnit value : values()) {
            if (value.symbol.equalsIgnoreCase(symbol))
                return value;
        }
        throw new IllegalArgumentException(String.format("Unsupported data storage unit: %s. Supported units are: %s",
                symbol, Arrays.stream(values())
                        .map(u -> u.symbol)
                        .collect(Collectors.joining(", "))));
    }

    static final long MAX = Long.MAX_VALUE;

    /**
     * The unit symbol
     */
    private final String symbol;

    DataStorageUnit(String symbol) {
        this.symbol = symbol;
    }

    public long toBytes(long d) {
        throw new AbstractMethodError();
    }

    public long toKibibytes(long d) {
        throw new AbstractMethodError();
    }

    public long toMebibytes(long d) {
        throw new AbstractMethodError();
    }

    public long toGibibytes(long d) {
        throw new AbstractMethodError();
    }

    public long convert(long source, DataStorageUnit sourceUnit) {
        throw new AbstractMethodError();
    }
}
