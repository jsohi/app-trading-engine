package com.trading.engine.media.driver;

import io.aeron.driver.ThreadingMode;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Configuration for the standalone Aeron Media Driver. */
public final class MediaDriverConfig {

  private static final Logger LOG = LogManager.getLogger(MediaDriverConfig.class);
  private static final String PROPERTIES_FILE = "media-driver.properties";

  private static final String ARG_AERON_DIR = "--aeron-dir=";
  private static final String ARG_THREADING_MODE = "--threading-mode=";
  private static final String ARG_TERM_BUFFER_LENGTH = "--term-buffer-length=";
  private static final String ARG_IPC_TERM_LENGTH = "--ipc-term-length=";
  private static final String ARG_DIR_DELETE_ON_START = "--dir-delete-on-start=";

  private static final String DEFAULT_AERON_DIR = "/dev/shm/aeron-trading";
  private static final ThreadingMode DEFAULT_THREADING_MODE = ThreadingMode.SHARED;
  private static final int DEFAULT_TERM_BUFFER_LENGTH = 256 * 1024;
  private static final int DEFAULT_IPC_TERM_LENGTH = 256 * 1024;
  private static final boolean DEFAULT_DIR_DELETE_ON_START = true;

  private final String aeronDir;
  private final ThreadingMode threadingMode;
  private final int termBufferLength;
  private final int ipcTermLength;
  private final boolean dirDeleteOnStart;

  public MediaDriverConfig(
      final String aeronDir,
      final ThreadingMode threadingMode,
      final int termBufferLength,
      final int ipcTermLength,
      final boolean dirDeleteOnStart) {
    this.aeronDir = aeronDir;
    this.threadingMode = threadingMode;
    this.termBufferLength = termBufferLength;
    this.ipcTermLength = ipcTermLength;
    this.dirDeleteOnStart = dirDeleteOnStart;
  }

  /** Load defaults from the classpath properties file. */
  public static MediaDriverConfig loadDefaults() {
    final Properties props = new Properties();
    try (InputStream in =
        MediaDriverConfig.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
      if (in != null) {
        props.load(in);
      }
    } catch (final IOException e) {
      LOG.warn("Failed to load '{}', using hard-coded defaults", PROPERTIES_FILE, e);
    }

    final ThreadingMode threadingMode =
        getEnumProperty(props, "threading.mode", DEFAULT_THREADING_MODE);
    final int termBufferLength =
        getIntProperty(props, "term.buffer.length", DEFAULT_TERM_BUFFER_LENGTH);
    final int ipcTermLength = getIntProperty(props, "ipc.term.length", DEFAULT_IPC_TERM_LENGTH);

    return new MediaDriverConfig(
        props.getProperty("aeron.dir", DEFAULT_AERON_DIR),
        threadingMode,
        termBufferLength,
        ipcTermLength,
        Boolean.parseBoolean(
            props.getProperty("dir.delete.on.start", String.valueOf(DEFAULT_DIR_DELETE_ON_START))));
  }

  /** Parse CLI arguments, falling back to defaults from the properties file. */
  public static MediaDriverConfig parseArgs(final String[] args) {
    final MediaDriverConfig defaults = loadDefaults();

    String aeronDir = defaults.aeronDir();
    ThreadingMode threadingMode = defaults.threadingMode();
    int termBufferLength = defaults.termBufferLength();
    int ipcTermLength = defaults.ipcTermLength();
    boolean dirDeleteOnStart = defaults.dirDeleteOnStart();

    for (final String arg : args) {
      final int eqIdx = arg.indexOf('=');
      if (eqIdx < 0) {
        continue;
      }
      final String key = arg.substring(0, eqIdx + 1);
      final String value = arg.substring(eqIdx + 1);

      switch (key) {
        case ARG_AERON_DIR:
          if (!value.isEmpty()) {
            aeronDir = value;
          } else {
            LOG.warn("Empty value for --aeron-dir, using default: {}", defaults.aeronDir());
          }
          break;
        case ARG_THREADING_MODE:
          threadingMode = parseEnumArg(value, "--threading-mode", defaults.threadingMode());
          break;
        case ARG_TERM_BUFFER_LENGTH:
          termBufferLength =
              parseIntArg(value, "--term-buffer-length", defaults.termBufferLength());
          break;
        case ARG_IPC_TERM_LENGTH:
          ipcTermLength = parseIntArg(value, "--ipc-term-length", defaults.ipcTermLength());
          break;
        case ARG_DIR_DELETE_ON_START:
          dirDeleteOnStart = Boolean.parseBoolean(value);
          break;
        default:
          break;
      }
    }

    return new MediaDriverConfig(
        aeronDir, threadingMode, termBufferLength, ipcTermLength, dirDeleteOnStart);
  }

  public String aeronDir() {
    return aeronDir;
  }

  public ThreadingMode threadingMode() {
    return threadingMode;
  }

  public int termBufferLength() {
    return termBufferLength;
  }

  public int ipcTermLength() {
    return ipcTermLength;
  }

  public boolean dirDeleteOnStart() {
    return dirDeleteOnStart;
  }

  private static int getIntProperty(
      final Properties props, final String key, final int defaultValue) {
    try {
      return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
    } catch (final NumberFormatException e) {
      LOG.warn("Invalid {} in '{}', using default: {}", key, PROPERTIES_FILE, defaultValue);
      return defaultValue;
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Enum<E>> E getEnumProperty(
      final Properties props, final String key, final E defaultValue) {
    try {
      return (E) Enum.valueOf(defaultValue.getClass(), props.getProperty(key, defaultValue.name()));
    } catch (final IllegalArgumentException e) {
      LOG.warn("Invalid {} in '{}', using default: {}", key, PROPERTIES_FILE, defaultValue);
      return defaultValue;
    }
  }

  private static int parseIntArg(final String value, final String argName, final int defaultValue) {
    try {
      return Integer.parseInt(value);
    } catch (final NumberFormatException e) {
      LOG.warn("Invalid value for {}, using default: {}", argName, defaultValue);
      return defaultValue;
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Enum<E>> E parseEnumArg(
      final String value, final String argName, final E defaultValue) {
    try {
      return (E) Enum.valueOf(defaultValue.getClass(), value);
    } catch (final IllegalArgumentException e) {
      LOG.warn("Invalid value for {}, using default: {}", argName, defaultValue);
      return defaultValue;
    }
  }

  @Override
  public String toString() {
    return "MediaDriverConfig{"
        + "aeronDir='"
        + aeronDir
        + '\''
        + ", threadingMode="
        + threadingMode
        + ", termBufferLength="
        + termBufferLength
        + ", ipcTermLength="
        + ipcTermLength
        + ", dirDeleteOnStart="
        + dirDeleteOnStart
        + '}';
  }
}
