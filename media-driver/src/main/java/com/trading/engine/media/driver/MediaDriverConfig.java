package com.trading.engine.media.driver;

import io.aeron.driver.ThreadingMode;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Configuration for the standalone Aeron Media Driver. */
public final class MediaDriverConfig {

  private static final String PROPERTIES_FILE = "media-driver.properties";

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
      // Fall through to hard-coded defaults
    }

    return new MediaDriverConfig(
        props.getProperty("aeron.dir", DEFAULT_AERON_DIR),
        ThreadingMode.valueOf(props.getProperty("threading.mode", DEFAULT_THREADING_MODE.name())),
        Integer.parseInt(
            props.getProperty("term.buffer.length", String.valueOf(DEFAULT_TERM_BUFFER_LENGTH))),
        Integer.parseInt(
            props.getProperty("ipc.term.length", String.valueOf(DEFAULT_IPC_TERM_LENGTH))),
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
      if (arg.startsWith("--aeron-dir=")) {
        aeronDir = arg.substring("--aeron-dir=".length());
      } else if (arg.startsWith("--threading-mode=")) {
        threadingMode = ThreadingMode.valueOf(arg.substring("--threading-mode=".length()));
      } else if (arg.startsWith("--term-buffer-length=")) {
        termBufferLength = Integer.parseInt(arg.substring("--term-buffer-length=".length()));
      } else if (arg.startsWith("--ipc-term-length=")) {
        ipcTermLength = Integer.parseInt(arg.substring("--ipc-term-length=".length()));
      } else if (arg.startsWith("--dir-delete-on-start=")) {
        dirDeleteOnStart = Boolean.parseBoolean(arg.substring("--dir-delete-on-start=".length()));
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
