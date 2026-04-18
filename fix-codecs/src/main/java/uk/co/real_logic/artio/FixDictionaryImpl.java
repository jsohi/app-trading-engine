package uk.co.real_logic.artio;

/**
 * Bridge class at the default Artio lookup path ({@code uk.co.real_logic.artio.FixDictionaryImpl}).
 *
 * <p>Artio's {@code EngineConfiguration.conclude()} and {@code LibraryConfiguration.conclude()}
 * both call {@code FixDictionary.findDefault()} which reflectively loads this class from the
 * default package {@code uk.co.real_logic.artio}. Our Artio-generated codecs live in {@code
 * com.trading.engine.fix} (a non-default package), so this class extends the real generated
 * implementation and makes it discoverable at the default lookup path.
 *
 * <p>This avoids having to set {@code acceptorfixDictionary()} on every {@code EngineConfiguration}
 * and {@code LibraryConfiguration} in the codebase.
 */
public class FixDictionaryImpl extends com.trading.engine.fix.FixDictionaryImpl {}
