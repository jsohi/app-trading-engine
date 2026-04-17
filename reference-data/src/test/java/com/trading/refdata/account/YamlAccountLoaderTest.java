package com.trading.refdata.account;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.refdata.ReferenceDataLoadException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.Test;

/** Tests YAML loading, validation, and edge-case handling for {@link YamlAccountLoader}. */
final class YamlAccountLoaderTest {

  private Path testResource(final String name) {
    final var url = Objects.requireNonNull(getClass().getClassLoader().getResource(name), name);
    try {
      return Path.of(url.toURI());
    } catch (final URISyntaxException e) {
      throw new AssertionError("invalid test resource URI: " + url, e);
    }
  }

  @Test
  void loadValidAccounts() throws Exception {
    final var loader = new YamlAccountLoader(testResource("accounts-valid.yaml"));
    final var records = loader.load();

    assertEquals(2, records.size());

    final var first = records.get(0);
    assertEquals(1L, first.accountId());
    assertEquals(0L, first.parentAccountId());
    assertEquals("TEST-001", first.accountCode());
    assertEquals("Internal", first.acctIdSource());
    assertEquals("Test Account One", first.accountName());
    assertEquals("Client", first.accountType());
    assertEquals("USD", first.baseCurrency());
    assertEquals("Active", first.status());
    assertEquals("OK", first.complianceStatus());
    assertEquals(3L, first.capabilities());

    final var second = records.get(1);
    assertEquals(2L, second.accountId());
    assertEquals(1L, second.parentAccountId());
    assertEquals("TEST-002", second.accountCode());
    assertEquals("BIC", second.acctIdSource());
    assertEquals("House", second.accountType());
    assertEquals("EUR", second.baseCurrency());
    assertEquals("Suspended", second.status());
    assertEquals("PendingReview", second.complianceStatus());
    assertEquals(1L, second.capabilities());
  }

  @Test
  void loadEmptyFile() throws Exception {
    final var loader = new YamlAccountLoader(testResource("accounts-empty.yaml"));
    final var records = loader.load();

    assertTrue(records.isEmpty());
  }

  @Test
  void loadMalformedYamlThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-malformed.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("malformed YAML"));
  }

  @Test
  void loadDuplicateAccountCodeThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-duplicate.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("duplicate accountCode"));
    assertTrue(ex.getMessage().contains("DUPE-CODE"));
  }

  @Test
  void loadMissingFileThrows() {
    final var loader = new YamlAccountLoader(Path.of("/nonexistent/accounts.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("cannot read"));
  }

  @Test
  void loadDuplicateAccountIdThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-duplicate-id.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("duplicate accountId"));
  }

  @Test
  void loadZeroAccountIdThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-zero-id.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("accountId must be > 0"));
  }

  @Test
  void loadBlankAccountCodeThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-blank-code.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("must not be blank"));
  }

  @Test
  void loadMissingAccountIdThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-missing-id.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("accountId"));
  }

  @Test
  void loadMissingRequiredFieldThrows() {
    final var loader = new YamlAccountLoader(testResource("accounts-missing-field.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Account", ex.entityType());
    assertTrue(ex.getMessage().contains("accountType"));
  }

  @Test
  void sourceNameReturnsFileName() {
    final var loader = new YamlAccountLoader(Path.of("/some/path/accounts.yaml"));
    assertEquals("accounts.yaml", loader.sourceName());
  }

  @Test
  void loadDefaultsOptionalFields() throws Exception {
    final var loader = new YamlAccountLoader(testResource("accounts-defaults.yaml"));
    final var records = loader.load();

    assertEquals(1, records.size());
    final var record = records.get(0);
    assertEquals("Internal", record.acctIdSource());
    assertEquals("Active", record.status());
    assertEquals("OK", record.complianceStatus());
    assertEquals(0L, record.parentAccountId());
    assertEquals(0L, record.capabilities());
  }
}
