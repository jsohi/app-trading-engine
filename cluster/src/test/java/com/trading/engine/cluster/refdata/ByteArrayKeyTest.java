package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class ByteArrayKeyTest {

  @Test
  void contentBasedHashCodeEqual() {
    final ByteArrayKey a = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    final ByteArrayKey b = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void contentBasedHashCodeDiffers() {
    final ByteArrayKey a = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    final ByteArrayKey b = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'F'});
    assertNotEquals(a, b);
    // Hash codes are not required to differ but FNV-1a should distinguish these.
    assertNotEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void differentLengthsAreNotEqual() {
    final ByteArrayKey a = new ByteArrayKey(new byte[] {'A', 'C', 'M'});
    final ByteArrayKey b = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    assertNotEquals(a, b);
  }

  @Test
  void emptyKey() {
    final ByteArrayKey a = new ByteArrayKey(new byte[0]);
    final ByteArrayKey b = new ByteArrayKey(new byte[0]);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(0, a.length());
  }

  @Test
  void copyOfByteArraySlice() {
    final byte[] src = {'X', 'A', 'C', 'M', 'E', 'Y'};
    final ByteArrayKey k = ByteArrayKey.copyOf(src, 1, 4);
    assertEquals(4, k.length());
    final ByteArrayKey direct = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    assertEquals(direct, k);
  }

  @Test
  void copyOfDirectBufferSlice() {
    final byte[] backing = {'X', 'A', 'C', 'M', 'E', 'Y'};
    final UnsafeBuffer buf = new UnsafeBuffer(backing);
    final ByteArrayKey k = ByteArrayKey.copyOf(buf, 1, 4);
    final ByteArrayKey direct = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    assertEquals(direct, k);
  }

  @Test
  void copyOfRejectsBadOffsetOrLength() {
    assertThrows(NullPointerException.class, () -> ByteArrayKey.copyOf((byte[]) null, 0, 0));
    final byte[] src = {'A', 'B', 'C'};
    assertThrows(IndexOutOfBoundsException.class, () -> ByteArrayKey.copyOf(src, 1, 5));
    assertThrows(IndexOutOfBoundsException.class, () -> ByteArrayKey.copyOf(src, -1, 1));
  }

  @Test
  void emptyForLookupRequiresNonNegativeMaxLength() {
    assertThrows(IllegalArgumentException.class, () -> ByteArrayKey.emptyForLookup(-1));
  }

  @Test
  void reusableLookupProbe() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(16);
    assertEquals(0, probe.length());

    final UnsafeBuffer src1 = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    probe.set(src1, 0, 4);
    assertEquals(4, probe.length());
    final ByteArrayKey acme = new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'});
    assertEquals(acme, probe);
    assertEquals(acme.hashCode(), probe.hashCode());

    final UnsafeBuffer src2 = new UnsafeBuffer(new byte[] {'B', 'I', 'G', 'C', 'O'});
    probe.set(src2, 0, 5);
    assertEquals(5, probe.length());
    final ByteArrayKey bigco = new ByteArrayKey(new byte[] {'B', 'I', 'G', 'C', 'O'});
    assertEquals(bigco, probe);
    assertNotEquals(acme, probe);
  }

  @Test
  void setRejectsLengthExceedingProbeCapacity() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(4);
    final UnsafeBuffer src = new UnsafeBuffer(new byte[] {'A', 'B', 'C', 'D', 'E'});
    assertThrows(IndexOutOfBoundsException.class, () -> probe.set(src, 0, 5));
  }

  @Test
  void worksAsHashMapKey() {
    final Object2ObjectHashMap<ByteArrayKey, String> map = new Object2ObjectHashMap<>();
    map.put(new ByteArrayKey(new byte[] {'A', 'C', 'M', 'E'}), "one");
    map.put(new ByteArrayKey(new byte[] {'B', 'I', 'G', 'C', 'O'}), "two");

    // A reusable probe should look up the right values.
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(16);
    final UnsafeBuffer acme = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    probe.set(acme, 0, 4);
    assertEquals("one", map.get(probe));

    final UnsafeBuffer bigco = new UnsafeBuffer(new byte[] {'B', 'I', 'G', 'C', 'O'});
    probe.set(bigco, 0, 5);
    assertEquals("two", map.get(probe));

    // A code that isn't there returns null.
    final UnsafeBuffer xyz = new UnsafeBuffer(new byte[] {'X', 'Y', 'Z'});
    probe.set(xyz, 0, 3);
    assertNull(map.get(probe));
  }

  @Test
  void equalsContractWithNonByteArrayKey() {
    final ByteArrayKey a = new ByteArrayKey(new byte[] {'A'});
    assertNotEquals(a, "A");
    assertNotEquals(a, null);
    assertEquals(a, a);
  }

  @Test
  void byteArraySetOverloadMatchesDirectBufferSet() {
    final ByteArrayKey probe = ByteArrayKey.emptyForLookup(16);
    final byte[] bytes = {'A', 'C', 'M', 'E'};

    probe.set(bytes, 0, 4);
    final ByteArrayKey acme = new ByteArrayKey(bytes);
    assertEquals(acme, probe);

    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final ByteArrayKey probe2 = ByteArrayKey.emptyForLookup(16);
    probe2.set(buf, 0, 4);
    assertEquals(probe, probe2);
    assertEquals(probe.hashCode(), probe2.hashCode());
  }

  @Test
  void constructorRejectsNull() {
    assertThrows(NullPointerException.class, () -> new ByteArrayKey(null));
  }
}
