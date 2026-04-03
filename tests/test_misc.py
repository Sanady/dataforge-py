"""Tests for the MiscProvider."""

import re
import uuid

from dataforge import DataForge

_UUID4_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

_UUID7_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class TestMiscScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_uuid4_returns_str(self) -> None:
        result = self.forge.misc.uuid4()
        assert isinstance(result, str)

    def test_uuid4_format(self) -> None:
        for _ in range(50):
            result = self.forge.misc.uuid4()
            assert _UUID4_PATTERN.match(result), f"Bad UUID4: {result}"

    def test_boolean_returns_bool(self) -> None:
        result = self.forge.misc.boolean()
        assert isinstance(result, bool)

    def test_random_element_picks_from_tuple(self) -> None:
        elements = ("apple", "banana", "cherry")
        for _ in range(50):
            result = self.forge.misc.random_element(elements)
            assert result in elements

    def test_null_or_returns_none_or_value(self) -> None:
        seen_none = False
        seen_value = False
        for _ in range(200):
            result = self.forge.misc.null_or("hello", probability=0.3)
            if result is None:
                seen_none = True
            else:
                assert result == "hello"
                seen_value = True
        # With 200 iterations at p=0.3, we should see both outcomes
        assert seen_none, "Expected at least one None"
        assert seen_value, "Expected at least one non-None value"


class TestMiscBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_uuid4_batch(self) -> None:
        result = self.forge.misc.uuid4(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(_UUID4_PATTERN.match(u) for u in result)

    def test_boolean_batch(self) -> None:
        result = self.forge.misc.boolean(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(b, bool) for b in result)


class TestMiscSeed:
    def test_uuid4_reproducible_with_same_seed(self) -> None:
        forge1 = DataForge(locale="en_US", seed=42)
        forge2 = DataForge(locale="en_US", seed=42)
        assert forge1.misc.uuid4() == forge2.misc.uuid4()

    def test_uuid4_batch_reproducible_with_same_seed(self) -> None:
        forge1 = DataForge(locale="en_US", seed=42)
        forge2 = DataForge(locale="en_US", seed=42)
        assert forge1.misc.uuid4(count=10) == forge2.misc.uuid4(count=10)


class TestUUID7Scalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_uuid7_returns_str(self) -> None:
        result = self.forge.misc.uuid7()
        assert isinstance(result, str)

    def test_uuid7_format(self) -> None:
        for _ in range(50):
            result = self.forge.misc.uuid7()
            assert _UUID7_PATTERN.match(result), f"Bad UUID7: {result}"

    def test_uuid7_is_valid_uuid(self) -> None:
        result = self.forge.misc.uuid7()
        parsed = uuid.UUID(result)
        assert parsed.version == 7

    def test_uuid7_time_ordered(self) -> None:
        import time

        u1 = self.forge.misc.uuid7()
        time.sleep(0.002)  # small delay to ensure different timestamp
        u2 = self.forge.misc.uuid7()
        # UUID7 is time-ordered: u1 < u2 (lexicographic on the hex string)
        assert u1 < u2, f"Expected {u1} < {u2} (time-ordered)"


class TestUUID7Batch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_uuid7_batch(self) -> None:
        result = self.forge.misc.uuid7(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(_UUID7_PATTERN.match(u) for u in result)

    def test_uuid7_batch_all_valid(self) -> None:
        result = self.forge.misc.uuid7(count=20)
        for u in result:
            parsed = uuid.UUID(u)
            assert parsed.version == 7


class TestUUID7Seed:
    def test_uuid7_random_bits_reproducible(self) -> None:
        forge1 = DataForge(locale="en_US", seed=42)
        forge2 = DataForge(locale="en_US", seed=42)
        u1 = uuid.UUID(forge1.misc.uuid7())
        u2 = uuid.UUID(forge2.misc.uuid7())
        # Random bits are in the lower 62 bits + 12 bits after version
        # Extract rand_a (bits 52-63) and rand_b (bits 66-127)
        # The timestamp may differ slightly, but random bits must match
        mask_rand_a = 0xFFF  # 12 bits after version nibble
        mask_rand_b = (1 << 62) - 1  # lower 62 bits
        int1 = u1.int
        int2 = u2.int
        rand_a_1 = (int1 >> 64) & mask_rand_a
        rand_a_2 = (int2 >> 64) & mask_rand_a
        rand_b_1 = int1 & mask_rand_b
        rand_b_2 = int2 & mask_rand_b
        assert rand_a_1 == rand_a_2, "rand_a should be reproducible with same seed"
        assert rand_b_1 == rand_b_2, "rand_b should be reproducible with same seed"
