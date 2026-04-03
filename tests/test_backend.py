"""Tests for the RandomEngine backend."""

from dataforge.backend import RandomEngine


class TestRandomEngineScalar:
    def test_choice_returns_string(self) -> None:
        engine = RandomEngine(seed=1)
        data = ("a", "b", "c")
        result = engine.choice(data)
        assert isinstance(result, str)
        assert result in data

    def test_random_int_in_range(self) -> None:
        engine = RandomEngine(seed=1)
        for _ in range(100):
            val = engine.random_int(10, 50)
            assert 10 <= val <= 50

    def test_numerify_replaces_hashes(self) -> None:
        engine = RandomEngine(seed=1)
        result = engine.numerify("#####")
        assert len(result) == 5
        assert result.isdigit()

    def test_numerify_preserves_non_hash(self) -> None:
        engine = RandomEngine(seed=1)
        result = engine.numerify("##-##")
        assert len(result) == 5
        assert result[2] == "-"


class TestRandomEngineChoices:
    def test_choices_returns_list(self) -> None:
        engine = RandomEngine(seed=1)
        data = ("a", "b", "c")
        result = engine.choices(data, 10)
        assert isinstance(result, list)
        assert len(result) == 10
        assert all(item in data for item in result)

    def test_choices_large_batch(self) -> None:
        engine = RandomEngine(seed=1)
        data = ("x", "y", "z")
        result = engine.choices(data, 10_000)
        assert len(result) == 10_000
        assert all(item in data for item in result)


class TestRandomEngineSeed:
    def test_same_seed_same_output(self) -> None:
        engine1 = RandomEngine(seed=42)
        engine2 = RandomEngine(seed=42)
        data = ("a", "b", "c", "d", "e")
        results1 = [engine1.choice(data) for _ in range(100)]
        results2 = [engine2.choice(data) for _ in range(100)]
        assert results1 == results2

    def test_different_seed_different_output(self) -> None:
        engine1 = RandomEngine(seed=1)
        engine2 = RandomEngine(seed=2)
        data = ("a", "b", "c", "d", "e", "f", "g", "h")
        results1 = [engine1.choice(data) for _ in range(50)]
        results2 = [engine2.choice(data) for _ in range(50)]
        # Extremely unlikely to be equal with different seeds
        assert results1 != results2

    def test_reseed_resets_state(self) -> None:
        engine = RandomEngine(seed=42)
        data = ("a", "b", "c", "d", "e")
        first_run = [engine.choice(data) for _ in range(20)]
        engine.seed(42)
        second_run = [engine.choice(data) for _ in range(20)]
        assert first_run == second_run


class TestWeightedChoices:
    def test_weighted_choices_returns_list(self) -> None:
        engine = RandomEngine(seed=1)
        data = ("a", "b", "c")
        weights = (1.0, 1.0, 1.0)
        result = engine.weighted_choices(data, weights, 10)
        assert isinstance(result, list)
        assert len(result) == 10
        assert all(item in data for item in result)

    def test_weighted_choices_respects_weights(self) -> None:
        engine = RandomEngine(seed=42)
        data = ("rare", "common")
        # "common" should appear much more frequently
        weights = (0.01, 0.99)
        result = engine.weighted_choices(data, weights, 1000)
        common_count = result.count("common")
        # Should be overwhelmingly "common"
        assert common_count > 900

    def test_weighted_choices_zero_weight(self) -> None:
        engine = RandomEngine(seed=1)
        data = ("never", "always")
        weights = (0.0, 1.0)
        result = engine.weighted_choices(data, weights, 100)
        assert all(item == "always" for item in result)

    def test_weighted_choices_large_batch(self) -> None:
        engine = RandomEngine(seed=1)
        data = ("x", "y", "z")
        weights = (1.0, 2.0, 3.0)
        result = engine.weighted_choices(data, weights, 10_000)
        assert len(result) == 10_000
        assert all(item in data for item in result)

    def test_weighted_choices_deterministic(self) -> None:
        data = ("a", "b", "c")
        weights = (1.0, 2.0, 3.0)
        e1 = RandomEngine(seed=99)
        e2 = RandomEngine(seed=99)
        assert e1.weighted_choices(data, weights, 50) == e2.weighted_choices(
            data, weights, 50
        )
