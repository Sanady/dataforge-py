"""Stdlib-only unittest suite for dataforge.backend.RandomEngine."""

import re
import string
import unittest
from unittest import mock

from dataforge import backend as backend_module
from dataforge.backend import RandomEngine

DIGITS = set(string.digits)
LETTERS_UPPER = set(string.ascii_uppercase)
LETTERS_ALL = set(string.ascii_letters)


class TestNumerify(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_all_hash_fast_path_produces_exact_digits(self):
        result = self.engine.numerify("#####")
        self.assertEqual(len(result), 5)
        self.assertTrue(set(result) <= DIGITS)

    def test_no_hash_returns_pattern_unchanged(self):
        self.assertEqual(self.engine.numerify("ABC-123"), "ABC-123")

    def test_empty_string_quirk_returns_zero(self):
        # QUIRK: for "", hash_count == len(pattern) is 0 == 0 (True), so the
        # empty pattern routes to random_digits_str(0) -> "0". This test pins
        # the actual behavior so any change is a deliberate, reviewed decision.
        self.assertEqual(self.engine.numerify(""), "0")

    def test_mixed_replaces_only_hash_positions(self):
        result = self.engine.numerify("A#B##")
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0], "A")
        self.assertEqual(result[2], "B")
        self.assertTrue(
            result[1] in DIGITS and result[3] in DIGITS and result[4] in DIGITS
        )

    def test_determinism_same_seed(self):
        a = RandomEngine(seed=7).numerify("##-##")
        b = RandomEngine(seed=7).numerify("##-##")
        self.assertEqual(a, b)


class TestLetterify(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_no_question_marks_identity(self):
        self.assertEqual(self.engine.letterify("XYZ"), "XYZ")

    def test_default_pool_includes_mixed_case(self):
        result = self.engine.letterify("?" * 200)
        self.assertTrue(set(result) <= LETTERS_ALL)
        self.assertTrue(any(c.isupper() for c in result))
        self.assertTrue(any(c.islower() for c in result))

    def test_upper_flag_restricts_to_uppercase(self):
        result = self.engine.letterify("?" * 100, upper=True)
        self.assertTrue(set(result) <= LETTERS_UPPER)

    def test_preserves_literal_positions(self):
        result = self.engine.letterify("?-?")
        self.assertEqual(result[1], "-")
        self.assertEqual(len(result), 3)


class TestBothify(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_no_tokens_identity(self):
        self.assertEqual(self.engine.bothify("plain"), "plain")

    def test_hashes_only(self):
        result = self.engine.bothify("##")
        self.assertTrue(set(result) <= DIGITS)

    def test_questions_only(self):
        result = self.engine.bothify("??")
        self.assertTrue(set(result) <= LETTERS_ALL)

    def test_combined_replaces_respective_tokens(self):
        result = self.engine.bothify("#?-?#")
        self.assertEqual(result[2], "-")
        self.assertTrue(result[0] in DIGITS and result[4] in DIGITS)
        self.assertTrue(result[1] in LETTERS_ALL and result[3] in LETTERS_ALL)


class TestRandomDigitsStr(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_n_zero_quirk_returns_zero(self):
        # QUIRK: n=0 takes the n<=18 branch; str(0).zfill(0) returns '0'
        # (zfill never truncates). Pin actual behavior.
        self.assertEqual(self.engine.random_digits_str(0), "0")

    def test_n_one_single_digit(self):
        self.assertEqual(len(self.engine.random_digits_str(1)), 1)

    def test_boundary_18_exact_length(self):
        result = self.engine.random_digits_str(18)
        self.assertEqual(len(result), 18)
        self.assertTrue(set(result) <= DIGITS)

    def test_boundary_19_enters_chunk_loop(self):
        result = self.engine.random_digits_str(19)
        self.assertEqual(len(result), 19)
        self.assertTrue(set(result) <= DIGITS)

    def test_large_n_multiple_chunks(self):
        result = self.engine.random_digits_str(50)
        self.assertEqual(len(result), 50)
        self.assertTrue(set(result) <= DIGITS)


class TestWeightedCache(unittest.TestCase):
    def setUp(self) -> None:
        backend_module._CUM_WEIGHTS_CACHE.clear()
        self.engine = RandomEngine(seed=42)

    def tearDown(self) -> None:
        backend_module._CUM_WEIGHTS_CACHE.clear()

    def test_miss_populates_cache_with_cumulative_sums(self):
        weights = (0.1, 0.6, 0.3)
        self.assertNotIn(id(weights), backend_module._CUM_WEIGHTS_CACHE)
        self.engine.weighted_choices(("a", "b", "c"), weights, 5)
        cum = backend_module._CUM_WEIGHTS_CACHE[id(weights)]
        self.assertEqual(len(cum), 3)
        self.assertAlmostEqual(cum[0], 0.1)
        self.assertAlmostEqual(cum[-1], 1.0)
        self.assertLessEqual(cum[0], cum[1])
        self.assertLessEqual(cum[1], cum[2])

    def test_hit_reuses_cached_entry(self):
        weights = (1.0, 2.0)
        self.engine.weighted_choices(("x", "y"), weights, 3)
        first_entry = backend_module._CUM_WEIGHTS_CACHE[id(weights)]
        self.engine.weighted_choices(("x", "y"), weights, 3)
        self.assertIs(backend_module._CUM_WEIGHTS_CACHE[id(weights)], first_entry)

    def test_heavy_first_weight_biases_output(self):
        weights = (0.99, 0.005, 0.005)
        result = self.engine.weighted_choices(("a", "b", "c"), weights, 2000)
        self.assertGreater(result.count("a"), 1800)

    def test_weighted_choice_returns_scalar_from_data(self):
        result = self.engine.weighted_choice(("only",), (1.0,))
        self.assertEqual(result, "only")


class TestDistributions(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_random_int_inclusive_bounds(self):
        for _ in range(200):
            v = self.engine.random_int(5, 7)
            self.assertTrue(5 <= v <= 7)
        self.assertIsInstance(v, int)

    def test_random_float_bounds_and_precision(self):
        v = self.engine.random_float(1.0, 2.0, precision=3)
        self.assertTrue(1.0 <= v <= 2.0)
        self.assertEqual(round(v, 3), v)

    def test_random_float_zero_precision_is_integral(self):
        v = self.engine.random_float(0.0, 100.0, precision=0)
        self.assertEqual(v, round(v, 0))

    def test_gauss_int_clamps_to_range(self):
        for _ in range(500):
            v = self.engine.gauss_int(mu=5.0, sigma=1000.0, min_val=0, max_val=10)
            self.assertTrue(0 <= v <= 10)
            self.assertIsInstance(v, int)

    def test_triangular_default_mode_within_bounds(self):
        v = self.engine.triangular(0.0, 10.0)
        self.assertTrue(0.0 <= v <= 10.0)

    def test_triangular_explicit_mode(self):
        v = self.engine.triangular(0.0, 10.0, mode=9.0)
        self.assertTrue(0.0 <= v <= 10.0)

    def test_exponential_and_gamma_positive(self):
        self.assertGreaterEqual(self.engine.exponential(1.0), 0.0)
        self.assertGreater(self.engine.gamma(2.0, 2.0), 0.0)

    def test_log_normal_pareto_beta_vonmises_types(self):
        self.assertGreater(self.engine.log_normal(0.0, 1.0), 0.0)
        self.assertGreaterEqual(self.engine.pareto(1.0), 1.0)
        beta_v = self.engine.beta(2.0, 5.0)
        self.assertTrue(0.0 <= beta_v <= 1.0)
        self.assertIsInstance(self.engine.vonmises(0.0, 1.0), float)

    def test_zipf_within_support(self):
        for _ in range(100):
            v = self.engine.zipf(alpha=1.5, n=10)
            self.assertTrue(1 <= v <= 10)
            self.assertIsInstance(v, int)

    def test_getrandbits_bit_length(self):
        v = self.engine.getrandbits(8)
        self.assertTrue(0 <= v < 256)

    def test_sample_uniqueness(self):
        data = tuple(range(100))
        result = self.engine.sample(data, 25)
        self.assertEqual(len(result), 25)
        self.assertEqual(len(set(result)), 25)
        self.assertTrue(set(result) <= set(data))


class TestRegexify(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_digit_escape_class(self):
        result = self.engine.regexify(r"\d\d\d")
        self.assertTrue(set(result) <= DIGITS)
        self.assertEqual(len(result), 3)

    def test_word_escape_class(self):
        allowed = LETTERS_ALL | DIGITS | {"_"}
        result = self.engine.regexify(r"\w" * 10)
        self.assertTrue(set(result) <= allowed)

    def test_space_escape_class(self):
        self.assertIn(self.engine.regexify(r"\s"), (" ", "\t"))

    def test_unknown_escape_quirk_double_emits(self):
        # QUIRK: the else-branch appends esc, then _regexify_quantifier (with
        # no following quantifier) appends base again -> double emission.
        self.assertEqual(self.engine.regexify(r"\n"), "nn")

    def test_trailing_backslash_emitted_literally(self):
        self.assertEqual(self.engine.regexify("a\\"), "a\\")

    def test_char_class_range(self):
        for _ in range(50):
            self.assertIn(self.engine.regexify("[a-c]"), "abc")

    def test_char_class_explicit_set(self):
        for _ in range(50):
            self.assertIn(self.engine.regexify("[xyz]"), "xyz")

    def test_unterminated_char_class_literal(self):
        self.assertEqual(self.engine.regexify("[abc"), "[abc")

    def test_group_alternation(self):
        for _ in range(50):
            self.assertIn(self.engine.regexify("(cat|dog)"), ("cat", "dog"))

    def test_unterminated_group_literal(self):
        self.assertEqual(self.engine.regexify("(ab"), "(ab")

    def test_group_with_count_quantifier(self):
        result = self.engine.regexify("(ab){3}")
        self.assertEqual(result, "ababab")

    def test_group_with_range_quantifier(self):
        result = self.engine.regexify("(ab){2,4}")
        self.assertEqual(len(result) % 2, 0)
        self.assertTrue(4 <= len(result) <= 8)
        self.assertTrue(set(result) <= {"a", "b"})

    def test_group_open_range_quantifier(self):
        self.assertEqual(self.engine.regexify("(ab){2,}"), "abab")

    def test_group_plus_quantifier(self):
        result = self.engine.regexify("(ab)+")
        self.assertTrue(2 <= len(result) <= 6)
        self.assertEqual(len(result) % 2, 0)

    def test_group_star_quantifier_allows_zero(self):
        result = self.engine.regexify("(ab)*")
        self.assertTrue(0 <= len(result) <= 6)
        self.assertEqual(len(result) % 2, 0)

    def test_group_question_quantifier(self):
        result = self.engine.regexify("(ab)?")
        self.assertIn(result, ("", "ab"))

    def test_group_with_unclosed_brace_treats_brace_literally(self):
        # '{3' has no closing '}': the group emits its chosen option, then the
        # unclosed '{' falls to the default literal path for '{' and '3'.
        self.assertEqual(self.engine.regexify("(ab){3"), "ab{3")

    def test_dot_wildcard_printable_ascii(self):
        result = self.engine.regexify("....")
        self.assertEqual(len(result), 4)
        self.assertTrue(all(33 <= ord(c) <= 126 for c in result))

    def test_literal_with_exact_quantifier(self):
        self.assertEqual(self.engine.regexify("a{4}"), "aaaa")

    def test_literal_with_range_quantifier(self):
        result = self.engine.regexify("b{2,5}")
        self.assertTrue(2 <= len(result) <= 5)
        self.assertEqual(set(result), {"b"})

    def test_literal_plus_and_star(self):
        plus = self.engine.regexify("c+")
        self.assertTrue(1 <= len(plus) <= 3 and set(plus) == {"c"})
        star = self.engine.regexify("d*")
        self.assertTrue(0 <= len(star) <= 3 and set(star) <= {"d"})

    def test_literal_question_optional(self):
        self.assertIn(self.engine.regexify("e?"), ("", "e"))

    def test_literal_question_appends_when_random_high(self):
        # Lines 365-366: '?' literal quantifier, random() > 0.5 -> append base.
        with mock.patch.object(self.engine._rng, "random", return_value=0.9):
            self.assertEqual(self.engine.regexify("e?"), "e")

    def test_literal_question_skips_when_random_low(self):
        with mock.patch.object(self.engine._rng, "random", return_value=0.1):
            self.assertEqual(self.engine.regexify("e?"), "")

    def test_unclosed_brace_treated_as_literal(self):
        self.assertEqual(self.engine.regexify("a{3"), "a{3")

    def test_quantifier_at_end_appends_base(self):
        self.assertEqual(self.engine.regexify("z"), "z")

    def test_complex_pattern_matches_expectation(self):
        result = self.engine.regexify(r"[A-Z]{2}\d{3}")
        self.assertTrue(re.fullmatch(r"[A-Z]{2}\d{3}", result), msg=result)

    def test_determinism_same_seed(self):
        a = RandomEngine(seed=99).regexify(r"\w{8}")
        b = RandomEngine(seed=99).regexify(r"\w{8}")
        self.assertEqual(a, b)


class TestParseCharClass(unittest.TestCase):
    def test_range_expansion(self):
        self.assertEqual(RandomEngine._parse_char_class("a-c"), "abc")

    def test_mixed_ranges_and_literals(self):
        self.assertEqual(RandomEngine._parse_char_class("a-bX0-1"), "abX01")

    def test_dash_at_end_is_literal(self):
        self.assertEqual(RandomEngine._parse_char_class("a-"), "a-")

    def test_empty_spec_returns_fallback(self):
        self.assertEqual(RandomEngine._parse_char_class(""), "?")


class TestBasicPrimitivesGap(unittest.TestCase):
    """Close uncovered lines 28, 32, 120, 147, 156: choice/choices/seed/gauss."""

    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_choice_returns_member_of_data(self):
        # Line 28: single-element selection via rng.choice
        data = ("alpha", "beta", "gamma")
        for _ in range(50):
            self.assertIn(self.engine.choice(data), data)

    def test_choices_count_and_membership(self):
        # Line 32: batch selection via rng.choices
        data = (1, 2, 3)
        result = self.engine.choices(data, 40)
        self.assertEqual(len(result), 40)
        self.assertTrue(set(result) <= set(data))

    def test_seed_method_resets_state(self):
        # Line 120: explicit seed() produces reproducible stream
        self.engine.seed(123)
        first = self.engine.random_int(0, 10**9)
        self.engine.seed(123)
        self.assertEqual(self.engine.random_int(0, 10**9), first)

    def test_gauss_returns_float(self):
        # Line 156: unclamped gaussian
        v = self.engine.gauss(0.0, 1.0)
        self.assertIsInstance(v, float)

    def test_weighted_choice_cache_hit_skips_recompute(self):
        # Arc 147->152: cum is not None -> skip accumulate, go straight to
        # choices. Pre-populate the cache for this weights id, then call.
        backend_module._CUM_WEIGHTS_CACHE.clear()
        weights = (0.3, 0.7)
        cached = [0.3, 1.0]
        backend_module._CUM_WEIGHTS_CACHE[id(weights)] = cached
        result = self.engine.weighted_choice(("a", "b"), weights)
        self.assertIn(result, ("a", "b"))
        # Same object retained -> hit branch taken, not recomputed
        self.assertIs(backend_module._CUM_WEIGHTS_CACHE[id(weights)], cached)
        backend_module._CUM_WEIGHTS_CACHE.clear()

    def test_weighted_choice_cache_miss_populates(self):
        # Line 147: the cache-miss branch of weighted_choice (distinct from
        # weighted_choices). Fresh weights object guarantees a miss.
        backend_module._CUM_WEIGHTS_CACHE.clear()
        weights = (0.5, 0.5)
        self.assertNotIn(id(weights), backend_module._CUM_WEIGHTS_CACHE)
        self.engine.weighted_choice(("a", "b"), weights)
        self.assertIn(id(weights), backend_module._CUM_WEIGHTS_CACHE)
        backend_module._CUM_WEIGHTS_CACHE.clear()


class TestRegexifyGroupQuantifierGap(unittest.TestCase):
    """Close lines 277, 279-281, 353, 360, 365-367: group-level quantifiers
    that iterate _rng.choice(options) directly (char_pool=None in the group
    handler), plus the '?' group-quantifier random>0.5 append path."""

    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_group_count_quantifier_uses_choice_per_rep(self):
        # Lines 277/353: '(ab|cd){3}' -> for each rep, rng.choice(options)
        result = self.engine.regexify("(ab|cd){3}")
        self.assertEqual(len(result), 6)
        self.assertTrue(set(result) <= {"a", "b", "c", "d"})
        # Each 2-char chunk must be a full option, not mixed letters
        chunks = [result[i : i + 2] for i in range(0, 6, 2)]
        for c in chunks:
            self.assertIn(c, ("ab", "cd"))

    def test_group_plus_quantifier_choice_per_rep(self):
        # Lines 353/360: '+' group with multiple options
        result = self.engine.regexify("(x|y)+")
        self.assertTrue(1 <= len(result) <= 3)
        self.assertTrue(set(result) <= {"x", "y"})

    def test_group_star_quantifier_choice_per_rep(self):
        # Line 360: '*' group quantifier path
        result = self.engine.regexify("(p|q)*")
        self.assertTrue(0 <= len(result) <= 3)
        self.assertTrue(set(result) <= {"p", "q"})

    def test_group_question_appends_when_random_high(self):
        # Lines 279-281: '?' group quantifier with random() > 0.5 -> append.
        # Mock the engine's RNG (a collaborator boundary, not the logic under
        # test) to force the > 0.5 branch deterministically.
        with mock.patch.object(self.engine._rng, "random", return_value=0.9):
            result = self.engine.regexify("(m)?")
        self.assertEqual(result, "m")

    def test_group_question_skips_when_random_low(self):
        # random() <= 0.5 -> skip append branch (arc 279->284).
        with mock.patch.object(self.engine._rng, "random", return_value=0.1):
            result = self.engine.regexify("(m)?")
        self.assertEqual(result, "")

    def test_group_question_both_arcs(self):
        # Drive random() above then below 0.5 across two calls on one engine so
        # coverage registers both the append arc and the skip arc for the '?'
        # group quantifier, closing the partial branch.
        with mock.patch.object(self.engine._rng, "random", side_effect=[0.9, 0.1]):
            high = self.engine.regexify("(m)?")
            low = self.engine.regexify("(m)?")
        self.assertEqual(high, "m")
        self.assertEqual(low, "")

    def test_group_count_quantifier_line277_direct(self):
        # Line 277: the exact `result.append(_rng.choice(options))` inside the
        # group '{n}' repetition. Force randint to a known rep count and choice
        # to a fixed option for a fully deterministic assertion.
        with (
            mock.patch.object(self.engine._rng, "randint", return_value=2),
            mock.patch.object(self.engine._rng, "choice", return_value="ab"),
        ):
            result = self.engine.regexify("(ab){2,5}")
        self.assertEqual(result, "abab")

    def test_group_star_quantifier_line277_branch(self):
        # Line 277 via the '*' group branch (reps in 0..3). Force reps=2 and a
        # fixed option so the append loop body executes deterministically.
        with (
            mock.patch.object(self.engine._rng, "randint", return_value=2),
            mock.patch.object(self.engine._rng, "choice", return_value="zz"),
        ):
            result = self.engine.regexify("(zz)*")
        self.assertEqual(result, "zzzz")


class TestQuantifierCharPoolGap(unittest.TestCase):
    """Close lines 353/360 char_pool branches for class/dot quantifiers."""

    def setUp(self) -> None:
        self.engine = RandomEngine(seed=42)

    def test_char_class_plus_draws_from_pool(self):
        # '[abc]+' -> char_pool set, '+' branch, rng.choice(char_pool)
        result = self.engine.regexify("[abc]+")
        self.assertTrue(1 <= len(result) <= 3)
        self.assertTrue(set(result) <= {"a", "b", "c"})

    def test_char_class_star_draws_from_pool(self):
        result = self.engine.regexify("[01]*")
        self.assertTrue(0 <= len(result) <= 3)
        self.assertTrue(set(result) <= {"0", "1"})

    def test_char_class_count_from_pool(self):
        result = self.engine.regexify("[ab]{4}")
        self.assertEqual(len(result), 4)
        self.assertTrue(set(result) <= {"a", "b"})

    def test_dot_with_count_quantifier(self):
        result = self.engine.regexify(".{5}")
        self.assertEqual(len(result), 5)
        self.assertTrue(all(33 <= ord(c) <= 126 for c in result))


if __name__ == "__main__":
    unittest.main()
