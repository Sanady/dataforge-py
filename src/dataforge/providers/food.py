"""FoodProvider — generates fake food-related data.

Includes dish names, cuisines, ingredients, restaurant names,
dietary tags, beverages, and cooking methods.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_DISHES: tuple[str, ...] = (
    "Spaghetti Carbonara",
    "Chicken Tikka Masala",
    "Sushi Roll",
    "Pad Thai",
    "Beef Bourguignon",
    "Fish and Chips",
    "Caesar Salad",
    "Margherita Pizza",
    "Tacos al Pastor",
    "Pho",
    "Ramen",
    "Biryani",
    "Moussaka",
    "Falafel",
    "Paella",
    "Dim Sum",
    "Goulash",
    "Ceviche",
    "Beef Wellington",
    "Tom Yum Soup",
)

_CUISINES: tuple[str, ...] = (
    "Italian",
    "Chinese",
    "Mexican",
    "Japanese",
    "Indian",
    "French",
    "Thai",
    "Greek",
    "Spanish",
    "Korean",
    "Vietnamese",
    "Turkish",
    "Lebanese",
    "Ethiopian",
    "Brazilian",
    "Peruvian",
    "Moroccan",
    "Indonesian",
    "Caribbean",
    "American",
)

_INGREDIENTS: tuple[str, ...] = (
    "Chicken",
    "Beef",
    "Pork",
    "Salmon",
    "Shrimp",
    "Tofu",
    "Rice",
    "Pasta",
    "Potato",
    "Tomato",
    "Onion",
    "Garlic",
    "Ginger",
    "Basil",
    "Cilantro",
    "Olive Oil",
    "Butter",
    "Cheese",
    "Egg",
    "Flour",
)

_RESTAURANT_ADJECTIVES: tuple[str, ...] = (
    "Golden",
    "Royal",
    "Blue",
    "Green",
    "Silver",
    "Red",
    "Grand",
    "Little",
    "Old",
    "New",
    "Lucky",
    "Happy",
    "Secret",
    "Hidden",
    "Rustic",
)

_RESTAURANT_NOUNS: tuple[str, ...] = (
    "Kitchen",
    "Bistro",
    "Grill",
    "Cafe",
    "Tavern",
    "Diner",
    "House",
    "Table",
    "Oven",
    "Spoon",
    "Fork",
    "Plate",
    "Garden",
    "Market",
    "Corner",
)

_DIETARY_TAGS: tuple[str, ...] = (
    "Vegetarian",
    "Vegan",
    "Gluten-Free",
    "Dairy-Free",
    "Nut-Free",
    "Keto",
    "Paleo",
    "Low-Carb",
    "Organic",
    "Halal",
    "Kosher",
    "Pescatarian",
    "Sugar-Free",
    "Whole30",
    "Mediterranean",
)

_BEVERAGES: tuple[str, ...] = (
    "Coffee",
    "Green Tea",
    "Black Tea",
    "Orange Juice",
    "Lemonade",
    "Smoothie",
    "Milkshake",
    "Espresso",
    "Cappuccino",
    "Latte",
    "Hot Chocolate",
    "Iced Tea",
    "Sparkling Water",
    "Kombucha",
    "Matcha",
)

_COOKING_METHODS: tuple[str, ...] = (
    "Grilled",
    "Baked",
    "Fried",
    "Steamed",
    "Roasted",
    "Sauteed",
    "Braised",
    "Poached",
    "Smoked",
    "Stir-Fried",
    "Broiled",
    "Blanched",
    "Deep-Fried",
    "Pan-Seared",
    "Slow-Cooked",
)


class FoodProvider(BaseProvider):
    """Generates fake food-related data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "food"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "dish": "dish",
        "dish_name": "dish",
        "cuisine": "cuisine",
        "cuisine_type": "cuisine",
        "ingredient": "ingredient",
        "restaurant": "restaurant",
        "restaurant_name": "restaurant",
        "dietary_tag": "dietary_tag",
        "diet": "dietary_tag",
        "beverage": "beverage",
        "drink": "beverage",
        "cooking_method": "cooking_method",
        "meal_price": "meal_price",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "dish": _DISHES,
        "cuisine": _CUISINES,
        "ingredient": _INGREDIENTS,
        "dietary_tag": _DIETARY_TAGS,
        "beverage": _BEVERAGES,
        "cooking_method": _COOKING_METHODS,
    }

    # Scalar helpers

    def _one_restaurant(self) -> str:
        choice = self._engine._rng.choice
        return f"The {choice(_RESTAURANT_ADJECTIVES)} {choice(_RESTAURANT_NOUNS)}"

    def _one_meal_price(self) -> str:
        ri = self._engine.random_int
        dollars = ri(5, 75)
        cents = ri(0, 99)
        return f"${dollars}.{cents:02d}"

    # Public API — custom methods

    def restaurant(self, count: int = 1) -> str | list[str]:
        """Generate a restaurant name (e.g. ``"The Golden Kitchen"``)."""
        if count == 1:
            return self._one_restaurant()
        return [self._one_restaurant() for _ in range(count)]

    def meal_price(self, count: int = 1) -> str | list[str]:
        """Generate a meal price (e.g. ``"$24.99"``)."""
        if count == 1:
            return self._one_meal_price()
        return [self._one_meal_price() for _ in range(count)]
