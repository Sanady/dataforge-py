"""FoodProvider — generates fake food-related data.

Includes dish names, cuisines, ingredients, restaurant names,
dietary tags, beverages, and cooking methods.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

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
    "Croissant",
    "Shakshuka",
    "Butter Chicken",
    "Risotto",
    "Lasagna",
    "Gyoza",
    "Empanada",
    "Poutine",
    "Borscht",
    "Naan Bread",
    "Kimchi Fried Rice",
    "Churros",
    "Baklava",
    "Tiramisu",
    "Creme Brulee",
    "Banoffee Pie",
    "Eggs Benedict",
    "Club Sandwich",
    "Lobster Bisque",
    "Beef Stroganoff",
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
    "British",
    "German",
    "Russian",
    "Australian",
    "Filipino",
    "Malaysian",
    "Portuguese",
    "Argentine",
    "Egyptian",
    "Scandinavian",
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
    "Sugar",
    "Salt",
    "Pepper",
    "Lemon",
    "Lime",
    "Avocado",
    "Mushroom",
    "Bell Pepper",
    "Spinach",
    "Broccoli",
    "Coconut Milk",
    "Soy Sauce",
    "Cumin",
    "Paprika",
    "Cinnamon",
    "Oregano",
    "Thyme",
    "Rosemary",
    "Parsley",
    "Chili Flakes",
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
    "Urban",
    "Coastal",
    "Mountain",
    "Garden",
    "Sunset",
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
    "Terrace",
    "Harbor",
    "Cellar",
    "Palace",
    "Pantry",
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
    "Raw",
    "Plant-Based",
    "Lactose-Free",
    "Soy-Free",
    "Low-Sodium",
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
    "Chai Latte",
    "Fresh Juice",
    "Agua Fresca",
    "Mojito",
    "Sangria",
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
    "Sous Vide",
    "Gratin",
    "Flambeed",
    "Pickled",
    "Fermented",
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

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

    def _one_restaurant(self) -> str:
        """Generate a single restaurant name."""
        choice = self._engine._rng.choice
        return f"The {choice(_RESTAURANT_ADJECTIVES)} {choice(_RESTAURANT_NOUNS)}"

    def _one_meal_price(self) -> str:
        """Generate a single meal price string."""
        ri = self._engine.random_int
        dollars = ri(5, 75)
        cents = ri(0, 99)
        return f"${dollars}.{cents:02d}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def dish(self) -> str: ...
    @overload
    def dish(self, count: Literal[1]) -> str: ...
    @overload
    def dish(self, count: int) -> str | list[str]: ...
    def dish(self, count: int = 1) -> str | list[str]:
        """Generate a dish name (e.g. ``"Pad Thai"``).

        Parameters
        ----------
        count : int
            Number of dish names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_DISHES)
        return self._engine.choices(_DISHES, count)

    @overload
    def cuisine(self) -> str: ...
    @overload
    def cuisine(self, count: Literal[1]) -> str: ...
    @overload
    def cuisine(self, count: int) -> str | list[str]: ...
    def cuisine(self, count: int = 1) -> str | list[str]:
        """Generate a cuisine type (e.g. ``"Italian"``).

        Parameters
        ----------
        count : int
            Number of cuisine types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CUISINES)
        return self._engine.choices(_CUISINES, count)

    @overload
    def ingredient(self) -> str: ...
    @overload
    def ingredient(self, count: Literal[1]) -> str: ...
    @overload
    def ingredient(self, count: int) -> str | list[str]: ...
    def ingredient(self, count: int = 1) -> str | list[str]:
        """Generate an ingredient name (e.g. ``"Garlic"``).

        Parameters
        ----------
        count : int
            Number of ingredients to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_INGREDIENTS)
        return self._engine.choices(_INGREDIENTS, count)

    @overload
    def restaurant(self) -> str: ...
    @overload
    def restaurant(self, count: Literal[1]) -> str: ...
    @overload
    def restaurant(self, count: int) -> str | list[str]: ...
    def restaurant(self, count: int = 1) -> str | list[str]:
        """Generate a restaurant name (e.g. ``"The Golden Kitchen"``).

        Parameters
        ----------
        count : int
            Number of restaurant names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_restaurant()
        return [self._one_restaurant() for _ in range(count)]

    @overload
    def dietary_tag(self) -> str: ...
    @overload
    def dietary_tag(self, count: Literal[1]) -> str: ...
    @overload
    def dietary_tag(self, count: int) -> str | list[str]: ...
    def dietary_tag(self, count: int = 1) -> str | list[str]:
        """Generate a dietary tag (e.g. ``"Vegan"``).

        Parameters
        ----------
        count : int
            Number of dietary tags to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_DIETARY_TAGS)
        return self._engine.choices(_DIETARY_TAGS, count)

    @overload
    def beverage(self) -> str: ...
    @overload
    def beverage(self, count: Literal[1]) -> str: ...
    @overload
    def beverage(self, count: int) -> str | list[str]: ...
    def beverage(self, count: int = 1) -> str | list[str]:
        """Generate a beverage name (e.g. ``"Cappuccino"``).

        Parameters
        ----------
        count : int
            Number of beverages to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_BEVERAGES)
        return self._engine.choices(_BEVERAGES, count)

    @overload
    def cooking_method(self) -> str: ...
    @overload
    def cooking_method(self, count: Literal[1]) -> str: ...
    @overload
    def cooking_method(self, count: int) -> str | list[str]: ...
    def cooking_method(self, count: int = 1) -> str | list[str]:
        """Generate a cooking method (e.g. ``"Grilled"``).

        Parameters
        ----------
        count : int
            Number of cooking methods to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_COOKING_METHODS)
        return self._engine.choices(_COOKING_METHODS, count)

    @overload
    def meal_price(self) -> str: ...
    @overload
    def meal_price(self, count: Literal[1]) -> str: ...
    @overload
    def meal_price(self, count: int) -> str | list[str]: ...
    def meal_price(self, count: int = 1) -> str | list[str]:
        """Generate a meal price (e.g. ``"$24.99"``).

        Parameters
        ----------
        count : int
            Number of meal prices to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_meal_price()
        return [self._one_meal_price() for _ in range(count)]
