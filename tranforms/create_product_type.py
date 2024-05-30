import re

# Define the list of product types to identify
product_types = [
    "Milk", "Bread", "Eggs", "Potatoes", "Bananas",
    "Bacon", "Butter", "Juice", "Biscuits"
]

# Define conflicting keywords for each product type
conflicting_keywords = {
    "Milk": ["chocolate", "almond", "soy", "milkybar", ],
    "Bread": [],
    "Eggs": [],
    "Potatoes": [],
    "Bananas": [],
    "Bacon": [],
    "Butter": [],
    "Juice": [],
    "Biscuits": []
}

def identify_product_type(product_name):
    """
    Identify the product type from the product name using precise matching and exclude conflicting keywords.
    """
    for product in product_types:
        # Use word boundaries to match whole words only
        if re.search(r'\b' + re.escape(product) + r'\b', product_name, re.IGNORECASE):
            # Check for conflicting keywords
            if all(not re.search(r'\b' + re.escape(conflict) + r'\b', product_name, re.IGNORECASE) for conflict in conflicting_keywords.get(product, [])):
                return product
    return "Other"

