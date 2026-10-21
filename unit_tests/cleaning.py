def standardize_product_id(product_id):
    """Trim whitespace and convert a product ID to uppercase."""
    if product_id is None:
        return None
    return product_id.strip().upper()
