import re


def string_match_pattern(string: str, list_patterns: list[str]) -> bool:
    """
    Validates that a string does not match any forbidden patterns.
    
    Args:
        string: Input string to validate
        list_patterns: List of regex patterns to check against
    
    Returns:
        bool: True if string passes validation, False otherwise
    
    Raises:
        ValueError: If inputs are invalid
        re.error: If regex patterns are malformed
    """
    if not isinstance(string, str) or not isinstance(list_patterns, list):
        raise ValueError(f"Invalid input types")
    
    for pattern in list_patterns:
        if re.search(pattern, string):
            return False
    return True
