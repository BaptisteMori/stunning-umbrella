import re


def string_matches_any_pattern(string: str, patterns: list[str]) -> bool:
    """
    Check if string matches any of the provided regex patterns.
    
    Args:
        string: Input string to check
        patterns: List of regex patterns
    
    Returns:
        bool: True if string matches any pattern, False otherwise
    """
    if not isinstance(string, str):
        raise ValueError(f"Expected string, got {type(string).__name__}")
    if not isinstance(patterns, list):
        raise ValueError(f"Expected list of patterns, got {type(patterns).__name__}")
    
    for pattern in patterns:
        try:
            if re.search(pattern, string):
                return True
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{pattern}': {e}")
    
    return False