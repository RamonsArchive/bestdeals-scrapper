import logging
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_object(current, target, path=None):
    """
    Recursively search through dicts and lists for the first occurrence
    of `target` as a key in a dict.  Returns the corresponding value,
    or None if not found.
    """
    if path is None:
        path = []

    try:
        # If it's a dict, check for target key then recurse into values
        if isinstance(current, dict):
            if target in current:
                logger.debug(f"Found `{target}` at path: {path}. This is current: {current}")
                return current[target]

            for key, value in current.items():
                result = find_object(value, target, path + [f".{key}"])
                if result is not None:
                    return result

        # If it's a list/tuple, recurse into each element
        elif isinstance(current, (list, tuple)):
            for idx, element in enumerate(current):
                result = find_object(element, target, path + [f"[{idx}]"])
                if result is not None:
                    return result

        # anything else — just skip
        else:
            return None

    except RecursionError:
        logger.error(f"Recursion depth exceeded when searching for `{target}` at path: {path}")
    except Exception as e:
        # catch whatever weirdness we didn't expect, log it, and keep going
        logger.warning(f"Error `{e}` at path {path!r} when searching for `{target}`")

    return None


def find_object(current, target, path=None):
    """
    Recursively search through dicts and lists for the first occurrence
    of `target` as a key in a dict.  Returns the corresponding value,
    or None if not found.
    """
    if path is None:
        path = []

    try:
        # If it's a dict, check for target key then recurse into values
        if isinstance(current, dict):
            if target in current:
                logger.debug(f"Found `{target}` at path: {path}. This is current: {current}")
                return current[target]

            for key, value in current.items():
                result = find_object(value, target, path + [f".{key}"])
                if result is not None:
                    return result

        # If it's a list/tuple, recurse into each element
        elif isinstance(current, (list, tuple)):
            for idx, element in enumerate(current):
                result = find_object(element, target, path + [f"[{idx}]"])
                if result is not None:
                    return result

        # anything else — just skip
        else:
            return None

    except RecursionError:
        logger.error(f"Recursion depth exceeded when searching for `{target}` at path: {path}")
    except Exception as e:
        # catch whatever weirdness we didn't expect, log it, and keep going
        logger.warning(f"Error `{e}` at path {path!r} when searching for `{target}`")

    return None