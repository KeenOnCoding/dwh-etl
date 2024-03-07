import os
import fnmatch


def find_files_with_pattern(pattern, path):
    """
    Traverses all the directories and finds paths to files
    accordingly to pattern supplied
    Args:
        pattern: pattern to match against filename
        path: root path from where to start searching

    Returns: list of found paths to files which name matches the pattern

    """
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result
