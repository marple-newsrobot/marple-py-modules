# encoding: utf-8

import re
import sys 
from datetime import datetime

VERSION_FILE = "CURRENT_VERSION.txt"
CHANGELOG_FILE = "CHANGES.txt"

def is_valid_version(version):
    """ Check if a string is a valid version 
    """
    return bool(re.search("[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}", version))

def validate(version):
    """ Check if a version is valid, thro error if not
    """
    if is_valid_version(version):
        return version
    else:
        msg = "{} is not a valid version".format(version)
        raise ValueError(msg)

def get_current_version():
    """ Get the current version from file
    """
    with open(VERSION_FILE) as f:
        current_version = f.read().strip()
        validate(current_version)
        return current_version

def _increment_version(version):
    """ Increment version 0.0.1 => 0.0.2
    """
    validate(version)
    parts = version.split(".")
    last_digit = int(parts[-1])
    last_digit += 1
    incremented_version = ".".join(parts[:-1] + [str(last_digit)])
    return incremented_version

def main():
    """
    current_version = get_current_version()
    suggested_version = _increment_version(current_version)

    # Ask user for version
    new_version = raw_input('New version (currently {}) [{}]: '\
        .format(current_version, suggested_version))
    new_version = new_version or suggested_version
    validate(new_version)

    # Ask user for message
    msg = raw_input("Describe version changes (will be added to {}): "\
        .format(CHANGELOG_FILE))
    """
    if len(sys.argv) < 3:
        raise ValueError("Must provide version and message as arguments.")


    new_version = sys.argv[1]
    validate(new_version)
    msg = sys.argv[2]
    
    date = datetime.now().strftime("%Y-%m-%d")
    
    # Update changelog
    # Format: v<version>, <date> -- Initial release.
    change_log_row = "v{}, {} -- {}".format(new_version, date, msg)
    with open(CHANGELOG_FILE, 'a') as file:
        file.write(change_log_row + "\n")
        print u"{} updated: '{}'".format(CHANGELOG_FILE, change_log_row)

    # Update CURRENT_VERSION
    with open(VERSION_FILE, 'r+') as file:
        file.write(new_version)


if __name__ == '__main__':
    main() 