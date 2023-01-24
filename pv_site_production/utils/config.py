"""
Utils related to configuration files.
"""

import pathlib
import string
from typing import Any

import yaml


# Inspired from:
# https://dustinoprea.com/2022/04/01/python-substitute-values-into-yaml-during-parse/
def load_config_from_string(config: str, context: dict[str, str] | None = None) -> Any:
    """Same as `load_config` but directly from the YAML string."""
    if context is None:
        context = {}

    def string_constructor(loader, node):
        t = string.Template(node.value)
        value = t.substitute(context)
        return value

    Loader = yaml.SafeLoader
    Loader.add_constructor("tag:yaml.org,2002:str", string_constructor)

    token_re = string.Template.pattern
    Loader.add_implicit_resolver("tag:yaml.org,2002:str", token_re, None)

    return yaml.load(config, Loader=Loader)


def load_config(config: pathlib.Path, context: dict[str, str] | None = None) -> Any:
    """Parse a YAML string config into an object.

    Arguments:
    ---------
    config: The configuration file. The contente of the file can contain placeholders like
        $variable or ${variable}.
    context: A {key: value} dictionary where the keys are the placeholders variables in the
        config and where the values are the values we want to replace them with.

    Return:
    ------
        The configuration object.
    """
    with open(config) as f:
        return load_config_from_string(f, context)
