"""
Utils related to imports.
"""

import importlib
from typing import Any


def import_from_module(module_path: str) -> Any:
    """
    Import object from a dot-separated path.

    `func = import_from_module('some.module.func')`

    is equivalent to

    `from some.module import func`
    -------
    """
    module, obj_name = module_path.rsplit(".", maxsplit=1)
    return getattr(importlib.import_module(module), obj_name)


def instantiate(
    cls: str, args: list[Any] | None = None, kwargs: dict[str, Any] | None = None
) -> Any:
    """
    Instantiate a python object from its class name and arguments.

    This is useful to specify python objects in yaml config in a very flexible way.

    DO NOT CALL on content you do not trust as this function can execute arbitrary code.

    Arguments:
    ---------
        cls: The dot-separated path to the class, e.g:
            "pv_site_production.data.nwp_data_source.NwpDataSource".
        args: Positional arguments to pass to the class.
        kwargs: Keyword arguments to passe to the class.

    Returns
    -------
        <cls>(*args, **kwargs)
    """
    if args is None:
        args = []

    if kwargs is None:
        kwargs = {}

    class_ = import_from_module(cls)
    return class_(*args, **kwargs)
