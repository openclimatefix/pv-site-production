import importlib
from typing import Any


def import_from_module(module_path: str) -> Any:
    """
    `func = import_from_module('some.module.func')`

    is equivalent to

    `from some.module import func`
    -------
    """
    module, obj_name = module_path.rsplit(".", maxsplit=1)
    return getattr(importlib.import_module(module), obj_name)


def instantiate(cls, args=None, kwargs=None) -> Any:
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    class_ = import_from_module(cls)
    return class_(*args, **kwargs)
