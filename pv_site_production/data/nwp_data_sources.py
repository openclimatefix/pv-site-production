"""
NWP Data sources
"""

from typing import Any

import fsspec
import xarray as xr
from psp.data.data_sources.nwp import NwpDataSource as NwpDataSourceBase


# Our original data source was designed with .zarr files in mind.
# We adapt it to also support .netcdf files, in particular .netcdf files in the cloud.
# TODO Implement this into the original NwpDataSource class instead.
class NwpDataSource(NwpDataSourceBase):
    """NWP Data source that supports both .zarr and .netcdf locally or in the cloud."""

    def __init__(self, path: str, storage_kwargs: dict[str, Any] | None = None):
        """Constructor"""
        self._path = path

        if storage_kwargs is None:
            storage_kwargs = {}

        self._storage_kwargs = storage_kwargs
        self._open()

        self._cache_dir = None

    def _open(self):
        path = self._path

        if path.endswith(".zarr"):
            return super()._open()
        elif path.endswith(".netcdf"):

            if path.startswith("s3://"):
                # loading netcdf file, download bytes and then load as xarray
                f = fsspec.open(path, mode="rb", **self._storage_kwargs)
                # We can't close the file object because then we won't be able to access it when the
                # model tries to read from it.
                # The alternative would be to open/close in the `.at` and `.at_get` methods (see the
                # parent class).
                f = f.__enter__()
                self._data = xr.open_dataset(f, engine="h5netcdf")
            else:
                self._data = xr.open_dataset(path, engine="h5netcdf")
        else:
            raise NotImplementedError
