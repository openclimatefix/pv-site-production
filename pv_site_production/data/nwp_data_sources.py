import io
from typing import Any

import fsspec
import xarray as xr
from psp.data.data_sources.nwp import NwpDataSource as NwpDataSourceBase


# Our original data source was designed with .zarr files in mind.
# We adapt it to also support .netcdf files, in particular .netcdf files in the cloud.
# TODO Implement this into the original NwpDataSource class instead.
class NwpDataSource(NwpDataSourceBase):
    def __init__(self, path: str, storage_kwargs: dict[str, Any] | None = None):
        self._path = path

        if storage_kwargs is None:
            storage_kwargs = {}

        self._storage_kwargs = storage_kwargs
        self._open()

    def _open(self):
        path = self._path

        if path.endswith(".zarr"):
            return super()._open()
        elif path.endswith(".netcdf"):

            if path.startswith("s3://"):
                # loading netcdf file, download bytes and then load as xarray
                with fsspec.open(path, mode="rb", **self._storage_kwargs) as f:
                    file_bytes = f.read()

                with io.BytesIO(file_bytes) as f:
                    self._data = xr.load_dataset(f, engine="h5netcdf")
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError
