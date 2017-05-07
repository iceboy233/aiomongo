from typing import Any, BinaryIO, List, Union

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import ConfigurationError

from .grid_file import GridIn, GridOut


class GridFS:
    def __init__(self, database: 'aiomongo.Database', collection: str = 'fs'):
        if not database.write_concern.acknowledged:
            raise ConfigurationError('database must use '
                                     'acknowledged write_concern')

        self.__database = database
        self.__collection = database[collection]
        self.__files = self.__collection.files
        self.__chunks = self.__collection.chunks

    async def put(self, data: Union[bytes, BinaryIO], **kwargs) -> GridIn:
        """Put data in GridFS as a new file.

        Equivalent to doing::

          try:
              f = new_file(**kwargs)
              f.write(data)
          finally:
              f.close()

        `data` can be either an instance of :class:`str` (:class:`bytes`
        in python 3) or a file-like object providing a :meth:`read` method.
        If an `encoding` keyword argument is passed, `data` can also be a
        :class:`unicode` (:class:`str` in python 3) instance, which will
        be encoded as `encoding` before being written. Any keyword arguments
        will be passed through to the created file - see
        :meth:`~gridfs.grid_file.GridIn` for possible arguments. Returns the
        ``"_id"`` of the created file.

        If the ``"_id"`` of the file is manually specified, it must
        not already exist in GridFS. Otherwise
        :class:`~gridfs.errors.FileExists` is raised.

        :Parameters:
          - `data`: data to be written as a file.
          - `**kwargs` (optional): keyword arguments for file creation
        """
        grid_file = GridIn(self.__collection, **kwargs)
        try:
            grid_file.write(data)
        finally:
            await grid_file.close()

        return grid_file._id

    async def get(self, file_id: Any) -> GridOut:
        """Get a file from GridFS by ``"_id"``.

        Returns an instance of :class:`~gridfs.grid_file.GridOut`,
        which provides a file-like interface for reading.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to get
        """
        gout = GridOut(self.__collection, file_id)

        # Raise NoFile now, instead of on first attribute access.
        await gout._ensure_file()
        return gout

    async def get_version(self, filename=None, version=-1, **kwargs):
        """Get a file from GridFS by ``"filename"`` or metadata fields.

        Returns a version of the file in GridFS whose filename matches
        `filename` and whose metadata fields match the supplied keyword
        arguments, as an instance of :class:`~gridfs.grid_file.GridOut`.

        Version numbering is a convenience atop the GridFS API provided
        by MongoDB. If more than one file matches the query (either by
        `filename` alone, by metadata fields, or by a combination of
        both), then version ``-1`` will be the most recently uploaded
        matching file, ``-2`` the second most recently
        uploaded, etc. Version ``0`` will be the first version
        uploaded, ``1`` the second version, etc. So if three versions
        have been uploaded, then version ``0`` is the same as version
        ``-3``, version ``1`` is the same as version ``-2``, and
        version ``2`` is the same as version ``-1``.

        Raises :class:`~gridfs.errors.NoFile` if no such version of
        that file exists.

        :Parameters:
          - `filename`: ``"filename"`` of the file to get, or `None`
          - `version` (optional): version of the file to get (defaults
            to -1, the most recent version uploaded)
          - `**kwargs` (optional): find files by custom metadata.

        .. versionchanged:: 3.1
           ``get_version`` no longer ensures indexes.
        """
        query = kwargs
        if filename is not None:
            query['filename'] = filename

        cursor = self.__files.find(query)
        if version < 0:
            skip = abs(version) - 1
            cursor.limit(-1).skip(skip).sort('uploadDate', DESCENDING)
        else:
            cursor.limit(-1).skip(version).sort('uploadDate', ASCENDING)
        try:
            grid_file = next(cursor)
            return GridOut(self.__collection, file_document=grid_file)
        except StopIteration:
            raise NoFile('no version %d for filename %r' % (version, filename))

    async def get_last_version(self, filename=None, **kwargs):
        """Get the most recent version of a file in GridFS by ``"filename"``
        or metadata fields.

        Equivalent to calling :meth:`get_version` with the default
        `version` (``-1``).

        :Parameters:
          - `filename`: ``"filename"`` of the file to get, or `None`
          - `**kwargs` (optional): find files by custom metadata.
        """
        return self.get_version(filename=filename, **kwargs)

    async def delete(self, file_id: Any) -> None:
        """Delete a file from GridFS by ``"_id"``.

        Deletes all data belonging to the file with ``"_id"``:
        `file_id`.

        .. warning:: Any processes/threads reading from the file while
           this method is executing will likely see an invalid/corrupt
           file. Care should be taken to avoid concurrent reads to a file
           while it is being deleted.

        .. note:: Deletes of non-existent files are considered successful
           since the end result is the same: no file with that _id remains.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to delete
        """
        await self.__files.delete_one({'_id': file_id})
        await self.__chunks.delete_many({'files_id': file_id})

    async def list(self) -> List:
        """List the names of all files stored in this instance of
        :class:`GridFS`.
        """
        # With an index, distinct includes documents with no filename
        # as None.
        return [
            name for name in await self.__files.distinct('filename')
            if name is not None]
