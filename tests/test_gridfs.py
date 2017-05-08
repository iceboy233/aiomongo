import datetime
import pytest

from bson.binary import Binary
from gridfs.errors import CorruptGridFile, NoFile


class TestGridFs:

    @pytest.mark.asyncio
    async def test_basic(self, test_db, test_fs):
        oid = await test_fs.put(b'hello world')
        assert b'hello world' == await (await test_fs.get(oid)).read()
        assert 1 == await test_db.fs.files.count()
        assert 1 == await test_db.fs.chunks.count()

        await test_fs.delete(oid)
        with pytest.raises(NoFile):
            await test_fs.get(oid)
        assert 0 == await test_db.fs.files.count()
        assert 0 == await test_db.fs.chunks.count()

        with pytest.raises(NoFile):
            await test_fs.get('foo')
        oid = await test_fs.put(b'hello world', _id='foo')
        assert 'foo' == oid
        assert b'hello world' == await (await test_fs.get('foo')).read()

    @pytest.mark.asyncio
    async def test_multi_chunk_delete(self, test_db, test_fs):
        assert 0 == await test_db.fs.files.count()
        assert 0 == await test_db.fs.chunks.count()
        oid = await test_fs.put(b'hello', chunkSize=1)
        assert 1 == await test_db.fs.files.count()
        assert 5 == await test_db.fs.chunks.count()
        await test_fs.delete(oid)
        assert 0 == await test_db.fs.files.count()
        assert 0 == await test_db.fs.chunks.count()

    @pytest.mark.asyncio
    async def test_list(self, test_fs):
        assert [] == await test_fs.list()
        await test_fs.put(b'hello world')
        assert [] == await test_fs.list()

        # PYTHON-598: in server versions before 2.5.x, creating an index on
        # filename, uploadDate causes list() to include None.
        await test_fs.get_last_version()
        assert [] == await test_fs.list()

        await test_fs.put(b'', filename='mike')
        await test_fs.put(b'foo', filename='test')
        await test_fs.put(b'', filename='hello world')

        assert {'mike', 'test', 'hello world'} == set(await test_fs.list())

    @pytest.mark.asyncio
    async def test_empty_file(self, test_db, test_fs):
        oid = await test_fs.put(b'')
        assert b'' == await (await test_fs.get(oid)).read()
        assert 1 == await test_db.fs.files.count()
        assert 0 == await test_db.fs.chunks.count()

        raw = await test_db.fs.files.find_one()
        assert 0 == raw['length']
        assert oid == raw['_id']
        assert isinstance(raw['uploadDate'], datetime.datetime)
        assert 255 * 1024 == raw['chunkSize']
        assert isinstance(raw['md5'], str)

    @pytest.mark.asyncio
    async def test_corrupt_chunk(self, test_db, test_fs):
        files_id = await test_fs.put(b'foobar')
        await test_db.fs.chunks.update_one({'files_id': files_id},
                                           {'$set': {'data': Binary(b'foo', 0)}})
        try:
            out = await test_fs.get(files_id)
            with pytest.raises(CorruptGridFile):
                await out.read()

            out = await test_fs.get(files_id)
            with pytest.raises(CorruptGridFile):
                await out.readline()
        finally:
            await test_fs.delete(files_id)

    @pytest.mark.asyncio
    async def test_put_ensures_index(self, test_db, test_fs):
        # setUp has dropped collections.
        names = await test_db.collection_names()
        assert not [name for name in names if name.startswith('fs')]

        chunks = test_db.fs.chunks
        files = test_db.fs.files
        await test_fs.put(b'junk')

        assert any(
            info.get('key') == [('files_id', 1), ('n', 1)]
            for info in (await chunks.index_information()).values())
        assert any(
            info.get('key') == [('filename', 1), ('uploadDate', 1)]
            for info in (await files.index_information()).values())

    @pytest.mark.asyncio
    async def test_get_last_version(self, test_fs):
        one = await test_fs.put(b'foo', filename='test')
        two = test_fs.new_file(filename='test')
        await two.write(b'bar')
        await two.close()
        two = two._id
        three = await test_fs.put(b'baz', filename='test')

        assert b'baz' == await (await test_fs.get_last_version('test')).read()
        await test_fs.delete(three)
        assert b'bar' == await (await test_fs.get_last_version('test')).read()
        await test_fs.delete(two)
        assert b'foo' == await (await test_fs.get_last_version('test')).read()
        await test_fs.delete(one)
        with pytest.raises(NoFile):
            await test_fs.get_last_version('test')

    @pytest.mark.asyncio
    async def test_get_last_version_with_metadata(self, test_fs):
        one = await test_fs.put(b'foo', filename='test', author='author')
        two = await test_fs.put(b'bar', filename='test', author='author')

        assert b'bar' == await (await test_fs.get_last_version(author='author')).read()
        await test_fs.delete(two)
        assert b'foo' == await (await test_fs.get_last_version(author='author')).read()
        await test_fs.delete(one)

        one = await test_fs.put(b'foo', filename='test', author='author1')
        two = await test_fs.put(b'bar', filename='test', author='author2')

        assert b'foo' == await (await test_fs.get_last_version(author='author1')).read()
        assert b'bar' == await (await test_fs.get_last_version(author='author2')).read()
        assert b'bar' == await (await test_fs.get_last_version(filename='test')).read()

        with pytest.raises(NoFile):
            await test_fs.get_last_version(author='author3')
        with pytest.raises(NoFile):
            await test_fs.get_last_version(filename='nottest', author='author1')

        await test_fs.delete(one)
        await test_fs.delete(two)
