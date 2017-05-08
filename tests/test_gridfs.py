import datetime
import pytest
from io import BytesIO

from bson.binary import Binary
from gridfs.errors import CorruptGridFile, FileExists, NoFile


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
        two = await test_fs.new_file(filename='test')
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

    @pytest.mark.asyncio
    async def test_get_version(self, test_fs):
        await test_fs.put(b'foo', filename='test')
        await test_fs.put(b'bar', filename='test')
        await test_fs.put(b'baz', filename='test')

        assert b'foo' == await (await test_fs.get_version('test', 0)).read()
        assert b'bar' == await (await test_fs.get_version('test', 1)).read()
        assert b'baz' == await (await test_fs.get_version('test', 2)).read()

        assert b'baz' == await (await test_fs.get_version('test', -1)).read()
        assert b'bar' == await (await test_fs.get_version('test', -2)).read()
        assert b'foo' == await (await test_fs.get_version('test', -3)).read()

        with pytest.raises(NoFile):
            await test_fs.get_version('test', 3)
        with pytest.raises(NoFile):
            await test_fs.get_version('test', -4)

    @pytest.mark.asyncio
    async def test_get_version_with_metadata(self, test_fs):
        one = await test_fs.put(b'foo', filename='test', author='author1')
        two = await test_fs.put(b'bar', filename='test', author='author1')
        three = await test_fs.put(b'baz', filename='test', author='author2')

        assert b'foo' == await (await test_fs.get_version(filename='test', author='author1', version=-2)).read()
        assert b'bar' == await (await test_fs.get_version(filename='test', author='author1', version=-1)).read()
        assert b'foo' == await (await test_fs.get_version(filename='test', author='author1', version=0)).read()
        assert b'bar' == await (await test_fs.get_version(filename='test', author='author1', version=1)).read()
        assert b'baz' == await (await test_fs.get_version(filename='test', author='author2', version=0)).read()
        assert b'baz' == await (await test_fs.get_version(filename='test', version=-1)).read()
        assert b'baz' == await (await test_fs.get_version(filename='test', version=2)).read()

        with pytest.raises(NoFile):
            await test_fs.get_version(filename='test', author='author3')
        with pytest.raises(NoFile):
            await test_fs.get_version(filename='test', author='author1', version=2)

        await test_fs.delete(one)
        await test_fs.delete(two)
        await test_fs.delete(three)

    @pytest.mark.asyncio
    async def test_put_filelike(self, test_db, test_fs):
        oid = await test_fs.put(BytesIO(b'hello world'), chunk_size=1)
        assert 11 == await test_db.fs.chunks.count()
        assert b'hello world' == await (await test_fs.get(oid)).read()

    @pytest.mark.asyncio
    async def test_file_exists(self, test_fs):
        oid = await test_fs.put(b'hello')
        with pytest.raises(FileExists):
            await test_fs.put(b'world', _id=oid)

        one = await test_fs.new_file(_id=123)
        await one.write(b'some content')
        await one.close()

        two = await test_fs.new_file(_id=123)
        with pytest.raises(FileExists):
            await two.write(b'x' * 262146)

    @pytest.mark.asyncio
    async def test_exists(self, test_fs):
        oid = await test_fs.put(b'hello')
        assert await test_fs.exists(oid)
        assert await test_fs.exists({'_id': oid})
        assert await test_fs.exists(_id=oid)

        assert not await test_fs.exists(filename='mike')
        assert not await test_fs.exists('mike')

        oid = await test_fs.put(b'hello', filename='mike', foo=12)
        assert await test_fs.exists(oid)
        assert await test_fs.exists({'_id': oid})
        assert await test_fs.exists(_id=oid)
        assert await test_fs.exists(filename='mike')
        assert await test_fs.exists({'filename': 'mike'})
        assert await test_fs.exists(foo=12)
        assert await test_fs.exists({'foo': 12})
        assert await test_fs.exists(foo={'$gt': 11})
        assert await test_fs.exists({'foo': {'$gt': 11}})

        assert not await test_fs.exists(foo=13)
        assert not await test_fs.exists({'foo': 13})
        assert not await test_fs.exists(foo={'$gt': 12})
        assert not await test_fs.exists({'foo': {'$gt': 12}})

    @pytest.mark.asyncio
    async def test_put_unicode(self, test_fs):
        with pytest.raises(TypeError):
            await test_fs.put(u'hello')

        oid = await test_fs.put(u'hello', encoding='utf-8')
        assert b'hello' == await (await test_fs.get(oid)).read()
        assert 'utf-8' == (await test_fs.get(oid)).encoding

        oid = await test_fs.put(u'aé', encoding='iso-8859-1')
        assert u'aé'.encode('iso-8859-1') == await (await test_fs.get(oid)).read()
        assert 'iso-8859-1' == (await test_fs.get(oid)).encoding

    @pytest.mark.asyncio
    async def test_missing_length_iter(self, test_db, test_fs):
        # Test fix that guards against PHP-237
        await test_fs.put(b'', filename='empty')
        doc = await test_db.fs.files.find_one({'filename': 'empty'})
        doc.pop('length')
        await test_db.fs.files.replace_one({'_id': doc['_id']}, doc)
        f = await test_fs.get_last_version(filename='empty')

        async def iterate_file(grid_file):
            async for chunk in grid_file:
                pass
            return True

        assert await iterate_file(f)

    @pytest.mark.asyncio
    async def test_gridfs_find(self, test_fs):
        await test_fs.put(b'test2', filename='two')
        await test_fs.put(b'test2+', filename='two')
        await test_fs.put(b'test1', filename='one')
        await test_fs.put(b'test2++', filename='two')
        assert 3 == await test_fs.find({'filename': 'two'}).count()
        assert 4 == await test_fs.find().count()
        cursor = test_fs.find(
            no_cursor_timeout=False).sort('uploadDate', -1).skip(1).limit(2)
        gout = await cursor.__anext__()
        assert b'test1' == await gout.read()
        cursor.rewind()
        gout = await cursor.__anext__()
        assert b'test1' == await gout.read()
        gout = await cursor.__anext__()
        assert b'test2+' == await gout.read()
        with pytest.raises(StopAsyncIteration):
            await cursor.__anext__()
        await cursor.close()
        with pytest.raises(TypeError):
            test_fs.find({}, {'_id': True})

    @pytest.mark.asyncio
    async def test_gridfs_find_one(self, test_fs):
        assert None == await test_fs.find_one()

        id1 = await test_fs.put(b'test1', filename='file1')
        assert b'test1' == await (await test_fs.find_one()).read()

        id2 = await test_fs.put(b'test2', filename='file2', meta='data')
        assert b'test1' == await (await test_fs.find_one(id1)).read()
        assert b'test2' == await (await test_fs.find_one(id2)).read()

        assert b'test1' == await (await test_fs.find_one({'filename': 'file1'})).read()

        assert 'data' == (await test_fs.find_one(id2)).meta

    @pytest.mark.asyncio
    async def test_grid_in_non_int_chunksize(self, test_db, test_fs):
        # Lua, and perhaps other buggy GridFS clients, store size as a float.
        data = b'data'
        await test_fs.put(data, filename='f')
        await test_db.fs.files.update_one({'filename': 'f'},
                                          {'$set': {'chunkSize': 100.0}})

        assert data == await (await test_fs.get_version('f')).read()
