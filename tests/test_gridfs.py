import datetime
import pytest
from gridfs.errors import NoFile


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
