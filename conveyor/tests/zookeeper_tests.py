import conveyor


client = None


def setup():
    global client

    client = conveyor.Conveyor(host_id='test_client', app_handler=None)


def test_zookeeper():
    assert conveyor.zookeeper.path_join(['a', 'b', 'c']) == 'a/b/c'
    assert conveyor.zookeeper.path_join(['a', 'b', 'c'], absolute=True) == '/a/b/c'

    assert conveyor.zookeeper.path_split('/a/b/c') == ['a', 'b', 'c']

    assert conveyor.zookeeper.get_parent_node('/a/b/c') == '/a/b'

    conveyor.zookeeper.create_r(handle=client.handle, path='/a/b/c', data='test data')

    conveyor.zookeeper.delete_r(handle=client.handle, path='/a')
