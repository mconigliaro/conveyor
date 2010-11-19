import conveyor


client = None


def setup():
    global client

    client = conveyor.Conveyor(host_id='test_client')

def test_zookeeper():
    assert conveyor.zookeeper.get_parent_node('/parent/child') == '/parent'

    conveyor.zookeeper.create_r(handle=client.handle, path='/test_create_r/a/b/c', data='test data')

    conveyor.zookeeper.delete_r(handle=client.handle, path='/test_create_r')
