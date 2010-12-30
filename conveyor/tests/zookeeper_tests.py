from __future__ import absolute_import

import conveyor


client = None


def setup():
    global client

    client = conveyor.Conveyor()


def test_path_join():
    assert conveyor.zookeeper.path_join('a', 'b', 'c') == '/a/b/c'


def test_path_join_relative():
    assert conveyor.zookeeper.path_join('a', 'b', 'c', relative=True) == 'a/b/c'


def test_path_split():
    assert conveyor.zookeeper.path_split('/a/b/c') == ['a', 'b', 'c']


def test_get_parent_node():
    assert conveyor.zookeeper.get_parent_node('/a/b/c') == '/a/b'


def test_create_and_delete_recursive():
    conveyor.zookeeper.create_r(handle=client.handle, path='/a/b/c', data='test data')
    conveyor.zookeeper.delete_r(handle=client.handle, path='/a')


def teardown():
    client.close()
