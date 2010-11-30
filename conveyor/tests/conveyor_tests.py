import conveyor


client = None
apps = []


def setup():
    global client, apps

    client = conveyor.Conveyor(host_id='test_client', groups=['test_group0'])

    conveyor.zookeeper.delete_r(handle=client.handle, path='/applications')
    #conveyor.zookeeper.delete_r(handle=client.handle, path='/deployments')

    apps.append(conveyor.node_types.Application(id='test_app0', data={'version': '1.0', 'groups': ['test_group0']}))
    apps.append(conveyor.node_types.Application(id='test_app1', data={'version': '2.0', 'groups': ['test_group0']}))
    apps.append(conveyor.node_types.Application(id='test_app2', data={'version': '1.0', 'groups': ['test_group1']}))


def test_conveyor():
    assert conveyor.node_types.Application.get_path() == '/applications'
    assert conveyor.node_types.Application.get_path('test') == '/applications/test'

    for app in apps:
        app.write(client.handle)
        assert conveyor.node_types.Application.read(handle=client.handle, id=app.id).id == app.id
    conveyor.node_types.Application(id='test_app2', data={'version': '2.0', 'groups': ['test_group1']}).write(client.handle)

    assert conveyor.node_types.Application.read(handle=client.handle, id='test_app0').in_groups(['test_group0'])
    assert not conveyor.node_types.Application.read(handle=client.handle, id='test_app0').in_groups(['test_group1'])

    assert len(conveyor.node_types.Host.read_all(handle=client.handle)) == 1
    assert len(conveyor.node_types.Application.read_all(handle=client.handle, groups=['test_group0'])) == 2

    assert len(conveyor.node_types.Host.list(handle=client.handle)) == 1
    assert len(conveyor.node_types.Application.list(handle=client.handle)) == 3

    for app in apps:
        conveyor.node_types.Application.delete(handle=client.handle, id=app.id)