import conveyor


client = None
apps = list()
app_count = 3


def setup():
    global client, apps, app_count

    client = conveyor.Conveyor(host_id='test_client', groups=['test_group0'])

    conveyor.zookeeper.delete_r(handle=client.handle, path='/apps')
    for i in range(app_count):
        apps.append(conveyor.node_types.Application(id='test_app%d' % i, version=(i % 3), groups=['test_group%d' % (i % 3)]))

def test_conveyor():
    assert conveyor.node_types.Application.get_path() == '/apps'
    assert conveyor.node_types.Application.get_path(id='test') == '/apps/test'

    for i in range(1):
        for app in apps:
            app.write(client.handle)
            assert conveyor.node_types.Application.read(handle=client.handle, id=app.id).id == app.id

    assert conveyor.node_types.Application.read(handle=client.handle, id='test_app0').in_groups(['test_group0'])
    assert not conveyor.node_types.Application.read(handle=client.handle, id='test_app0').in_groups(['test_group1'])

    assert conveyor.node_types.Application.read(handle=client.handle, id='test_app2').version_greater_than(1)
    assert not conveyor.node_types.Application.read(handle=client.handle, id='test_app2').version_greater_than(3)

    assert len(conveyor.node_types.Host.read_all(handle=client.handle)) == 1
    assert len(conveyor.node_types.Application.read_all(handle=client.handle)) == app_count

    assert len(conveyor.node_types.Host.list(handle=client.handle)) == 1
    assert len(conveyor.node_types.Application.list(handle=client.handle)) == app_count

    for app in apps:
        conveyor.node_types.Application.delete(handle=client.handle, id=app.id)