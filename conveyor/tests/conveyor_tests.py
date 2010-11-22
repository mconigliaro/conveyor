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
    assert conveyor.node_types.get_path(conveyor.node_types.Application) == '/apps'
    assert conveyor.node_types.get_path(conveyor.node_types.Application, 'test') == '/apps/test'

    for i in range(1):
        for app in apps:
            app.write(client.handle)
            assert client.get_app(id=app.id).id == app.id

    assert client.get_app(id='test_app0').in_groups(['test_group0'])
    assert not client.get_app(id='test_app0').in_groups(['test_group1'])

    assert client.get_app(id='test_app2').version_greater_than(1)
    assert not client.get_app(id='test_app2').version_greater_than(3)

    assert len(client.get_apps()) == app_count

    assert len(client.list_apps()) == app_count

    for app in apps:
        client.delete_app(id=app.id)