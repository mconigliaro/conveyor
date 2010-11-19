import conveyor


client = None
apps = list()
app_count = 3


def setup():
    global client, apps, app_count

    client = conveyor.Conveyor(host_id='test_client', groups=['test_group0'])

    conveyor.zookeeper.delete_r(handle=client.handle, path='/apps')
    for i in range(app_count):
        apps.append({
            'id': 'test_app%d' % i,
            'version': (i % 3),
            'groups': ['test_group%d' % (i % 3)]
        })

def test_conveyor():
    assert conveyor.node_types.get_path('apps') == '/apps'
    assert conveyor.node_types.get_path('apps', 'test') == '/apps/test'

    for i in range(1):
        for app in apps:
            client.create_app(**app)
            assert client.get_app(id=app['id']).id == app['id']

    assert len(client.get_apps()) == app_count

    assert len(client.list_apps()) == app_count

    for app in apps:
        client.delete_app(id=app['id'])