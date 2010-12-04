import conveyor


client = None
apps = []


def setup():
    global client, apps

    client = conveyor.Conveyor(groups=['test_group0'], app_handler=None)

    try:
        conveyor.zookeeper.delete_r(handle=client.handle, path=conveyor.zookeeper.path_join('applications'))
    except conveyor.zookeeper.NoNodeException:
        pass

    apps.append(conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app0'), data={'version': '1.0', 'groups': ['test_group0']}))
    apps.append(conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app1'), data={'version': '2.0', 'groups': ['test_group0']}))
    apps.append(conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app2'), data={'version': '1.0', 'groups': ['test_group1']}))


def test_conveyor():
    for app in apps:
        app.write(client.handle)
        assert conveyor.nodes.Application.read(handle=client.handle, path=app.path).path == app.path
    conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app2'), data={'version': '2.0', 'groups': ['test_group1']}).write(client.handle)

    assert conveyor.nodes.Application.read(handle=client.handle, path=conveyor.zookeeper.path_join('applications', 'test_app0')).in_groups(['test_group0'])
    assert not conveyor.nodes.Application.read(handle=client.handle, path=conveyor.zookeeper.path_join('applications', 'test_app0')).in_groups(['test_group1'])

    assert len(conveyor.nodes.Application.read_all(handle=client.handle, path=conveyor.zookeeper.path_join('applications'), groups=['test_group0'])) == 2

    assert len(conveyor.nodes.list(handle=client.handle, path=conveyor.zookeeper.path_join('applications'))) == 3

    conveyor.nodes.DeploymentSlot(path=conveyor.zookeeper.path_join('applications', 'test_app0', 'test_client')).occupy(handle=client.handle)
    conveyor.nodes.DeploymentSlot.free(handle=client.handle, path=conveyor.zookeeper.path_join('applications', 'test_app0', 'test_client'), app_handler_result=True)

    for app in apps:
        conveyor.nodes.delete(handle=client.handle, path=app.path)


def teardown():
    conveyor.nodes.delete(handle=client.handle, path=conveyor.zookeeper.path_join('applications'))

    client.close()