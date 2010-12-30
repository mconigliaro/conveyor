import conveyor


client = None
apps = []


def setup():
    global client, apps

    client = conveyor.Conveyor(groups=['test_group0'])

    try:
        conveyor.zookeeper.delete_r(handle=client.handle, path=conveyor.zookeeper.path_join('applications'))
    except conveyor.zookeeper.NoNodeException:
        pass

    apps.append(conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app0'), data={'version': '1.0', 'groups': ['test_group0']}))
    apps.append(conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app1'), data={'version': '2.0', 'groups': ['test_group0']}))
    apps.append(conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app2'), data={'version': '1.0', 'groups': ['test_group1']}))


def test_write_and_read_applications():
    for app in apps:
        app.write(client.handle)
        assert conveyor.nodes.Application.read(handle=client.handle, path=app.path).path == app.path
    conveyor.nodes.Application(path=conveyor.zookeeper.path_join('applications', 'test_app2'), data={'version': '2.0', 'groups': ['test_group1']}).write(client.handle)

    assert conveyor.nodes.Application.read(handle=client.handle, path=conveyor.zookeeper.path_join('applications', 'test_app0')).in_groups(['test_group0'])
    assert not conveyor.nodes.Application.read(handle=client.handle, path=conveyor.zookeeper.path_join('applications', 'test_app0')).in_groups(['test_group1'])


def test_deployment_slots():
    conveyor.nodes.DeploymentSlot(path=conveyor.zookeeper.path_join('applications', 'test_app0', 'test_client')).occupy(handle=client.handle)
    conveyor.nodes.DeploymentSlot.free(handle=client.handle, path=conveyor.zookeeper.path_join('applications', 'test_app0', 'test_client'), deploy_result=True)


def teardown():
    for app in apps:
        conveyor.nodes.delete(handle=client.handle, path=app.path)

    conveyor.nodes.delete(handle=client.handle, path=conveyor.zookeeper.path_join('applications'))

    client.close()