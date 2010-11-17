import conveyor


controller = None

clients = {}
for i in range(1, 4):
    clients[i] = {
        'host_id': 'client%d' % i,
        'groups': ['group%d' % (i % 2)],
        'instance': None
    }

apps = {}


def setup():
    global clients, controller

    controller = conveyor.Conveyor(host_id='controller')
    for id in clients:
        clients[id]['instance'] = conveyor.Conveyor(host_id=clients[id]['host_id'], groups=clients[id]['groups'])

def test_zkplus():
    assert conveyor.zkplus.get_parent_node('/parent/child') == '/parent'

    conveyor.zkplus.create_r(handle=controller.handle, path='/test_create_r/a/b/c', data='test data')

    conveyor.zkplus.delete_r(handle=controller.handle, path='/test_create_r')

def test_conveyor():
    assert controller.get_path('apps') == '/apps'
    assert controller.get_path('apps', 'test') == '/apps/test'

    assert controller.zookeeper('exists', '/hosts') != None

    controller.create_app(name='test', version=1, groups='group1')
    controller.create_app(name='test', version=2, groups='group1')
    controller.create_app(name='test2', version=1, groups='group1')

    controller.get_app(name='test2')

    assert len(controller.list_apps()) > 0

    controller.delete_app(name='test')
    controller.delete_app(name='test2')