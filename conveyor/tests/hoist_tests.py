import json
import subprocess

import conveyor


def run_command(command):
    return subprocess.Popen(command, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0].strip()


def get_json_from_command(command):
    return json.loads(run_command(command))


def test_create_and_get_application():
    run_command('./bin/hoist --groups=group1 application create test 1')
    app = get_json_from_command('./bin/hoist application get test')
    assert app['groups'] == ['group1']
    assert app['version'] == '1'


def test_list_applications():
    assert get_json_from_command('./bin/hoist application list') == ['test']


def test_delete_applications():
    run_command('./bin/hoist application delete test')