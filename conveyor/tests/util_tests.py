import conveyor


def test_run_command():
    assert conveyor.util.run_command('/bin/echo test') == 'test'


def test_comma_str_to_list():
    assert conveyor.util.comma_str_to_list('a,b,c') == ['a', 'b', 'c']


def test_read_options():
    assert conveyor.util.read_options({'a': 1, 'b': 2, 'c': 3}, {'a': 9, 'b': 2}) == {'a': 9, 'b': 2, 'c': 3}
