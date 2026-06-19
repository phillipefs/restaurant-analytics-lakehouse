from utils import common_functions


def test_return_hello(capsys):
    common_functions.return_hello()

    captured = capsys.readouterr()
    assert captured.out == "Hello, World!\n"
    assert captured.err == ""
