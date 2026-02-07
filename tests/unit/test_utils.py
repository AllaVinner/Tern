from tern.utils import format_tree


def test_format_tree():
    paths = ["a/b/c", "a/b/d", "a/c/e", "a/c/f", "a/d", "b/a"]
    expected_string = """\
/
├── a
│   ├── b
│   │   ├── c
│   │   └── d
│   ├── c
│   │   ├── e
│   │   └── f
│   └── d
└── b
    └── a
"""
    s = format_tree(paths)
    assert s == expected_string
