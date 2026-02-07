from collections import defaultdict
import datetime


def make_tree() -> defaultdict:
    return defaultdict(make_tree)


def add_path(tree, parts):
    for part in parts:
        tree = tree[part]


def _format_tree(tree: dict, prefix="") -> str:
    s = ""
    items = list(tree.items())
    for i, (name, subtree) in enumerate(items):
        is_last = i == len(items) - 1
        connector = "└── " if is_last else "├── "
        s += prefix + connector + name + "\n"
        extension = "    " if is_last else "│   "
        s += _format_tree(subtree, prefix + extension)
    return s


def format_tree(paths: list[str], *, prefix="/") -> str:
    tree = make_tree()

    for path in paths:
        parts = [p for p in path.split("/") if p]
        add_path(tree, parts)
    if prefix != "":
        prefix += "\n"
    return prefix + _format_tree(tree)


def print_tree(paths: list[str], *, prefix="/") -> None:
    print(format_tree(paths, prefix=prefix))


def create_now_str() -> str:
    return datetime.datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
