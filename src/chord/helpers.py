import hashlib
from typing import Union

def generate_id(key: Union[bytes, str], keysize=8 // 4) -> str:
    """
    Generate an ID for a key or node on the ring.

    Args:
        key (Union[bytes, str]): Key or node IP to hash.
        keysize (int): The number of characters to extract from the hash.

    Returns:
        str: The first `keysize` characters from the key hash.
    """
    _key = key
    if not isinstance(_key, bytes):
        _key = key.encode("utf-8")

    key_hash = hashlib.sha1(_key).hexdigest()
    # Get the first `keysize` characters from the hash
    return key_hash[:keysize]


def gen_finger(addr: str, ring_sz: int, keysize: int) -> dict:
    """
    Generate an entry in the finger table.

    Args:
        addr (str): Address of the node.
        ring_sz (int): Size of the ring.
        keysize (int): The number of characters to extract from the hash.

    Returns:
        dict: A dictionary containing the address, ID, and numeric ID.
    """
    _id = generate_id(addr.encode("utf-8"), keysize=keysize)
    return {"addr": addr, "id": _id, "numeric_id": int(_id, 16) % ring_sz}


def between(
    _id: int,
    left: int,
    right: int,
    inclusive_left=False,
    inclusive_right=True,
    ring_sz=2 ** 8
) -> bool:
    """
    Check if `_id` lies between `left` and `right` in a circular ring.

    Args:
        _id (int): The ID to check.
        left (int): The left boundary.
        right (int): The right boundary.
        inclusive_left (bool): Whether to include the left boundary.
        inclusive_right (bool): Whether to include the right boundary.
        ring_sz (int): The size of the ring.

    Returns:
        bool: True if `_id` lies between `left` and `right`, False otherwise.
    """
    if left != right:
        if inclusive_left:
            left = (left - 1 + ring_sz) % ring_sz
        if inclusive_right:
            right = (right + 1) % ring_sz
    if left < right:
        return left < _id < right
    else:
        return (_id > max(left, right)) or (_id < min(left, right))


def print_table(dict_arr, col_list=None):
    """
    Pretty print a list of dictionaries as a table.

    Args:
        dict_arr (list): A list of dictionaries to print.
        col_list (list, optional): A list of column names to include. Defaults to None.

    Returns:
        None
    """
    if not col_list:
        col_list = list(dict_arr[0].keys() if dict_arr else [])
    _list = []  # 1st row = header
    for item in dict_arr:
        if item is not None:
            _list.append(tuple([str(item[col] or "") for col in col_list]))
    _list = list(set(_list))
    _list = [col_list] + _list
    # Maximum size of the col for each element
    col_sz = [max(map(len, col)) for col in zip(*_list)]
    # Insert separating line before every line, and an extra one for ending.

    for i in range(0, len(_list) + 1)[::-1]:
        _list.insert(i, ["-" * i for i in col_sz])
    # Two formats for each content line and each separating line
    format_str = " | ".join(["{{:<{}}}".format(i) for i in col_sz])
    format_sep = "-+-".join(["{{:<{}}}".format(i) for i in col_sz])
    for item in _list:
        if item[0][0] == "-":
            print(format_sep.format(*item))
        else:
            print(format_str.format(*item))
