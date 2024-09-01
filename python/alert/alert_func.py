def threshold(a: list, b: list, exp: str) -> (bool, int):
    equal = False
    if '=' in exp:
        equal = True

    c, d = a, b
    if exp == 'A > B' or exp == 'A >= B':
        c, d = b, a
    for i, (x, y) in enumerate(zip(c, d)):
        if x < y:
            return True, i
        if equal and x == y:
            return True, i
    return False, 0


def direction_threshold(a: list, b: list, exp: str) -> (bool, int):
    c, d = a, b
    if exp == 'A down B':
        c, d = b, a
    for i, (x, y) in enumerate(zip(c, d)):
        if x >= y:
            # 找到了第一个上传的标识,如果是第一数据，那么就continue，继续找
            if i == 0:
                continue
            if c[i - 1] < d[i - 1]:
                return True, i
    return False, 0
