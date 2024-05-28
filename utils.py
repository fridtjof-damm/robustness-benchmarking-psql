def format_tuple(t: tuple) -> str:
    # (1, "algeria", 10) -> 1; algeria; 10
    result = ''
    for i in t:
        result += str(i) + ';'
    return result[:-1]
