import re


def strip_tags(x):
    return re.sub(r'<[^>]*?>', '', str(x)).replace("&quot;", '"')
