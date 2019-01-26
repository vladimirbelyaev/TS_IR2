with open('urldata', 'r') as data, open('urlset', 'w') as out:
    for line in data:
        link = line.split('\t')[1]
        if link.startswith("www."):
            out.write(link[4:])
        else:
            out.write(link)

with open('queries', 'r') as data, open('queryset', 'w') as out:
    for line in data:
        out.write(line.split('\t')[1])
