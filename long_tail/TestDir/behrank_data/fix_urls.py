with open('urldata', 'r') as data, open('urldata_fixed', 'w') as out:
    for line in data:
        link = line.split('\t')[1]
        if link.startswith("www."):
            out.write(line.split("\t")[0] + "\t" + link[4:])
        else:
            out.write(line)
