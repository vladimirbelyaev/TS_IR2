with open('queries') as f, open('querylen', 'w') as out:
    for line in f:
        spl = line.split('\t')
        out.write(spl[0] + '\t' + spl[1][:-1] + "\t" + str(len(spl[1].split(' '))) + "\n")
