SAMPLEPATH = "sample"
TRAINPATH = "train.marks"
OUT_PATH = "all_qd_pairs"

with open(OUT_PATH, 'w') as output:
    with open(SAMPLEPATH, 'r') as inp:
        for line in inp:
            output.write('\t'.join(line.split(',')))
    with open(TRAINPATH, 'r') as inp:
        for line in inp:
            output.write("\t".join(line.split("\t")[:2]) + "\n")
