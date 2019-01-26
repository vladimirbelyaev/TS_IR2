DF_PATH = 'longtail_DF'
QUERIES_PATH = 'queries_fix'
OUT_PATH = 'queries_DF'

from tqdm import tqdm
df_dict = dict()
print("Filling dict")
numdocs = 582166
with open(DF_PATH, 'r') as DF:
    for line in tqdm(DF):
        spl = line.strip().split("\t")
        if len(spl) != 2:
            continue
        df_dict[spl[0].lower()] = numdocs/int(spl[1])
print("Generating file")
sep = "::"
with open(QUERIES_PATH, 'r') as queries, open(OUT_PATH, 'w') as out:
    for line in tqdm(queries):
        spl = line.strip().split("\t")
        body = spl[1]
        num = spl[0]
        bodywords = body.split(" ")
        result = [word + sep + df_dict.get(word, "-1") for word in bodywords]
        out.write(num + "\t" + " ".join(result) + "\n")
