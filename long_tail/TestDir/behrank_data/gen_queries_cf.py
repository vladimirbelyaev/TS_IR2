DF_PATH = 'longtail_CF'
QUERIES_PATH = 'queries_fix'
OUT_PATH = 'queries_CF'

from tqdm import tqdm
df_dict = dict()
print("Filling dict")
TotalLemms = 0
with open(DF_PATH, 'r') as DF:
    for line in tqdm(DF):
        spl = line.strip().split("\t")
        if len(spl) != 2:
            continue
        df_dict[spl[0].lower()] = int(spl[1])
        TotalLemms += int(spl[1])
for key, value in tqdm(df_dict.items()):
    df_dict[key] = TotalLemms/value
print("Generating file")
sep = "::"
with open(QUERIES_PATH, 'r') as queries, open(OUT_PATH, 'w') as out:
    for line in tqdm(queries):
        spl = line.strip().split("\t")
        body = spl[1]
        num = spl[0]
        bodywords = body.split(" ")
        result = [word + sep + str(df_dict.get(word, "-1")) for word in bodywords]
        out.write(num + "\t" + " ".join(result) + "\n")
