from time import perf_counter
from pathlib import Path
from os.path import dirname, abspath
from collections import defaultdict

def unify_by_ceg(files:list[tuple[Path, int, list[int], int, int, str]], reorder:list[int]) -> None:
    t0:float = perf_counter()

    cegs:defaultdict[str,str] = defaultdict(str)
    no_dict:defaultdict[int, list[str]] = defaultdict(list[str])
    no_ceg:list[str] = []
    clean:list[str] = []
    header:str = ''

    for i in range(len(files)):
        with open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, files[i][0]), 'r', encoding='ansi') as fin:
            preheader:list[str] = fin.readline()[1:-2].split('";"')
            header += '"'+'";"'.join([preheader[j] for j in files[i][2]])+'";'

            for line in fin:
                data:list[str] = line[1:-2].split('";"')

                if ((files[i][4]>-1 and data[files[i][4]] != files[i][5])):
                    continue

                if (not(data[files[i][3]])):
                    no_ceg.append(line)
                    continue
                
                cegs[data[files[i][3]]] += '"'+'";"'.join([data[j] for j in files[i][2]])+'";'

                if (i==len(files)-1):
                    data = cegs[data[files[i][3]]][1:-2].split('";"')
                    data = [data[j] for j in reorder]
                
                    mark:int = 0
                    if (not(data[1])):
                        mark += 1
                    if (not(data[2] and data[3])):
                        mark += 3
                    
                    if (mark>0):
                        no_dict[mark].append('"'+'";"'.join(data)+'"\n')
                        continue

                    cegs[data[0]] = '"'+'";"'.join(data)+'"\n'
                    clean.append(data[0])
    
    preheader = header[1:-2].split('";"')
    header = '"'+'";"'.join([preheader[i] for i in reorder])+'"\n'

    if (no_ceg):
        with open("%s\\data\\error\\no-ceg.csv"%(Path(dirname(abspath(__file__))).parent), 'w', 1024*1024*256, encoding='utf-8') as fout:
            fout.writelines(no_ceg)
    
    for i in no_dict.keys():
        with open("%s\\data\\error\\no-%i-unified.csv"%(Path(dirname(abspath(__file__))).parent, i), 'w', 1024*1024*256, encoding='utf-8') as fout:
            fout.write(header)
            fout.writelines(no_dict[i])

    with open("%s\\data\\processed\\empreendimento-gd-unified.csv"%(Path(dirname(abspath(__file__))).parent), 'w', 1024*1024*1024, encoding='utf-8') as fout:
        fout.write(header)
        fout.writelines([cegs[key] for key in clean])
    
    print(perf_counter()-t0)