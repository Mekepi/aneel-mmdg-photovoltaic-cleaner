from time import perf_counter
from pathlib import Path
from os.path import dirname, abspath

def read_search_failure(file:Path, expected_columns:int, encoding:str='ansi') -> None:
    t0:float = perf_counter()

    with open(file, 'r', 1024*1024*1024, encoding=encoding) as f:
        failed:list[str] = [line for line in f if len(line.split('";"')) != expected_columns]
            
    if (failed):
        with open(file, 'w', 1024*1024*8, encoding='utf-8') as fout:
            fout.writelines(failed)
    
    print("failed: %i"%(len(failed)))
    print("execution time: %.2f"%(perf_counter()-t0))

def fix_columns(file:Path) -> None:
    
    with open('%s\\data\\processed\\%s'%(Path(dirname(abspath(__file__))).parent, file), 'r', encoding='utf-8') as fin:
        with open('%s\\data\\processed\\empreendimento-gd-unified-fixed-coords.csv'%(Path(dirname(abspath(__file__))).parent), 'w', encoding='utf-8') as fout:
            header:str = fin.readline()
            fout.write(header)

            outofbounds:list[str] = []
            for line in fin:
                data:list[str] = line.split('";"')
                if (-90<=float('.'.join(data[2].split(',')))<=90 and -180<=float('.'.join(data[3].split(',')))<=180):
                    fout.write(line)
                    continue
                data[2] = data[2].split(',')[0][:-2]+','+data[2].split(',')[0][-2:]
                data[3] = data[3].split(',')[0][:-2]+','+data[3].split(',')[0][-2:]
                fout.write('";"'.join(data))
                outofbounds.append(line)

    if (outofbounds):
        with open('%s\\data\\error\\out-of-bounds-coords.csv'%(Path(dirname(abspath(__file__))).parent), 'w', 1024*1024*256, encoding='utf-8') as fout:
            fout.write(header)
            fout.writelines(outofbounds)