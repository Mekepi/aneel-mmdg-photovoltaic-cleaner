from pathlib import Path
from os.path import dirname, abspath

def split_block(path:Path) -> None:
    with open('%s\\data\\processed\\%s'%(Path(dirname(abspath(__file__))).parent, path), 'r', 1024*1024*1024, encoding='utf-8') as fin:
        header:str = fin.readline()
        lines:list[str] = fin.readlines()
    
    blocks:int = len(lines)//800_000
    for i in range(1, blocks+1):
        with open('%s\\data\\processed\\block[%i].csv'%(Path(dirname(abspath(__file__))).parent, i), 'w', 1024*1024*1024, encoding='utf-8') as fout:
            fout.write(header)
            fout.writelines(lines[len(lines)*(i-1)//blocks:len(lines)*(i)//blocks])