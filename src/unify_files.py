from time import perf_counter
from os.path import dirname, abspath
from pathlib import Path
from collections import defaultdict
from urllib3 import request


def read_search_failure(coords_path:Path, expected_columns:int) -> None:
    t0:float = perf_counter()

    with open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, coords_path), 'r', 1024*1024*1024, encoding='ansi') as f:
        failed:list[str] = [line for line in f if len(line.split('";"')) != expected_columns]
            
    if (failed):
        with open("%s\\data\\error\\failed-%s"%(Path(dirname(abspath(__file__))).parent, coords_path), 'w', 1024*1024*8, encoding='utf-8') as fout:
            fout.writelines(failed)
    
    print("failed: %i"%(len(failed)))
    print("execution time: %.2f"%(perf_counter()-t0))

def unify_by_ceg(files:list[tuple[Path, int, list[int], int, int, str]], reorder:list[int]) -> None:
    t0:float = perf_counter()

    cegs:defaultdict[str,str] = defaultdict(str)
    no_dict:defaultdict[int, list[str]] = defaultdict(list[str])
    no_ceg:list[str] = []
    clean:list[str] = []
    header:str = ''

    for i in range(len(files)):
        with open("%s\\%s"%(dirname(abspath(__file__)), files[i][0]), 'r', encoding='ansi') as fin:
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
        with open("%s\\no-ceg.csv"%(dirname(abspath(__file__))), 'w', 1024*1024*256, encoding='ansi') as fout:
            fout.writelines(no_ceg)
    
    for i in no_dict.keys():
        with open("%s\\no-%i-unified.csv"%(dirname(abspath(__file__)), i), 'w', 1024*1024*256, encoding='ansi') as fout:
            fout.write(header)
            fout.writelines(no_dict[i])

    with open("%s\\empreendimento-gd-unified.csv"%(dirname(abspath(__file__))), 'w', 1024*1024*1024, encoding='ansi') as fout:
        fout.write(header)
        fout.writelines([cegs[key] for key in clean])
    
    print(perf_counter()-t0)

def fix_columns(file:Path) -> None:
    
    with open('%s\\%s'%(dirname(abspath(__file__)), file), 'r', encoding='ansi') as fin:
        with open('%s\\empreendimento-gd-unified-fixed-coords.csv'%(dirname(abspath(__file__))), 'w', encoding='ansi') as fout:
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
        with open('%s\\out-of-bounds-coords.csv'%(dirname(abspath(__file__))), 'w', 1024*1024*256, encoding='ansi') as fout:
            fout.write(header)
            fout.writelines(outofbounds)


def split_block(path:Path) -> None:
    with open('%s\\%s'%(dirname(abspath(__file__)), path), 'r', 1024*1024*1024, encoding='ansi') as fin:
        header:str = fin.readline()
        lines:list[str] = fin.readlines()
    
    blocks:int = len(lines)//800_000
    for i in range(1, blocks+1):
        with open('%s\\block%i.csv'%(dirname(abspath(__file__)), i), 'w', 1024*1024*1024, encoding='ansi') as fout:
            fout.write(header)
            fout.writelines(lines[len(lines)*(i-1)//blocks:len(lines)*(i)//blocks])

        

def main() -> None:
    # Etapa 0: Obter os dados:

    t0:float = perf_counter()
    try: raw = open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, "empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv"), 'bx')
    except FileExistsError: None
    except Exception as e: raise e
    else:
        raw.write(request('GET', 'https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a/download/empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv', preload_content=False, retries=False, timeout=None).data)
        raw.close()
    print(perf_counter()-t0)
    t0 = perf_counter()
    try: raw = open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, "empreendimento-geracao-distribuida.csv"), 'bx')
    except FileExistsError: None
    except Exception as e: raise e
    else:
        raw.write(request('GET', 'https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv', preload_content=False, retries=False, timeout=None).data)
        raw.close()
    print(perf_counter()-t0)
    
    # Etapa 1: descobrir o seprador, número de colunas e as colunas de não interesse de cada arquivo
    # Para ambos arquivos, o separador é '";"'.
    # Arquivo 1: 33 colunas, [0,1,3,6,7,9,10,11,12,14,18,22,25]
    # Arquivo 2: 12 colunas, [0,1,3,6]

    with open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, "empreendimento-geracao-distribuida.csv"), 'r', encoding='ansi') as f:
        header:list[str] = f.readline().split('";"')
        print(*[(i, header[i]) for i in range(len(header))], '\n\n')
        print(f.readline())

    with open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, "empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv"), 'r', encoding='ansi') as f:
        header = f.readline().split('";"')
        print(*[(i, header[i]) for i in range(len(header))], '\n\n')
        print(f.readline())
    

    # Etapa 2: verificar a uniformidade de ambos os arquivos.

    # Apenas o arquivo 'informacoes-tecnicas-fotovoltaica' possui problemas crônicos estruturais até o momento.
    # Os primeiros empreendimentos não têm ceg e 3 linhas quebradas por '\n' na coluna (10, 'NomModeloModulo').
    # Sendo assim, o arquivo original foi corrigido manualmente através da busca das linhas com ceg igual aos erros encontrados 
    # e as poucas linhas sem ceg no começo foram separadas.

    files:list[tuple[Path, int, list[int], int, int, str]] = [
        (Path("empreendimento-geracao-distribuida.csv"), 33, list(set(range(33))-set([0,1,3,6,7,9,10,11,12,14,18,22,25])), 19, 24, 'UFV'),
        (Path("empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv"), 12, list(set(range(12))-set([0,1,3,6])), 1, -1, '')
    ]
    
    for file in files:
        read_search_failure(*file[:2])
    
    # Com as devidas informações em mãos, podemos unificar os arquivos pelo ceg, excluindo colunas desnecessárias, as reorganizando e sepando linhas com problemas.
    # O número do arquivo erro é dado por não ter: geocódigo (+1), coordenadas(+3)

    """ unify_by_ceg(files, [8,4,14,15,16,20,25,23,26,21,24,27,22,11,2,3,10,6,7,5,0,1,17,19,18,12,13,9]) """

    unified:Path = Path("empreendimento-gd-unified.csv")

    # Por fim, checar a qualidade do arquivo unificado.
    #read_search_failure(unified, 28)

    # A partir daqui, vamos testar a qualidade dos dados e consertar informações que não fazem sentido:
    """ fix_columns(unified) """

    #split_block(Path('empreendimento-gd-unified-fixed-coords.csv'))

if __name__ == "__main__":
    main()


# gds:
# (0, '"DatGeracaoConjuntoDados') (1, 'AnmPeriodoReferencia') (2, 'NumCNPJDistribuidora') (3, 'SigAgente')
# (4, 'NomAgente') (5, 'CodClasseConsumo') (6, 'DscClasseConsumo') (7, 'CodSubGrupoTarifario') (8, 'DscSubGrupoTarifario')
# (9, 'CodUFibge') (10, 'SigUF') (11, 'CodRegiao') (12, 'NomRegiao') (13, 'CodMunicipioIbge') (14, 'NomMunicipio')
# (15, 'CodCEP') (16, 'SigTipoConsumidor') (17, 'NumCPFCNPJ') (18, 'NomTitularEmpreendimento') (19, 'CodEmpreendimento')
# (20, 'DthAtualizaCadastralEmpreend') (21, 'SigModalidadeEmpreendimento') (22, 'DscModalidadeHabilitado')
# (23, 'QtdUCRecebeCredito') (24, 'SigTipoGeracao') (25, 'DscFonteGeracao') (26, 'DscPorte') (27, 'NumCoordNEmpreendimento')
# (28, 'NumCoordEEmpreendimento') (29, 'MdaPotenciaInstaladaKW') (30, 'NomSubEstacao') (31, 'NumCoordESub') (32, 'NumCoordNSub"\n')

# info. tecnicas:
#(0, '"DatGeracaoConjuntoDados') (1, 'CodGeracaoDistribuida') (2, 'MdaAreaArranjo') (3, 'MdaPotenciaInstalada')
# (4, 'NomFabricanteModulo') (5, 'NomFabricanteInversor') (6, 'DatConexao') (7, 'MdaPotenciaModulos')
# (8, 'MdaPotenciaInversores') (9, 'QtdModulos') (10, 'NomModeloModulo') (11, 'NomModeloInversor"\n')


# unified
# (0, '"CodEmpreendimento') (1, 'CodMunicipioIbge') (2, 'NumCoordNEmpreendimento') (3, 'NumCoordEEmpreendimento')
# (4, 'MdaPotenciaInstaladaKW') (5, 'MdaAreaArranjo') (6, 'QtdModulos') (7, 'MdaPotenciaModulos') (8, 'NomModeloModulo')
# (9, 'NomFabricanteModulo') (10, 'MdaPotenciaInversores') (11, 'NomModeloInversor"') (12, 'NomFabricanteInversor')
# (13, 'QtdUCRecebeCredito') (14, 'CodClasseConsumo') (15, 'DscSubGrupoTarifario') (16, 'SigModalidadeEmpreendimento')
# (17, 'SigTipoConsumidor') (18, 'NumCPFCNPJ') (19, 'CodCEP') (20, 'NumCNPJDistribuidora') (21, 'NomAgente')
# (22, 'NomSubEstacao') (23, 'NumCoordNSub') (24, 'NumCoordESub') (25, 'SigTipoGeracao') (26, 'DscPorte') (27, 'DthAtualizaCadastralEmpreend"\n')