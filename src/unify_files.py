from time import perf_counter
from os.path import dirname, abspath
from pathlib import Path
from urllib3 import request

from validator import read_search_failure, fix_columns
from unify_by_ceg import unify_by_ceg
from splitter import split_block


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
        lines:list[str] = f.readlines()
        header:list[str] = lines[0].split('";"')
        print(*[(i, header[i]) for i in range(len(header))], '\n\n')
        print(len(lines))
        print(lines[1])

    with open("%s\\data\\raw\\%s"%(Path(dirname(abspath(__file__))).parent, "empreendimento-gd-informacoes-tecnicas-fotovoltaica.csv"), 'r', encoding='ansi') as f:
        lines = f.readlines()
        header = lines[0].split('";"')
        print(*[(i, header[i]) for i in range(len(header))], '\n\n')
        print(len(lines))
        print(lines[1])
    

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
        read_search_failure(Path("%s%s%s"%(Path(dirname(abspath(__file__))).parent,'\\data\\raw\\',file[0])), file[1])
    
    # Etapa 3: unificar os arquivos pelo ceg, excluindo colunas desnecessárias, as reorganizando e sepando linhas com problemas.
    # O número do arquivo erro é dado por não ter: geocódigo (+1), coordenadas(+3)

    unify_by_ceg(files, [8,4,14,15,16,20,25,23,26,21,24,27,22,11,2,3,10,6,7,5,0,1,17,19,18,12,13,9])

    unified:Path = Path("empreendimento-gd-unified.csv")
    read_search_failure(Path("%s%s%s"%(Path(dirname(abspath(__file__))).parent,'\\data\\processed\\',unified)), 28)

    # Etapa 4: testar a qualidade dos dados e consertar informações que não fazem sentido:
    fix_columns(unified)

    split_block(Path('empreendimento-gd-unified-fixed-coords.csv'))

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