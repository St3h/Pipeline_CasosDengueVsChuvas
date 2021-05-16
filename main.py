import re 
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText

from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


def texto_para_lista(elemento, delimitador='|'):
    '''
    Recebe um texto e um delimitador
    Retorna uma lista de elementos 
    '''
    return elemento.split(delimitador)

colunas_dengue = ['id','data_iniSE','casos', 'ibge_code','cidade','uf','cep','latitude','longitude']


def lista_para_dicionario(elemento, colunas):
    '''
    Recebe duas listas
    Retorna um dicionario
    '''
    return dict(zip(colunas, elemento))

def anomesdia_para_anomes(elemento):
    '''
    Recebe um dicionario com um campo de data anomesdia
    Retorna o mesmo dicionario adicionando um campo de anomes
    '''
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    '''
    Recebe um dicinoario
    Retorna uma tupla com a chave de estado e o dicionario
    '''
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    '''
    Recebe uma tupla chave, dicionario
    Retorna uma tupla chave+ano_mes, casos dengue
    '''
    uf, itens = elemento
    for item in itens:
        if bool(re.search(r'\d', item['casos'])):
            yield(f"{uf}-{item['ano_mes']}", float(item['casos']))
        else:
            yield(f"{uf}-{item['ano_mes']}", 0.0)

def chave_uf_anomes(elemento):
    '''
    Recebe uma lista
    Retorna uma tupla chave+ano_mes, mm
    '''
    data, mm, uf = elemento
    ano_mes= '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return (chave, mm)

def arredonda_mm(elemento):
    '''
    Recebe uma tupla
    Retorna a mesma tupla com float arredonadado
    '''
    chave, mm = elemento
    return(chave, round(mm,1))

def remove_dados_vazios(elemento):
    '''
    Recebe uma tupla
    Remove elementos que tenham dados vazios
    '''
    chave, dado = elemento
    if all([
        dado['chuva'],
        dado['dengue']
          ]):
        return True
    return False

def separa_elementos(elemento):
    '''
    Recebe uma tupla
    Retorna uma tupla formatada para o padrao csv
    '''
    chave, dados = elemento
    chuva = dados['chuva'][0]
    casos = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(casos)

def prepara_csv(elemento, delimitador=';'):
    '''
    Recebe uma tupla
    Retorna um string delimitada por ;
    '''
    return f"{delimitador}".join(elemento)

dengue = (
    pipeline
    | 'Leitura do dataset dengue' >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | 'Transformacao de texto para lista (dengue)' >> beam.Map(texto_para_lista)
    | 'Transformacao de lista em dicionario' >> beam.Map(lista_para_dicionario, colunas_dengue)
    | 'Criacao do campo ano_mes' >> beam.Map(anomesdia_para_anomes)
    | 'Criaco de chave pelo estado' >> beam.Map(chave_uf)
    | 'Agrupando pelo estado' >> beam.GroupByKey()
    | 'Descompactando casos de dengues' >> beam.FlatMap(casos_dengue)
    | 'Somando casos de dengue por estado' >> beam.CombinePerKey(sum)
    #| 'Apresentacao dos resultados dengue' >> beam.Map(print)    
)

chuva = (
    pipeline
    | 'Leitura do dataset chuva' >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | 'Transformacao de texto para lista (chuva)' >> beam.Map(texto_para_lista, delimitador=',')
    | 'Criacao da chave uf_ano_mes' >> beam.Map(chave_uf_anomes)
    | 'Somando quantidade de chuvas por estado' >> beam.CombinePerKey(sum)
    | 'Arrendondando resultado da soma para uma casa' >> beam.Map(arredonda_mm)
    #| 'Apresentacao dos resultados chuva' >> beam.Map(print)
)

resultado = (
    ({'chuva': chuva, 'dengue': dengue})
    | 'Mesclando pcollections' >> beam.CoGroupByKey()
    | 'Removendo dados vazios' >> beam.Filter(remove_dados_vazios)
    | 'Separando elementos' >> beam.Map(separa_elementos)
    | 'Preparando csv' >> beam.Map(prepara_csv)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix='.csv', header=header)


pipeline.run()