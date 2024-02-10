# -*- coding: utf-8 -*-

import os, sys 
from pdfminer.high_level import extract_text
import re
from mysql.connector import connect 
import json
import shutil
from datetime import datetime, timedelta


## Matriz para definir os padrões de cada pdf ##
SUBTIPO_DOC = {
    ## FEDERAIS ##
    "CN_Federal": {
        "Valid": ["Refere-se à situação do sujeito passivo no âmbito da RFB e da PGFN "],
        "Invalid": ["TESTE 1"],
        "WithDebits": [
            "As informações disponíveis na Secretaria da Receita Federal do Brasil",
            "situação cadastral declarada inapta pela Secretaria Especial da Receita Federal do Brasil",
            "As informações disponíveis na Procuradoria"
        ],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": [
            "O site ou aplicativo de origem parece estar indisponível",
            "Tentativas de consultar o site ou aplicativo de origem excedidas",
            "Nenhuma informação encontrada para a empresa",
            "Não foi possível concluir a ação para o contribuinte informado. Por favor, tente novamente dentro de alguns minutos."
        ],
    },
    "CN_FGTS": {
        "Valid": ["Certificado de Regularidade do FGTS"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["As informações disponíveis não são suficientes para a comprovação automática da regularidade do empregador perante o FGTS"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": [
            "Empregador não cadastrado",
            "Certidão não gerada devido à instabilidade do site de origem"
        ],
    },
    "CN_Trabalhista": {
        "Valid": ["CERTIDÃO NEGATIVA DE DÉBITOS TRABALHISTAS"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["CERTIDÃO POSITIVA DE DÉBITOS TRABALHISTAS"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },

    ## ESTADUAIS ##
    "CN_CADIN": {
        "Valid": ["Se você recebeu o Comunicado regularize sua situação no prazo de 90 (noventa) dias, contados da data de expedição do mesmo"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["Cadastro Informativo dos Créditos não Quitados de Órgãos e Entidades Estaduais CADIN Estadual"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },
    "CN_DividaAtiva": {
        "Valid": ["Débitos Tributários e de Dívida Ativa Estadual"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["As informações disponíveis nos registros da Receita Estadual"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },  
    "CN_Estadual": {
        "Valid": ["CERTIDAO DE DEBITO INSCRITO EM DIVIDA ATIVA - NEGATIVA"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["Emissão da Certidão de Regularidade Fiscal"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },  
    "CN_Fazenda": {
        "Valid": ["Secretaria da Fazenda e Planejamento"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["Certidões de Débitos não Inscritos"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },
    "CN_ICMS": {
        "Valid": ["CERTIDÃO NEGATIVA DE DÉBITOS - CND"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["TESTE 2"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },  
    "CN_PGE": {
        "Valid": ["PROCURADORIA GERAL DO ESTADO"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["As informações do contribuinte que constam da base de dados não permite a emissão da certidão de regularidade fiscal"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },
    "CN_Proc1GrauTjSP": {
        "Valid": ["Não existem processos para o CNPJ"],
        "Invalid": ["TESTE 1"],
        "WithDebits": ["Processos extintos não são considerados"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },  
    "CN_Sefaz": {
        "Valid": "emitida pela Secretaria de Fazenda",
        "Invalid": ["TESTE 1"],
        "WithDebits": ["TESTE 2"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },  
    "CN_Sintegra": {
        "Valid": "Consulta Pública ao Cadastro ICMS",
        "Invalid": ["TESTE 1"],
        "WithDebits": ["Comprovante de Inscrição e de Situação Cadastral"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    },  
    "CN_TRT2": {
        "Valid": "CERTIDÃO DE AÇÃO TRABALHISTA EM TRAMITAÇÃO PROCESSOS FÍSICOS",
        "Invalid": ["TESTE 1"],
        "WithDebits": ["Processos localizados contendo raiz de CNPJ idêntico ao fornecido pelo requerente"],
        "WithoutDebits": ["TESTE 3"],
        "ValidWithDebits": ["TESTE 4"],
        "Failed": ["TESTE 5"],
    }
}

## Matriz que traz os padrões para identificar e/ou calcular as validades ##
REGEX_VALIDADE = {
    "Est": {
        "Valid": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "Invalid": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "WithDebits": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "WithoutDebits": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}    
        },
        "ValidWithDebits": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "Failed": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}    
        },
    },  
    "Fed": {
        "Valid": {
            "Retornadas": [
                r'Válida até (\d{2}/\d{2}/\d{4})',
                r' a (\d{2}/\d{2}/\d{4})',
                r'Validade: (\d{2}/\d{2}/\d{4})',
            ],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "Invalid": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "WithDebits": {
            "Retornadas": [
                r'Validade: (\d{2}/\d{2}/\d{4})'
            ],  
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "WithoutDebits": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "ValidWithDebits": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
        "Failed": {
            "Retornadas": [],
            "NaoRetornadas": [],
            "AposEmissao": {}
        },
    }
}

## Os tipos de documentos estaduais e federais não foram incluídos ##
## direto na matriz SUBTIPO_DOC devido a redução no desempenho do script ##
ESTADUAL = ["CN_CADIN", "CN_DividaAtiva", "CN_Estadual", "CN_Fazenda", "CN_ICMS", "CN_PGE", "CN_Proc1GrauTjSP", "CN_Sefaz", "CN_Sintegra", "CN_TRT2"]
FEDERAL = ["CN_Federal", "CN_FGTS", "CN_Trabalhista", "CN_TRF1", "CN_TRF3"]

## Matriz utilizada apenas para o caso do vencimento ser trazido por escrito ##
MESES = {
    1: "JANEIRO",
    2: "FEVEREIRO",
    3: "MARÇO",
    4: "ABRIL",
    5: "MAIO",
    6: "JUNHO",
    7: "JULHO",
    8: "AGOSTO", 
    9: "SETEMBRO",
    10: "OUTUBRO",
    11: "NOVEMBRO",
    12: "DEZEMBRO"
}
    


def ConexaoMySQL():
    connection = connect( 
        host='',
        user='',
        passwd='',
        database=''
    )
    return connection
 
    
    


def ConstruirJson(text, cnpj):
    vencimento = None
    for doc, stts in SUBTIPO_DOC.items():
        for status, vl in stts.items():
            for valor in vl:
                if valor in text:
                    connection = ConexaoMySQL()
                    cursor = connection.cursor()

                    if cnpj == '49.762.636/0001-60':
                        return
                    
                    cursor.execute('''SELECT e.id_empresa, e.id_holding, ce.id_contabilidade FROM empresas e
                                    INNER JOIN contabilidade_empresas ce ON ce.id_empresa = e.id_empresa
                                    WHERE cnpj=%s''', (cnpj, ))
                    result = cursor.fetchall()
                    if len(result) > 0:
                        id_empresa = result[0][0]
                        id_holding = result[0][1]
                        id_contabilidade = result[0][2]
                    else:
                        print(f"O cnpj {cnpj} não está associado a nenhuma empresa.")
                        return

                    
                    match doc:
                        ## Para retornar os dados dos documentos federais e estaduais precisa remover o break no final desse case ##
                        ## Caso contrário vai retornar apenas os federais ##
                        case _ if doc in ESTADUAL:
                            tipo = "Est"
                            match status:
                                case _ if status == "Valid":
                                    key = REGEX_VALIDADE["Est"][status]
                                    vencimento = DefinirVencimento(key, doc, text)

                                case _ if status == "Invalid":
                                    print("PENDENTE")
                                
                                case _ if status == "WithDebits":
                                    key = REGEX_VALIDADE["Est"][status]
                                    vencimento = DefinirVencimento(key, doc, text)
                                        
                                case _ if status == "WithoutDebits":
                                    print("PENDENTE")
                                    
                                case _ if status == "ValidWithDebits":
                                    print("PENDENTE")
                                    
                                case _ if status == "Failed":
                                    print("Não encontrado")

                            break
                                    
                        case _ if doc in FEDERAL:
                            tipo = "Fed"
                            match status:
                                case _ if status == "Valid":
                                    key = REGEX_VALIDADE["Fed"][status]
                                    vencimento = DefinirVencimento(key, doc, text)

                                case _ if status == "Invalid":
                                    print("PENDENTE")
                                
                                case _ if status == "WithDebits":
                                    key = REGEX_VALIDADE["Fed"][status]
                                    vencimento = DefinirVencimento(key, doc, text)

                                case _ if status == "WithoutDebits":
                                    print("PENDENTE")
                                    
                                case _ if status == "ValidWithDebits":
                                    print("PENDENTE")
                                        
                                case _ if status == "Failed":
                                    print("Não encontrado")

                        case _:
                            print("CND inválida")
                        
                    match doc:
                        case _ if doc.startswith("CN_"):
                            nome_doc = "CN"
                            doc = doc.replace("CN_", "")
                                
                        case _ if doc.startswith("AF_"):
                            nome_doc = "AF"
                            doc = doc.replace("AF_", "")
                        
                    if id_empresa != None:
                        construct = {
                            "sub_tipo_doc": doc,
                            "status": status,
                            "id_empresa": id_empresa,
                            "tipo_doc": tipo,
                            "validade": vencimento,
                            "nome_doc": nome_doc,
                            "id_contabilidade": id_contabilidade,
                            "id_holding": id_holding
                        }      
                        return construct
                    else:
                        print("Empresa não localizada.")

        


def DefinirVencimento(key, doc, text):

    for ch, dc in key.items():
        for rgx in dc:
            vencimento_padrao = re.search(rgx, text)
            if vencimento_padrao:
                match ch:
                    ## Vencimentos já trazidos no pdf ##
                    case _ if ch == "Retornadas":
                        data = re.search(r'(\d{2}/\d{2}/\d{4})', vencimento_padrao.group(0))
                        vencimento = data.group(0).replace("/", "-")
                        _data = datetime.strptime(vencimento, "%d-%m-%Y")
                        day = f"0{_data.day}" if _data.day < 10 else _data.day
                        month = f"0{_data.month}" if _data.month < 10 else _data.month

                        ## Define a data de vencimento no formato aceito pelo banco ##
                        vencimento = "{}-{}-{}".format(_data.year, month, day)
                        return vencimento
                        
                    ## Vencimentos que exigem calcular a partir de sua data de emissão ##                              
                    case _ if ch == "NaoRetornadas":
                        data = re.search(r'(\d{2}/\d{2}/\d{4})', vencimento_padrao.group(0))
                        apos_emissao = key["AposEmissao"][doc]
                        if data != None:
                            vencimento = CalcularVencimento(apos_emissao, data.group(0))
                            return vencimento
                            
                        else:
                            
                            ## Caso específico onde traz a data de emissão nesse padrão: "({dia} {mês(Formato de nome))} DE {ano}" ##
                            for num, mesStr in MESES.items():
                                if mesStr in vencimento_padrao.group(0).upper():
                                    dia = f"0{vencimento_padrao.group(1)}" if int(vencimento_padrao.group(1)) < 10 else vencimento_padrao.group(1)
                                    mes = f"0{num}" if num < 10 else num
                                    ano = vencimento_padrao.group(3)

                                    dt = f"{dia}/{mes}/{ano}"
                                    vencimento = CalcularVencimento(apos_emissao, dt)
                                    return vencimento
                                        
    
    
    
def CalcularVencimento(dias, vencimento):
        
    data = re.search(r'(\d{2}/\d{2}/\d{4})', vencimento)
    data = data.group(0)
    data_inicial = datetime.strptime(data, "%d/%m/%Y")
    nova_data = data_inicial + timedelta(days=dias)
    vencimento = nova_data.strftime("%d/%m/%Y")
    vencimento = vencimento.replace("/", "-")
    _data = datetime.strptime(vencimento, "%d-%m-%Y")

    day = f"0{_data.day}" if _data.day < 10 else _data.day
    month = f"0{_data.month}" if _data.month < 10 else _data.month

    ## Define a data de vencimento no formato aceito pelo banco ##
    vencimento = "{}-{}-{}".format(_data.year, month, day)

    return vencimento

 
  

def AdicionaDocumento(f, text, cnpj, filename):
    dados = ConstruirJson(text, cnpj)
    if dados != None:
        if(dados['id_empresa'] != None):
            ## Define onde será salvo o arquivo ##
            caminho_atual = f
            novo_caminho = f"Contabilidade/Docs/Vencimento/"

            pasta_destino = f"/var/disco_efs/{dados['id_holding']}/{dados['id_empresa']}/" + novo_caminho
                
            if not os.path.exists(pasta_destino):
                os.makedirs(pasta_destino)

            shutil.copy(caminho_atual, pasta_destino)
            
            new_name = f"{dados['nome_doc']}_{dados['sub_tipo_doc']}.pdf"
            file_oldname = os.path.join(pasta_destino, filename)
            file_newname_newfile = os.path.join(pasta_destino, new_name)

            ## Remove os arquivos com o mesmo subtipo antes de adicionar o novo ##
            if os.path.exists(file_newname_newfile):
                os.remove(file_newname_newfile)
            
            os.rename(file_oldname, file_newname_newfile)
            

            ## Insere os dados coletados do PDF no banco ##
            connection = ConexaoMySQL()
            cursor = connection.cursor()

            cursor.execute('''REPLACE INTO empresas_documentos_validade (id_empresa, nome_doc, tipo_doc, sub_tipo_doc, status, validade, id_contador, caminho)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)''', 
                                (dados['id_empresa'], dados['nome_doc'], dados['tipo_doc'], dados['sub_tipo_doc'], dados['status'], dados['validade'], 10, novo_caminho+new_name, ))
                
            connection.commit()

            cursor.close()
            connection.close()
                
        print(json.dumps(dados, sort_keys=True, indent=4))





directory = '/var/disco_efs/monitor_contabil_temp'
for sub_directory in os.listdir(directory): 
    f = os.path.join(directory, sub_directory)
    for _, _, arquivo in os.walk(f):
        for filename in arquivo:

            f = os.path.join(directory, sub_directory, filename)
            doc = extract_text(f)
            trim = doc.split("\n")
            rmLineBreak = list(filter(lambda x: x.strip() != '', trim))

            ## Em um dos documentos os espaços estavam retornando como '\xa0', não deixando identificar padrões no texto ##
            cleaned_text_list = [line.replace('\xa0', ' ').strip() for line in rmLineBreak]
            doc = " ".join(cleaned_text_list)
            status_não_encontrado = "(não emitido)"
            extract_info = filename.split("-")
            cnpj = extract_info[1]

            if status_não_encontrado in cnpj:
                cnpj = cnpj.replace(status_não_encontrado, "")
            cnpj = "{}.{}.{}/{}-{}".format(cnpj[:2], cnpj[2:5], cnpj[5:8], cnpj[8:12], cnpj[12:14])
            
            AdicionaDocumento(f, doc, cnpj, filename)

