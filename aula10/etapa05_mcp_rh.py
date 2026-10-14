"""
ETAPA 5 -- MCP Server do RH (30 min). Volta aqui na ETAPA 9 (token, regra N8 e auditoria).

Os dados saem de dentro do agente e passam a morar num PROCESSO SEPARADO, que fala MCP. O agente (Etapa 5.3) e os
nós do grafo (Etapa 7, via cliente_mcp.chamar_mcp) só enxergam as tools abaixo: nunca os CSVs.

ESQUELETO: implemente as etapas na ordem (troque cada raise NotImplementedError pelo código da etapa).
  ETAPA 5.1 -- as 5 tools de LEITURA: declare cada uma com @mcp.tool(), escreva a DOCSTRING (é o "schema" que o LLM
               lê) e devolva o resultado da consulta pronta de dados_rh.py. Toda tool devolve UM dict.
  ETAPA 5.2 -- a tool de ESCRITA registrar_decisao, IDEMPOTENTE (protocolo repetido não duplica)
  ETAPA 9.2 -- autorização NO SERVER: token + regra N8 (chefia certa, nunca o próprio servidor) + decisão num enum
  ETAPA 9.3 -- auditoria: toda tentativa de escrita (ALLOW ou DENY) vira uma linha em saidas/auditoria.jsonl

REFERÊNCIAS NAS AULAS
  aula6/desafio2/mcp_unidades.py               MCP Server que lê um CSV                          <- o mais parecido (5.1)
  aula5/exemplos/03_mcp_parametros/server.py   tool com parâmetros opcionais
  aula5/exemplos/05_multiplas_tools/server.py  várias tools no mesmo server, tool de "descoberta"
  aula3/agente_retry3.py                       idempotência (5.2)
  aula5/exemplos/10_seguranca/server_seguro.py token, enum fechado e auditoria NO SERVER        <- o mais parecido (9.2/9.3)

Testar sem agente (MCP Inspector, precisa de Node.js):
    npx @modelcontextprotocol/inspector python etapa05_mcp_rh.py

Pronto quando: o Inspector lista as 6 tools; registrar C1 duas vezes gera um único registro.
"""
import sys
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer

from dados_rh import PASTA, SAIDAS, carregar_servidores, diaria, escala_da_equipe, normas, remuneracao, servidor_publico

# O server roda como SUBPROCESSO: ele não herda o ambiente de quem o chamou. Por isso carrega o .env sozinho
# (mesmo cuidado de aula5/exemplos/02_mcp_basico/database.py).
load_dotenv(PASTA / ".env")

mcp = MCPServer("rh-mcp")

ARQUIVO_REGISTROS = SAIDAS / "registros.json"
ARQUIVO_AUDITORIA = SAIDAS / "auditoria.jsonl"
TOKEN_REGISTRO = os.getenv("TOKEN_REGISTRO", "")
DECISOES_VALIDAS = {"deferido", "indeferido", "indeferido_pela_chefia"}
TIPOS_COM_CHEFIA = {"ferias", "diaria"}  # regra N8


def _ler_registros() -> dict:
    """(PRONTO) {protocolo: registro} gravados até agora."""
    if ARQUIVO_REGISTROS.exists():
        return json.loads(ARQUIVO_REGISTROS.read_text(encoding="utf-8"))
    return {}


def _gravar_registros(registros: dict) -> None:
    """(PRONTO)"""
    ARQUIVO_REGISTROS.write_text(json.dumps(registros, ensure_ascii=False, indent=2), encoding="utf-8")


# ------------------------------------------------------------------ ETAPA 5.1: leitura
@mcp.tool()
def consultar_servidor(matricula: str) -> dict:
    """Consulta os dados cadastrais de um servidor público pela matrícula.

    Args:
        matricula: Número de matrícula do servidor (4 dígitos).

    Returns:
        Um dicionário contendo matrícula, nome, cargo, equipe, saldo de férias em dias,
        abonos restantes e matrícula da chefia imediata. Retorna um dicionário de erro
        se a matrícula não for encontrada.
    """
    servidor = servidor_publico(matricula)
    if not servidor:
        return {"erro": f"Matrícula {matricula} não encontrada."}
    
    return servidor


@mcp.tool()
def consultar_escala(equipe: str, inicio: str, fim: str) -> dict:
    """Consulta a escala e os afastamentos de uma equipe em um período específico.

    Args:
        equipe: Nome da equipe a ser consultada (ex: 'Plantão A', 'Cartório').
        inicio: Data de início do período no formato AAAA-MM-DD.
        fim: Data de fim do período no formato AAAA-MM-DD.

    Returns:
        Um dicionário contendo o o tamanho da equipe, o limite de afastados
        e a lista de afastamentos no período, ou o atributo erro com o
        motivo do erro na consulta.
    """
    escala = escala_da_equipe(equipe, inicio, fim)
    
    # Se o dados_rh.py retornar um erro estruturado (ex: equipe não encontrada)
    if "erro" in escala:
        return { "erro": escala["erro"] }

    return {
        "tamanho_equipe": escala["tamanho_equipe"],
        "limite_afastados": escala["limite_afastados"],
        "afastamentos": escala["afastamentos"]
    }


@mcp.tool()
def consultar_normas(tema: str) -> dict:
    """Consulta as regras das normas internas (normas.md) filtradas por um tema específico.

    Args:
        tema: Tema da norma a ser consultada (ex: 'ferias', 'abono', 'diaria', 'escala').

    Returns:
        Um dicionário contendo o tema consultado e a lista de regras encontradas
        (cada regra com código, tema e descrição), ou o atributo erro com o
        motivo do erro na consulta.
    """
    # Devolva {"tema": tema, "regras": normas(tema)} (normas() está em dados_rh.py).
    # Por que um dict e não a lista direto? Uma tool MCP que devolve LISTA manda cada item como um conteúdo separado:
    # quem chama não distingue "lista com 1 regra" de "1 dict". Neste server, TODA tool devolve UM dict.
    norma = normas(tema)
    if not norma:
        return { "erro": f"Tema {tema} não encontrado nas normas." }

    return {
        "tema": tema,
        "regras": normas(tema)
    }


@mcp.tool()
def tabela_diarias(tipo_destino: str) -> dict:
    """Consulta a tabela de valores de diárias com base no tipo de destino da viagem.

    Args:
        tipo_destino: O tipo de destino da viagem ('capital', 'interior' ou 'exterior').

    Returns:
        Um dicionário contendo o tipo de destino, a moeda utilizada (BRL ou USD)
        e o valor da diária correspondente, ou o atributo erro com o
        motivo do erro na consulta.
    """
    # Tratamento para remover espaços extras e caracteres de quebra de linha (\r ou \n)
    destino_limpo = tipo_destino.strip()
    res = diaria(destino_limpo)
    
    if "erro" in res:
        return { "erro": res["erro"] }

    return {
        "tipo_destino": tipo_destino,
        "moeda": res["moeda"],
        "valor_diaria": res["valor_diaria"]
    }


@mcp.tool()
def consultar_remuneracao(matricula: str) -> dict:
    """[RESTRITA] Consulta o salário-base de um servidor público pelo número de matrícula.

    Args:
        matricula: Número de matrícula do servidor (4 dígitos).

    Returns:
        Um dicionário contendo a matrícula e o valor do salário-base do servidor.
        Dado restrito, destinado exclusivamente ao uso do agente Financeiro, ou o atributo erro com o
        motivo do erro na consulta.
    """
    # Devolva remuneracao(matricula) (dados_rh.py).
    retorno = remuneracao(matricula)

    if retorno is None:
        return { "erro": f"Matrícula {matricula} não encontrada ou sem remuneração disponível." }

    return {
        "matricula": matricula,
        "salario_base": retorno["salario_base"]
    }


# ------------------------------------------------------------------ ETAPA 5.2 / 9.2 / 9.3: escrita
def _auditar(protocolo: str, decisao_acesso: str, motivo: str, parametros: dict) -> None:
    # ETAPA 9.3 -- acrescente UMA linha JSON em ARQUIVO_AUDITORIA com: quando (datetime.now().isoformat()),
    #   thread_id, quem pediu (aprovador), tool ("registrar_decisao"), protocolo, decisao_acesso ("ALLOW"/"DENY"),
    #   motivo e os parâmetros (SEM o token e SEM o despacho inteiro).
    #   Referência: aula5/exemplos/10_seguranca/server_seguro.py (_registrar_auditoria), aqui em JSON Lines.
    #   Até fazer a 9.3, este pass deixa a escrita funcionar sem auditoria.
    registro = {
        "quando": datetime.now().isoformat(),
        "thread_id": parametros.get("thread_id", ""),
        "quem": parametros.get("aprovador", "sistema"),
        "tool": "registrar_decisao",
        "protocolo": protocolo,
        "decisao_acesso": decisao_acesso,  # "ALLOW" ou "DENY"
        "motivo": motivo,
        "parametros": {
            "matricula": parametros.get("matricula"),
            "tipo": parametros.get("tipo"),
            "inicio": parametros.get("inicio"),
            "fim": parametros.get("fim"),
            "decisao": parametros.get("decisao"),
            "aprovador": parametros.get("aprovador")
        }
    }
    # Garante que a pasta de saídas existe e adiciona a linha no arquivo .jsonl
    SAIDAS.mkdir(parents=True, exist_ok=True)
    with open(ARQUIVO_AUDITORIA, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")


@mcp.tool()
def registrar_decisao(protocolo: str, matricula: str, tipo: str, inicio: str, fim: str, decisao: str,
                      aprovador: str, despacho: str, token: str, thread_id: str = "") -> dict:
    """[ESCRITA -- requer autorização] Registra uma decisão de férias, abono ou diária de forma idempotente.

    Args:
        protocolo: Protocolo idempotente do pedido gerado pelas ferramentas.
        matricula: Matrícula do servidor solicitante (4 dígitos).
        tipo: Tipo do pedido ('ferias', 'abono', 'diaria', etc.).
        inicio: Data de início no formato AAAA-MM-DD.
        fim: Data de fim no formato AAAA-MM-DD.
        decisao: Decisão tomada ('deferido', 'indeferido', 'indeferido_pela_chefia').
        aprovador: Matrícula de quem aprovou/indeferiu ou 'sistema'.
        despacho: Texto completo do despacho gerado pelo redator.
        token: Token de segurança para autorização da escrita.
        thread_id: Identificador da thread no fluxo do grafo (opcional).

    Returns:
        Um dicionário informando o sucesso da operação, se houve duplicação (idempotência)
        ou se o acesso foi negado por regras de segurança do servidor.
    """
    parametros = {
        "matricula": matricula, 
        "tipo": tipo, 
        "inicio": inicio, 
        "fim": fim, 
        "decisao": decisao,
        "aprovador": aprovador, 
        "thread_id": thread_id
    }

    # ETAPA 9.2 -- ANTES de gravar, negue (e audite com DENY) quando:
    #   - token != TOKEN_REGISTRO (ou TOKEN_REGISTRO vazio);
    #   - decisao não está em DECISOES_VALIDAS;
    #   - aprovador == matricula (ninguém aprova o próprio pedido, N8);
    #   - decisao == "deferido" e tipo in TIPOS_COM_CHEFIA e aprovador != chefia do servidor no cadastro.
    #   Devolva {"sucesso": False, "erro": "..."} -- quem nega é o SERVER, não o agente.
    #   Referência: aula5/exemplos/10_seguranca/server_seguro.py (alterar_status_ocorrencia)
    if not token or token != TOKEN_REGISTRO:
        _auditar(protocolo, "DENY", "token inválido", parametros)
        return {"sucesso": False, "erro": "token inválido"}
    
    if decisao not in DECISOES_VALIDAS:
        _auditar(protocolo, "DENY", "decisão inválida", parametros)
        return {"sucesso": False, "erro": "decisão inválida"}
    
    if aprovador == matricula:
        _auditar(protocolo, "DENY", "autoaprovação (N8)", parametros)
        return {"sucesso": False, "erro": "autoaprovação (N8)"}

    # Validação N8: Se for deferido e exigir alçada, o aprovador DEVE ser a chefia cadastrada
    if decisao == "deferido" and tipo in TIPOS_COM_CHEFIA:
        servidor = servidor_publico(matricula)
        chefia_correta = servidor.get("chefia") if servidor else None
        if aprovador != chefia_correta:
            motivo_erro = f"aprovador não é a chefia {chefia_correta} (N8)"
            _auditar(protocolo, "DENY", motivo_erro, parametros)
            return {"sucesso": False, "erro": motivo_erro}

    # ETAPA 5.2 -- grave em ARQUIVO_REGISTROS (use _ler_registros/_gravar_registros).
    #   Protocolo já existe -> NÃO grave de novo; devolva {"sucesso": True, "duplicado": True, ...}.
    #   Senão grave {**parametros, "despacho": despacho, "registrado_em": ...} e devolva {"sucesso": True, ...}.
    #   Audite com ALLOW (9.3).
    # ------------------------------------------------------------------
    # ETAPA 5.2: Idempotência de Gravação
    # ------------------------------------------------------------------
    registros = _ler_registros()

    if protocolo in registros:
        # Protocolo já existe: não duplica, apenas avisa que já foi gravado
        _auditar(protocolo, "ALLOW", "duplicado", parametros) # (Usado na Etapa 9.3)
        return {
            "sucesso": True, 
            "duplicado": True, 
            "protocolo": protocolo
        }

    # Grava o novo registro
    registro_novo = {
        **parametros,
        "despacho": despacho,
        "registrado_em": datetime.now().isoformat()
    }
    
    registros[protocolo] = registro_novo
    _gravar_registros(registros)

    _auditar(protocolo, "ALLOW", "gravado", parametros) # (Usado na Etapa 9.3)
    
    return {
        "sucesso": True, 
        "duplicado": False, 
        "protocolo": protocolo
    }


if __name__ == "__main__":
    mcp.run(transport="stdio")
