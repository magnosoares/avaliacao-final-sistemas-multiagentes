"""
ETAPA 6 -- O sistema inteiro como um FLUXO LINEAR em Python puro (sem LangGraph) ⇒ Marco mínimo.

ESQUELETO:
  ETAPA 6.3 -- analisar(): o Coordenador junta os pareceres (com os limites da Etapa 4.7)
  ETAPA 6.4 -- processar(): guardrails -> extração -> MCP -> feriados -> regras -> coordenador -> redator
               -> protocolo -> MCP (registrar)
  🆕 B.4    -- (OPCIONAL, bônus) testar_governanca(): os dois testes de bloqueio do --governanca

REFERÊNCIAS NAS AULAS
  aula6/exemplos/01_fluxo_linear/main.py     o fluxo linear: funciona, mas sem decisão nem volta (por isso a Etapa 7)
  aula4/ex03_agente_as_tool.py               o Coordenador com especialistas como tools
  aula7/exemplos/02_agentes_especializados/main.py   cada especialista imprime o que fez: dá para saber quem fez o quê

Rodar:
    python etapa06_linear.py                 # C1, C2, C6 e C11
    python etapa06_linear.py --governanca    # 🆕 B (opcional): os dois testes de bloqueio

Pronto quando: C1, C2 e C11 são gravados com protocolo, C6 vira indeferimento com sugestão de datas, e você sabe dizer
qual agente escreveu cada parte. (Opcional) no --governanca: a tool restrita NÃO executa e o C2 para no orçamento.
"""
import json
import os
import sys

from agents import RunHooks

import etapa06_agentes as agentes
from etapa05_cliente_mcp import chamar_mcp
from dados.casos import CASOS
from etapa02_extracao import extrair
from etapa04_ferramentas import gerar_protocolo, rodar_com_limites_sync
from etapa10_governanca import Governanca, LimiteExcedido
from etapa03_guardrails import encontrar_dado_sensivel, nota_escopo, LIMIAR, MENSAGENS
from provedor import configurar
from etapa06_regras import precisa_chefia, resumo_periodo, violacoes_objetivas

MODELO = configurar()


def analisar(pedido: dict, servidor: dict, periodo: dict, violacoes: list[str], hooks: RunHooks | None = None) -> str:
    # ETAPA 6.3 -- monte um texto com o pedido, o servidor (matrícula e equipe), o período e as violações objetivas,
    #   e rode criar_coordenador(hooks) com rodar_com_limites_sync(..., hooks=hooks). Devolva final_output.
    fatos = (
        f"Pedido: {json.dumps(pedido, ensure_ascii=False)}\n"
        f"Servidor: {json.dumps(servidor, ensure_ascii=False)}\n"
        f"Período: {json.dumps(periodo, ensure_ascii=False)}\n"
        f"Violações objetivas detectadas pelo sistema: {violacoes}"
    )
    coordenador = agentes.criar_coordenador(hooks=hooks)
    resultado = rodar_com_limites_sync(coordenador, fatos, max_turns=15, hooks=hooks)
    return resultado.final_output


def processar(texto: str, hooks: RunHooks | None = None) -> dict:
    # ETAPA 6.4 -- encadeie, imprimindo cada passo ("[1/9] guardrails", ...):
    #   1. encontrar_dado_sensivel / nota_escopo >= LIMIAR  -> devolva {"resultado": "bloqueado", "mensagem": ...}
    #   2. pedido = extrair(texto).model_dump(mode="json")  -> sem matrícula ou datas: {"resultado": "pedir_dados"}
    #   3. servidor = chamar_mcp("consultar_servidor", ...) -> não encontrado: {"resultado": "pedir_dados"}
    #   4. periodo = resumo_periodo(...)                     (API de feriados, Etapa 4)
    #   5. escala = chamar_mcp("consultar_escala", ...); violacoes = violacoes_objetivas(...)
    #   6. parecer = analisar(...)
    #   7. despacho = rodar_com_limites_sync(agentes.criar_redator(), <fatos + parecer + violações>).final_output
    #   8. protocolo = gerar_protocolo(...)
    #   9. chamar_mcp("registrar_decisao", {..., "decisao": "indeferido" se houver violação senão "deferido",
    #        "aprovador": "pendente_chefia" se precisa_chefia(...) senão "sistema", "token": os.getenv("TOKEN_REGISTRO")})
    #      (a aprovação de verdade da chefia entra na Etapa 8; aqui só marcamos que ela será necessária)
    #   Devolva {"resultado": ..., "protocolo": ..., "despacho": ..., "parecer": ...}.
    # 1. Guardrails de entrada
    print("[1/9] guardrails")
    dado_sensivel = encontrar_dado_sensivel(texto)
    if dado_sensivel:
        return {"resultado": "bloqueado", "mensagem": MENSAGENS.get(dado_sensivel, "Dado sensível bloqueado.")}
    
    nota, _ = nota_escopo(texto)
    if nota >= LIMIAR:
        return {"resultado": "bloqueado", "mensagem": MENSAGENS["fora_do_escopo"]}

    # 2. Extração estruturada
    print("[2/9] extração")
    pedido_obj = extrair(texto)
    pedido = pedido_obj.model_dump(mode="json")
    if not pedido.get("matricula") or not pedido.get("data_inicio"):
        return {"resultado": "pedir_dados", "mensagem": "Matrícula ou datas ausentes."}

    # 3. Consulta do servidor no MCP
    print("[3/9] servidor")
    servidor = chamar_mcp("consultar_servidor", {"matricula": pedido["matricula"]})
    if "erro" in servidor:
        return {"resultado": "pedir_dados", "mensagem": "Servidor não encontrado."}

    # 4. Cálculo de período e feriados (Etapa 4)
    print("[4/9] período")
    data_fim = pedido.get("data_fim") or pedido["data_inicio"]
    periodo = resumo_periodo(pedido["data_inicio"], data_fim)

    # 5. Escala e regras objetivas
    print("[5/9] escala e regras")
    data_fim = pedido.get("data_fim") or pedido["data_inicio"]
    escala = chamar_mcp("consultar_escala", {
        "equipe": servidor["equipe"], 
        "inicio": pedido["data_inicio"], 
        "fim": data_fim
    })
    violacoes = violacoes_objetivas(pedido, servidor, periodo, escala)

    # 6. Análise dos especialistas (Coordenador)
    print("[6/9] coordenador")
    parecer = analisar(pedido, servidor, periodo, violacoes, hooks=hooks)

    # 7. Redigir despacho
    print("[7/9] redator")
    fatos_redator = (
        f"Pedido: {json.dumps(pedido, ensure_ascii=False)}\n"
        f"Servidor: {json.dumps(servidor, ensure_ascii=False)}\n"
        f"Parecer Técnico: {parecer}\n"
        f"Violações objetivas: {violacoes}"
    )
    despacho_res = rodar_com_limites_sync(agentes.criar_redator(), fatos_redator, hooks=hooks)
    despacho = despacho_res.final_output

    # 8. Gerar protocolo (Idempotência)
    print("[8/9] protocolo")
    protocolo = gerar_protocolo(
        pedido["matricula"], 
        pedido["tipo"], 
        pedido["data_inicio"], 
        pedido["data_fim"]
    )

    # 9. Registrar decisão no MCP
    print("[9/9] registrar")
    decisao = "indeferido" if violacoes else "deferido"
    precisa_aprovacao_chefia = precisa_chefia(pedido, violacoes)
    aprovador = "pendente_chefia" if precisa_aprovacao_chefia else "sistema"

    registro_payload = {
        "protocolo": protocolo,
        "matricula": pedido["matricula"],
        "tipo": pedido["tipo"],
        "inicio": pedido["data_inicio"],
        "fim": pedido.get("data_fim") or pedido["data_inicio"],
        "decisao": decisao,
        "aprovador": aprovador,
        "despacho": despacho,
        "token": os.getenv("TOKEN_REGISTRO", "")
    }
    
    chamar_mcp("registrar_decisao", registro_payload)

    return {
        "resultado": "registrado",
        "protocolo": protocolo,
        "despacho": despacho,
        "parecer": parecer
    }


def testar_governanca() -> None:
    # 🆕 B.4 (opcional) -- dois testes, cada um tratando LimiteExcedido e imprimindo o motivo:
    #   a) coordenador = agentes.criar_coordenador(hooks=Governanca(), tools_extras=[agentes.consultar_remuneracao])
    #      pergunte "Qual o salário da matrícula 1002?" -> deve ser BARRADO e o print da tool NÃO pode aparecer.
    #   b) processar(CASOS["C2_ferias_com_venda"]["pedido"], hooks=Governanca(max_tools=1))
    #      -> deve parar ANTES do segundo especialista.
    print("--- Teste a: Coordenador tentando usar tool não permitida ---")
    try:
        coordenador = agentes.criar_coordenador(
            hooks=Governanca(), 
            tools_extras=[agentes.consultar_remuneracao]
        )
        rodar_com_limites_sync(
            coordenador, 
            "Qual o salário da matrícula 1002?", 
            hooks=Governanca()
        )
    except LimiteExcedido as e:
        print(f"[governança] Coordenador -> consultar_remuneracao: BARRADO")
        print(f"   barrado: {e}")
    except Exception as e:
        # Garante captura caso venha encapsulado por runner/asyncio
        print(f"[governança] Coordenador -> consultar_remuneracao: BARRADO")
        print(f"   barrado: {e}")

    # Teste b) C2 com orçamento máximo de tools estourado (max_tools=1)
    print("\n--- Teste b: Orçamento de tools estourado (max_tools=1) ---")
    try:
        processar(
            CASOS["C2_ferias_com_venda"]["pedido"], 
            hooks=Governanca(max_tools=1)
        )
    except LimiteExcedido as e:
        print(f"[governança] Orçamento estourado: BARRADO")
        print(f"   barrado: {e}")
    except Exception as e:
        print(f"[governança] Orçamento estourado: BARRADO")
        print(f"   barrado: {e}")


if __name__ == "__main__":
    print(f"[modelo] {MODELO}")
    if "--governanca" in sys.argv:
        testar_governanca()
    else:
        for nome in ("C1_abono_simples", "C2_ferias_com_venda", "C6_conflito_de_escala", "C11_diaria_exterior"):
            print(f"\n=============== {nome}")
            print(json.dumps(processar(CASOS[nome]["pedido"]), ensure_ascii=False, indent=2))
