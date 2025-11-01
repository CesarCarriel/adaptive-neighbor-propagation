from qgis.core import QgsProject, QgsSpatialIndex
import numpy as np
from collections import deque
from typing import Dict, Set, Tuple


field_name = "talhao"
distancia_maxima_de_propagacao = 400

layer_name = 'talhao'
layer = QgsProject.instance().mapLayersByName(layer_name)[0]

args = dict(
    INPUT=self.layer,
    FIELD_NAME='fid_orig',
    FIELD_TYPE=1,
    FIELD_LENGTH=10,
    NEW_FIELD=True,
    FORMULA='$id',
    OUTPUT='memory:'
)

layer_com_fid = processing.run('native:fieldcalculator', args)['OUTPUT']

# Mapeamentos

metros_de_buffer_para_interseccao_com_talhoes_vizinhos = 30

talhoes_com_buffer = dict()

args = dict(
    INPUT=layer_com_fid,
    DISTANCE=metros_de_buffer_para_interseccao_com_talhoes_vizinhos,
    SEGMENTS=8,
    DISSOLVE=False,
    END_CAP_STYLE=0,
    JOIN_STYLE=0,
    MITER_LIMIT=2,
    OUTPUT='memory:'
)

buffer_layer = processing.run('native:buffer', args)['OUTPUT']

buffer_features = list(buffer_layer.getFeatures())
id_to_buffer_geom = {
    f['fid_orig']: f.geometry()
    for f in buffer_features
}
id_buffer_to_fid_orig = {f.id(): f['fid_orig'] for f in buffer_features}

index = QgsSpatialIndex(buffer_layer.getFeatures())

contatos = []

for feature in buffer_features:
    feature_id = feature['fid_orig']

    feature_com_buffer = id_to_buffer_geom[feature_id]
    candidatos_a_vizinhos = index.intersects(feature_com_buffer.boundingBox())

    for candidato_id_buffer in candidatos_a_vizinhos:
        candidato_a_vizinho = id_buffer_to_fid_orig[candidato_id_buffer]

        if candidato_a_vizinho <= feature_id:
            continue

        buffer_candidato_a_vizinho = id_to_buffer_geom[candidato_a_vizinho]

        if not feature_com_buffer.boundingBox().intersects(buffer_candidato_a_vizinho.boundingBox()):
            continue

        interseccao_com_candidato_a_vizinho = feature_com_buffer.intersection(buffer_candidato_a_vizinho)

        if not interseccao_com_candidato_a_vizinho.isEmpty():
            comprimento_de_interseccao_com_vizinho = interseccao_com_candidato_a_vizinho.length()
            if comprimento_de_interseccao_com_vizinho > 0:
                contatos.append(
                    (
                        feature_id,
                        candidato_a_vizinho,
                        comprimento_de_interseccao_com_vizinho
                    )
                )

if not contatos:
    raise Exception('Nenhum contato detectado dentro do buffer.')

limites_em_metros_para_conexoes_aceitaveis = {}

for feature_id in id_to_buffer_geom:
   distancias_com_os_vizinhos = [
      contato[2] for contato in contatos
      if contato[0] == feature_id or contato[1] == feature_id
   ]

   faixa_de_vizinhanca = np.percentile(distancias_com_os_vizinhos, 60) if distancias_com_os_vizinhos else 0


   limites_em_metros_para_conexoes_aceitaveis[feature_id] = faixa_de_vizinhanca 

vizinhos = {feature['fid_orig']: dict(permitido=[], nao_permitido=[]) for feature in buffer_features}

for feature_id, candidato_a_vizinho_id, comprimento in contatos:
    limite_favoravel_para_conectar = np.mean(
        [
            limites_em_metros_para_conexoes_aceitaveis[feature_id],
            limites_em_metros_para_conexoes_aceitaveis[candidato_a_vizinho_id]
        ]
    )

    deve_expandir_o_limite = (
        limites_em_metros_para_conexoes_aceitaveis[feature_id] == 0 or
        limites_em_metros_para_conexoes_aceitaveis[candidato_a_vizinho_id] == 0
    )

    if deve_expandir_o_limite:
        distancia_maxima_expandida = distancia_maxima_de_propagacao * 0.75

        limite_favoravel_para_conectar = min(distancia_maxima_de_propagacao, limite_favoravel_para_conectar + distancia_maxima_expandida)

    limite_favoravel_para_conectar = min(limite_favoravel_para_conectar, distancia_maxima_de_propagacao)

    chave = 'permitido' if comprimento <= limite_favoravel_para_conectar else 'nao_permitido'

    vizinhos[feature_id][chave].append((candidato_a_vizinho_id, comprimento))
    vizinhos[candidato_a_vizinho_id][chave].append((feature_id, comprimento))

vizinhos_permitidos = {feature_id: set(valor for valor, _ in relacionamentos['permitido']) for feature_id, relacionamentos in vizinhos.items()}
vizinhos_nao_permitidos = {feature_id: set(valor for valor, _ in relacionamentos['nao_permitido']) for feature_id, relacionamentos in vizinhos.items()}

conexoes = {feature_id: len(vizinhos_permitidos[feature_id]) for feature_id in vizinhos_permitidos}

feature_id_com_mais_conexoes = max(conexoes, key=conexoes.get)

def propagar_rede_a_partir_de_feature(feature_id_inicial: int, vizinhos_permitidos: Dict[int, Set[int]], vizinhos_nao_permitidos: Dict[int, Set[int]]) -> Tuple[Set[int], Set[int]]:
    # Um candidato é considerado em conflito se ele ou qualquer membro atual da rede
    # estão listados como não permitidos entre si.

    rede = {feature_id_inicial}
    restricoes = set(vizinhos_nao_permitidos.get(feature_id_inicial, set()))

    fila = deque([feature_id_inicial])

    while fila:
        atual = fila.popleft()

        for candidato in vizinhos_permitidos[atual]:

            if candidato in rede or candidato in restricoes:
                continue

            restricoes_do_candidato = vizinhos_nao_permitidos.get(candidato, set())

            candidato_tem_restricoes_com_a_rede = any(no in restricoes_do_candidato for no in rede)

            if candidato_tem_restricoes_com_a_rede:
                continue

            rede.add(candidato)
            fila.append(candidato)
            restricoes.update(restricoes_do_candidato)

    return rede, restricoes

rede, bloqueados = propagar_rede_a_partir_de_feature(feature_id_com_mais_conexoes, vizinhos_permitidos, vizinhos_nao_permitidos)

candidatos = set(vizinhos.keys()) - rede - bloqueados

for feature_id in candidatos.copy():
    if feature_id in bloqueados:
        continue

    restricoes = vizinhos_nao_permitidos.get(feature_id, set())
    conflito = any(no in restricoes for no in rede)

    if conflito:
        bloqueados.add(feature_id)
        continue

    rede.add(feature_id)

    candidatos.difference_update(restricoes)
    bloqueados.update(restricoes)

layer.removeSelection()
layer.select(list(rede))
