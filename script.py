from qgis.core import QgsProject, QgsSpatialIndex
import numpy as np
from collections import deque
from typing import Dict, Set, Tuple


max_propagation_distance = 400
buffer_meters_for_intersecting_neighbor_plots = 30

layer = iface.activeLayer()

if layer is None:
    raise Exception('Nenhuma layer selecionada. Selecione uma camada no painel de Layers.')

if layer.type() != layer.VectorLayer:
    raise Exception('A camada selecionada não é vetorial. Selecione uma camada de polígonos.')

if layer.geometryType() != QgsWkbTypes.PolygonGeometry:
    raise Exception('A camada selecionada precisa ser do tipo Polígono ou MultiPolígono.')

args = dict(
    INPUT=layer,
    FIELD_NAME='fid_orig',
    FIELD_TYPE=1,
    FIELD_LENGTH=10,
    NEW_FIELD=True,
    FORMULA='$id',
    OUTPUT='memory:'
)

layer_with_fid = processing.run('native:fieldcalculator', args)['OUTPUT']

args = dict(
    INPUT=layer_with_fid,
    DISTANCE=buffer_meters_for_intersecting_neighbor_plots,
    SEGMENTS=8,
    DISSOLVE=False,
    END_CAP_STYLE=0,
    JOIN_STYLE=0,
    MITER_LIMIT=2,
    OUTPUT='memory:'
)

buffer_layer = processing.run('native:buffer', args)['OUTPUT']

buffer_features = list(buffer_layer.getFeatures())

original_fid_to_buffer_geometry = {f['fid_orig']: f.geometry() for f in buffer_features}
buffer_fid_to_original_fid = {f.id(): f['fid_orig'] for f in buffer_features}

index = QgsSpatialIndex(buffer_layer.getFeatures())

overlapping_features = []

for feature in buffer_features:
    feature_id = feature['fid_orig']

    feature_with_buffer = original_fid_to_buffer_geometry[feature_id]
    candidate_neighbors = index.intersects(feature_with_buffer.boundingBox())

    for candidate_id in candidate_neighbors:
        candidate = buffer_fid_to_original_fid[candidate_id]

        if candidate <= feature_id:
            continue

        candidate_buffer = original_fid_to_buffer_geometry[candidate]

        if not feature_with_buffer.boundingBox().intersects(candidate_buffer.boundingBox()):
            continue

        candidate_neighbors_intersection = feature_with_buffer.intersection(candidate_buffer)

        if not candidate_neighbors_intersection.isEmpty():
            neighbor_intersection_length = candidate_neighbors_intersection.length()

            if neighbor_intersection_length > 0:
                overlapping_features.append(
                    (
                        feature_id,
                        candidate,
                        neighbor_intersection_length
                    )
                )

if not overlapping_features:
    raise Exception('Nenhum contato detectado dentro do buffer.')

limits_in_meters_for_acceptable_connections = {}

for feature_id in original_fid_to_buffer_geometry:
    distances_with_neighbors = [
        contact[2] for contact in overlapping_features
        if contact[0] == feature_id or contact[1] == feature_id
    ]

    neighborhood_range = np.percentile(distances_with_neighbors, 60) if distances_with_neighbors else 0

    limits_in_meters_for_acceptable_connections[feature_id] = neighborhood_range

neighbors = {feature['fid_orig']: dict(allowed=[], not_allowed=[]) for feature in buffer_features}

for feature_id, neighbor_candidate_id, length in overlapping_features:
    favorable_limit_to_connect = np.mean(
        [
            limits_in_meters_for_acceptable_connections[feature_id],
            limits_in_meters_for_acceptable_connections[neighbor_candidate_id]
        ]
    )

    should_expand_limit = (
        limits_in_meters_for_acceptable_connections[feature_id] == 0 or
        limits_in_meters_for_acceptable_connections[neighbor_candidate_id] == 0
    )

    if should_expand_limit:
        expanded_max_distance = max_propagation_distance * 0.75

        favorable_limit_to_connect = min(
            max_propagation_distance,
            favorable_limit_to_connect + expanded_max_distance
        )

    favorable_limit_to_connect = min(favorable_limit_to_connect, max_propagation_distance)

    key = 'allowed' if length <= favorable_limit_to_connect else 'not_allowed'

    neighbors[feature_id][key].append((neighbor_candidate_id, length))
    neighbors[neighbor_candidate_id][key].append((feature_id, length))

allowed_neighbors = {feature_id: set(value for value, _ in relations['allowed']) for feature_id, relations in neighbors.items()}
not_allowed_neighbors = {feature_id: set(value for value, _ in relations['not_allowed']) for feature_id, relations in neighbors.items()}

connections = {feature_id: len(allowed_neighbors[feature_id]) for feature_id in allowed_neighbors}

feature_id_with_most_connections = max(connections, key=connections.get)

def propagate_network_from_feature(initial_feature_id: int, allowed_neighbors: Dict[int, Set[int]], not_allowed_neighbors: Dict[int, Set[int]]) -> Tuple[Set[int], Set[int]]:
    # A candidate is considered in conflict if they or any current member of the network
    # are listed as not allowed between each other.

    network = {initial_feature_id}
    restrictions = set(not_allowed_neighbors.get(initial_feature_id, set()))

    queue = deque([initial_feature_id])

    while queue:
        current = queue.popleft()

        for candidate in allowed_neighbors[current]:

            if candidate in network or candidate in restrictions:
                continue

            candidate_restrictions = not_allowed_neighbors.get(candidate, set())

            candidate_has_restrictions_with_network = any(node in candidate_restrictions for node in network)

            if candidate_has_restrictions_with_network:
                continue

            network.add(candidate)
            queue.append(candidate)
            restrictions.update(candidate_restrictions)

    return network, restrictions

network, blocked = propagate_network_from_feature(
    feature_id_with_most_connections,
    allowed_neighbors,
    not_allowed_neighbors
)

candidates = set(neighbors.keys()) - network - blocked

for feature_id in candidates.copy():
    if feature_id in blocked:
        continue

    restrictions = not_allowed_neighbors.get(feature_id, set())
    conflict = any(node in restrictions for node in network)

    if conflict:
        blocked.add(feature_id)
        continue

    network.add(feature_id)

    candidates.difference_update(restrictions)
    blocked.update(restrictions)

layer.removeSelection()
layer.select(list(network))
