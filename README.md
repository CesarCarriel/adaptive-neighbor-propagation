# Seleção amostral de talhões - Adaptive Neighbor Propagation (ANP)

## Descrição

Este script em Python para QGIS automatiza a seleção amostral de talhões com base na relação espacial entre eles.

A ideia surgiu da rotina agrícola de escolher quais talhões serão visitados durante a pré-colheita da cana-de-açúcar, onde é necessário selecionar áreas representativas, evitando redundância e garantindo boa distribuição espacial.

O script utiliza grafos para representar a conectividade espacial entre talhões, combinando técnicas de geometria computacional, como buffers e interseção de polígonos, para medir o grau real de vizinhança entre as áreas. A partir desses relacionamentos, aplica limites dinâmicos e adaptativos baseados em percentis para definir conexões válidas mesmo em geometrias irregulares. Em seguida, propaga a rede por meio de uma abordagem inspirada em BFS, garantindo que apenas talhões compatíveis e sem conflitos formem a amostra final. O resultado é um método preciso e automatizado para seleção amostral de talhões, criado a partir da necessidade prática da pré-colheita da cana-de-açúcar e estruturado com técnicas modernas de análise espacial e teoria dos grafos.

## Requisitos

- QGIS com PyQGIS habilitado
- Camada poligonal selecionada no painel de Layers

## Parâmetros principais

Os valores abaixo foram os que considerei mais adequados para o meu cenário durante os testes.

- **max_propagation_distance****: define a distância máxima para considerar um talhão como potencial vizinho.
Esse parâmetro ajuda a incluir talhões isolados por APPs, áreas de vegetação, pátios, carreadores largos ou outras características físicas que criam separações maiores.

- **buffer_meters_for_intersecting_neighbor_plots**: define o tamanho do buffer usado para medir quantos metros de interseção existem entre dois talhões.
Apesar de carreadores comuns variarem entre 6, 8 e 12 metros, percebi que alguns talhões considerados vizinhos ficavam a mais de 20 metros dependendo da geometria, formato ou ângulo entre eles.

Os valores que utilizei são os seguintes:

```python
max_propagation_distance = 400 # distância máxima permitida para propagação da conexão
buffer_meters_for_intersecting_neighbor_plots = 30 # tamanho do buffer usado para detectar vizinhos, usei uma valor maior por segurança
```

## Fluxo do Algoritmo


### 1. Valida que a layer selecionada é um vetor poligonal.

### 2. Cria um campo ID original para referência (fid_orig).

### 3. Gera buffers e usa um índice espacial para identificar candidatos a vizinhos.
O valor do buffer vem da variavel **buffer_meters_for_intersecting_neighbor_plots**. 

### 4. Mede o comprimento da interseção entre buffers.

Para cada talhão, são analisadas as interseções dos buffers.
Em cada par que realmente encosta ou sobrepõe, é registrado quantos metros de interseção existem.
Esses valores são fundamentais para saber quais vizinhos fazem sentido para a propagação.

### 5. Define um limite aceitável de conexão para cada talhão com base no percentil 60 das distâncias.

A partir das distâncias de interseção registradas, uso o percentil 60 para gerar um limite dinâmico que representa a “qualidade” de vizinhança ideal do talhão.
Como os talhões não têm formatos regulares (como um tabuleiro de xadrez), esse limite adaptável funciona melhor do que um valor fixo.

### 6. Classifica vizinhos como permitidos ou não permitidos.

Cada par de talhões recebe essa classificação com base no limite calculado.

### 7. Propaga uma rede começando pelo talhão com mais conexões.

A propagação começa pelo talhão com mais vizinhos permitidos.
Depois disso, cada candidato só entra na rede se nenhum talhão já na rede tiver restrições com ele, e o candidato também não tiver restrições com os talhões já inclusos.

### 8. Seleciona, no mapa, apenas os talhões que compõem a rede resultante.

As feições que fazem parte da rede propagada são selecionadas na camada ativa.
A partir daí é possível exportar a seleção ou criar campos adicionais para mapear os pontos de coleta.

## Como usar

1. Carregue sua camada de talhões no QGIS e selecione-a.

2. Execute o script no console Python.

3. As feições selecionadas ao final representam a amostra espacial sugerida.

## Licença

MIT License.
