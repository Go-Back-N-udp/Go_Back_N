# Implementação do Protocolo Go-Back-N em Java via UDP

Trabalho final da disciplina de Redes de Computadores. O projeto implementa
uma transferência confiável de arquivos sobre UDP usando o protocolo
Go-Back-N (GBN), conforme a proposta do enunciado.

O sistema é dividido em dois módulos Java independentes:

- `Receptor`: aguarda datagramas UDP, recebe os parâmetros da sessão por
  handshake, aplica a FSM do receptor GBN, simula perdas e grava o arquivo
  recebido no caminho informado pelo emissor.
- `Emissor`: lê um arquivo de origem, divide o conteúdo em segmentos de até
  1024 bytes, transmite os pacotes por UDP usando janela deslizante GBN,
  processa ACKs cumulativos, retransmite em caso de timeout e envia `FIN` ao
  final.

## Requisitos

- JDK instalado.
- Nenhum framework externo é utilizado.
- Comunicação exclusivamente via sockets UDP (`DatagramSocket` e
  `DatagramPacket`).
- O `Receptor` deve ser iniciado antes do `Emissor`.

## Estrutura do projeto

```text
Go_Back_N/
├── README.md
├── src/
│   ├── Emissor.java
│   ├── Receptor.java
│   └── Pacote.java
└── Go-Back_N.iml
```

## Protocolo implementado

### Handshake

Antes do envio dos dados, o `Emissor` envia um pacote de controle
`HANDSHAKE` para o `Receptor`.

Esse pacote carrega:

- probabilidade de perda simulada;
- caminho absoluto em que o arquivo recebido deve ser salvo;
- tamanho total do arquivo de origem.

Depois de receber o handshake, o `Receptor` envia um ACK de confirmação para
que o `Emissor` possa começar a transmissão dos dados.

### Emissor Go-Back-N

O `Emissor` mantém as variáveis principais da FSM do GBN:

- `base`: número de sequência do pacote mais antigo ainda não confirmado;
- `nextSeqNum`: próximo número de sequência a ser transmitido;
- `windowSize`: tamanho da janela de transmissão, informado pelo usuário.

O emissor pode manter até `windowSize` pacotes não confirmados em trânsito.
Ao receber um ACK cumulativo de número `n`, ele avança `base` para `n + 1`.

O temporizador é único e está associado ao pacote mais antigo não confirmado.
Quando ocorre timeout, o emissor retransmite todos os pacotes entre `base` e
`nextSeqNum - 1`, como definido no Go-Back-N.

### Receptor Go-Back-N

O `Receptor` aceita somente pacotes de dados cujo número de sequência seja
igual a `expectedSeqNum`.

- Se `numSeq == expectedSeqNum`, o pacote está em ordem.
- Se `numSeq != expectedSeqNum`, o pacote é descartado e o receptor reenvia
  o último ACK cumulativo enviado.

O receptor não armazena pacotes fora de ordem, seguindo a FSM do receptor
GBN.

### Simulação de perda

Como redes locais normalmente não apresentam perda significativa de pacotes,
o `Receptor` simula perdas de forma aleatória.

Para cada pacote de dados recebido em ordem, é sorteado um valor `r` no
intervalo `[0, 1)`. Se `r < prob_perda`, o pacote é descartado
silenciosamente:

- o payload não é gravado no arquivo;
- nenhum ACK é enviado;
- `expectedSeqNum` não avança.

Pacotes que já chegam fora de ordem são descartados pela própria lógica do
GBN e não são contabilizados como perdas simuladas.

## Formato dos datagramas

Cada datagrama UDP carrega um pacote com cabeçalho de 11 bytes e payload de
até 1024 bytes.

| Campo | Tamanho | Descrição |
| --- | ---: | --- |
| `tipo` | 1 byte | `0=DATA`, `1=ACK`, `2=HANDSHAKE`, `3=FIN` |
| `num_seq` | 4 bytes | Número de sequência do pacote |
| `num_ack` | 4 bytes | Número de confirmação usado em ACKs |
| `tamanho_dados` | 2 bytes | Quantidade de bytes válidos no payload |
| `dados` | até 1024 bytes | Payload do arquivo ou dados de controle |

A serialização e desserialização dos pacotes é feita com `ByteBuffer` na
classe `Pacote`.

## Como compilar

Execute os comandos a partir da raiz do projeto `Go_Back_N`.

```bash
mkdir -p out
javac -d out src/*.java
```

Os arquivos compilados serão gerados no diretório `out`.

## Como executar

Use dois terminais: um para o `Receptor` e outro para o `Emissor`.

### 1. Iniciar o Receptor

Porta padrão:

```bash
java -cp out Receptor
```

Porta configurável:

```bash
java -cp out Receptor 5000
```

O receptor ficará aguardando o handshake inicial.

### 2. Iniciar o Emissor

Formato obrigatório definido no enunciado:

```bash
java -cp out Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>
```

Exemplo em rede local:

```bash
java -cp out Emissor /home/alice/foto.jpg 192.168.0.10:/tmp/foto_recebida.jpg 8 0.10
```

Exemplo executando emissor e receptor na mesma máquina:

```bash
java -cp out Emissor /home/alice/foto.jpg 127.0.0.1:/tmp/foto_recebida.jpg 8 0.10
```

O código também aceita uma porta opcional ao final, útil para testes:

```bash
java -cp out Emissor /home/alice/foto.jpg 127.0.0.1:/tmp/foto_recebida.jpg 8 0.10 5000
```

## Parâmetros do Emissor

| Parâmetro | Descrição |
| --- | --- |
| `<arquivo_origem>` | Caminho do arquivo que será enviado |
| `<IP_destino>:<path_destino>` | IP do receptor e caminho absoluto em que o arquivo será salvo |
| `<tamanho_janela>` | Tamanho da janela GBN (`N`) |
| `<prob_perda>` | Probabilidade de perda simulada, entre `0.0` e `1.0` |
| `[porta]` | Opcional. Porta UDP do receptor. Se omitida, usa `5000` |

Exemplo de probabilidade:

- `0.0`: sem perdas simuladas;
- `0.10`: 10% de probabilidade de perda;
- `0.25`: 25% de probabilidade de perda.

## Exemplo completo de teste local

Terminal 1:

```bash
cd Go_Back_N
mkdir -p out
javac -d out src/*.java
java -cp out Receptor 5000
```

Terminal 2:

```bash
cd Go_Back_N
java -cp out Emissor /tmp/arquivo_origem.bin 127.0.0.1:/tmp/arquivo_recebido.bin 8 0.10 5000
```

Ao final, compare os hashes dos arquivos:

```bash
md5sum /tmp/arquivo_origem.bin /tmp/arquivo_recebido.bin
```

Os dois hashes devem ser iguais para confirmar que a transferência foi
correta.

## Teste exigido para apresentação

Durante a apresentação, deve ser demonstrada a transferência de um arquivo de
pelo menos 1 MB com probabilidade de perda de 10%.

Exemplo para gerar um arquivo binário de 1 MB em Linux:

```bash
dd if=/dev/urandom of=/tmp/teste_1mb.bin bs=1024 count=1024
```

Depois, execute:

Terminal 1:

```bash
java -cp out Receptor 5000
```

Terminal 2:

```bash
java -cp out Emissor /tmp/teste_1mb.bin 127.0.0.1:/tmp/teste_1mb_recebido.bin 8 0.10 5000
```

Verificação:

```bash
md5sum /tmp/teste_1mb.bin /tmp/teste_1mb_recebido.bin
```

## Estatísticas exibidas

### Emissor

O emissor exibe:

- total de segmentos do arquivo;
- total de pacotes enviados, incluindo retransmissões;
- total de ACKs recebidos;
- total de pacotes retransmitidos;
- total de eventos de timeout;
- tempo total de transferência;
- throughput estimado;
- hash MD5 do arquivo original.

Durante a transferência, o emissor também informa progresso periódico com a
quantidade de pacotes enviados, ACKs recebidos, pacotes retransmitidos,
eventos de timeout e throughput estimado.

### Receptor

O receptor exibe:

- caminho em que o arquivo foi salvo;
- total de pacotes de dados processados;
- total de pacotes recebidos em ordem;
- total de pacotes fora de ordem descartados pela FSM;
- total de pacotes entregues com sucesso;
- total de pacotes descartados por perda simulada;
- taxa de perda efetiva;
- hash MD5 do arquivo recebido.

## Sugestão de testes para o relatório

O relatório técnico deve incluir resultados comparando diferentes tamanhos de
janela `N` e diferentes probabilidades de perda, conforme solicitado no
enunciado.

Sugestão de matriz de testes:

| Teste | Janela `N` | Probabilidade de perda | Arquivo |
| ---: | ---: | ---: | --- |
| 1 | 1 | `0.00` | binário de 1 MB |
| 2 | 4 | `0.00` | binário de 1 MB |
| 3 | 8 | `0.00` | binário de 1 MB |
| 4 | 4 | `0.10` | binário de 1 MB |
| 5 | 8 | `0.10` | binário de 1 MB |
| 6 | 8 | `0.20` | binário de 1 MB |

Para cada teste, registre:

- tempo total de transferência;
- throughput estimado;
- total de pacotes enviados;
- total de retransmissões;
- total de perdas simuladas;
- taxa de perda efetiva;
- hash MD5 do arquivo original e do arquivo recebido.

Esses dados podem ser apresentados em tabela e usados para discutir o impacto
do tamanho da janela e da probabilidade de perda no desempenho da
transferência.

## Observações importantes

- O caminho de destino enviado ao receptor deve ser absoluto ou válido no
  sistema de arquivos da máquina onde o `Receptor` está executando.
- Se emissor e receptor estiverem em máquinas diferentes, verifique se a porta
  UDP usada está liberada no firewall.
- O protocolo foi implementado para arquivos binários arbitrários, incluindo
  imagens, PDFs e executáveis.
- Com perda simulada alta, o número de retransmissões aumenta e o throughput
  tende a diminuir.
- O `FIN` é enviado após todos os pacotes de dados terem sido confirmados pelo
  receptor.

## Arquivos principais

- `src/Pacote.java`: define o formato do datagrama, tipos de pacote e métodos
  de serialização/desserialização.
- `src/Emissor.java`: implementa a FSM do emissor GBN, janela deslizante,
  temporizador, ACKs cumulativos, retransmissões e estatísticas.
- `src/Receptor.java`: implementa a FSM do receptor GBN, descarte de pacotes
  fora de ordem, simulação de perdas, gravação do arquivo e estatísticas.
