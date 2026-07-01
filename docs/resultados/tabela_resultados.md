# Resultados dos Testes Go-Back-N

Arquivo usado nos testes: `/tmp/teste_1mb.bin`

| Teste | Janela N | Perda configurada | Tempo (ms) | Throughput (KB/s) | Segmentos | Pacotes enviados | ACKs | Pacotes retransmitidos | Timeouts | Perdas simuladas | Taxa de perda efetiva | MD5 original | MD5 recebido | Integridade |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| 1 | 1 | 0.00 | 2183 | 469.08 | 1024 | 1024 | 1024 | 0 | 0 | 0 | 0.0000 | `542f1585beafa4512c39374d47ab9a35` | `542f1585beafa4512c39374d47ab9a35` | OK |
| 2 | 4 | 0.00 | 583 | 1756.43 | 1024 | 1024 | 1024 | 0 | 0 | 0 | 0.0000 | `542f1585beafa4512c39374d47ab9a35` | `542f1585beafa4512c39374d47ab9a35` | OK |
| 3 | 8 | 0.00 | 301 | 3401.99 | 1024 | 1024 | 1024 | 0 | 0 | 0 | 0.0000 | `542f1585beafa4512c39374d47ab9a35` | `542f1585beafa4512c39374d47ab9a35` | OK |
| 4 | 4 | 0.10 | 60468 | 16.93 | 1024 | 1502 | 1382 | 478 | 120 | 120 | 0.1049 | `542f1585beafa4512c39374d47ab9a35` | `542f1585beafa4512c39374d47ab9a35` | OK |
| 5 | 8 | 0.10 | 56225 | 18.21 | 1024 | 1920 | 1808 | 896 | 112 | 112 | 0.0986 | `542f1585beafa4512c39374d47ab9a35` | `542f1585beafa4512c39374d47ab9a35` | OK |
| 6 | 8 | 0.20 | 132193 | 7.75 | 1024 | 3104 | 2840 | 2080 | 264 | 264 | 0.2050 | `542f1585beafa4512c39374d47ab9a35` | `542f1585beafa4512c39374d47ab9a35` | OK |
