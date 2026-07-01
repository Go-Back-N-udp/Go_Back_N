import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;

/**
 * Módulo Emissor do protocolo Go-Back-N (GBN) sobre UDP.
 *
 * FSM do emissor (Kurose & Ross, Figura 3.20), com duas threads:
 *  - Thread principal: lê o arquivo em segmentos e os envia respeitando a
 *    janela [base, base+N), reage a "chamadas de cima" (novos segmentos).
 *  - Thread de recepção de ACKs: ouve ACKs cumulativos do receptor e avança
 *    a base da janela.
 *  - Um único temporizador (ScheduledExecutorService) cuida do pacote mais
 *    antigo não confirmado (base). Timeout -> retransmite tudo de base até
 *    nextSeqNum - 1.
 *
 * Uso:
 *   java Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda> [porta]
 */
public class Emissor {

    private static final int TIMEOUT_MS = 500;
    private static final int PORTA_PADRAO_RECEPTOR = 5000;

    // ---- Estado compartilhado entre as threads de envio e de recepção de ACK ----
    private final Object lock = new Object();
    private int base = 0;
    private int nextSeqNum = 0;
    private int windowSize;
    private List<Pacote> bufferEnvio; // buffer circular lógico: pacotes[seq % capacidade]

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerAtual;

    private DatagramSocket socket;
    private InetAddress enderecoDestino;
    private int portaDestino;

    // ---- Estatísticas ----
    private final AtomicLong totalEnviados = new AtomicLong(0);
    private final AtomicLong totalAcksRecebidos = new AtomicLong(0);
    private final AtomicLong totalRetransmissoes = new AtomicLong(0);
    private final AtomicBoolean transmissaoFinalizada = new AtomicBoolean(false);

    public static void main(String[] args) {
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        if (args.length < 4) {
            System.out.println("Uso: java Emissor <arquivo_origem> <IP_destino>:<path_destino> "
                    + "<tamanho_janela> <prob_perda> [porta]");
            System.out.println("Exemplo: java Emissor /home/alice/foto.jpg "
                    + "192.168.0.10:/tmp/foto_recebida.jpg 8 0.10");
            return;
        }

        String arquivoOrigem = args[0];
        String destinoCompleto = args[1];
        int tamanhoJanela = Integer.parseInt(args[2]);
        double probPerda = Double.parseDouble(args[3]);
        int porta = args.length >= 5 ? Integer.parseInt(args[4]) : PORTA_PADRAO_RECEPTOR;

        int idxDoisPontos = destinoCompleto.indexOf(':');
        if (idxDoisPontos < 0) {
            System.err.println("Formato inválido para IP_destino:path_destino. Exemplo: 192.168.0.10:/tmp/saida.jpg");
            return;
        }
        String ipDestino = destinoCompleto.substring(0, idxDoisPontos);
        String pathDestino = destinoCompleto.substring(idxDoisPontos + 1);

        try {
            new Emissor().transmitir(arquivoOrigem, ipDestino, pathDestino, tamanhoJanela, probPerda, porta);
        } catch (IOException e) {
            System.err.println("Erro de E/S no Emissor: " + e.getMessage());
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Erro ao calcular hash: " + e.getMessage());
        }
    }

    public void transmitir(String arquivoOrigem, String ipDestino, String pathDestino,
                            int tamanhoJanela, double probPerda, int porta)
            throws IOException, NoSuchAlgorithmException {

        this.windowSize = tamanhoJanela;
        this.bufferEnvio = new ArrayList<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        java.io.File arquivo = new java.io.File(arquivoOrigem);
        if (!arquivo.exists()) {
            System.err.println("[Emissor] Arquivo de origem não encontrado: " + arquivoOrigem);
            return;
        }
        long tamanhoArquivo = arquivo.length();

        socket = new DatagramSocket();
        enderecoDestino = InetAddress.getByName(ipDestino);
        portaDestino = porta;

        System.out.println("[Emissor] Conectando a " + ipDestino + ":" + portaDestino
                + " | janela=" + windowSize + " | probPerda=" + probPerda);

        // ---------- 1) Handshake ----------
        Pacote handshake = Pacote.criarHandshake(probPerda, pathDestino, tamanhoArquivo);
        enviarPacoteBruto(handshake);

        socket.setSoTimeout(5000);
        byte[] bufAck = new byte[Pacote.TAMANHO_MAX_PACOTE];
        DatagramPacket dgAck = new DatagramPacket(bufAck, bufAck.length);
        try {
            socket.receive(dgAck); // aguarda confirmação do handshake
            System.out.println("[Emissor] Handshake confirmado pelo Receptor.");
        } catch (IOException e) {
            System.err.println("[Emissor] Timeout aguardando confirmação do handshake. Abortando.");
            socket.close();
            scheduler.shutdownNow();
            return;
        }
        socket.setSoTimeout(0); // a thread de recepção de ACKs ficará bloqueada normalmente

        // ---------- 2) Carrega todos os segmentos do arquivo em memória como "buffer" ----------
        // (mantém o buffer indexado por seqnum % algo grande o suficiente; aqui usamos lista direta
        //  já que conhecemos o total de segmentos antecipadamente)
        List<byte[]> segmentos = lerSegmentos(arquivo);
        int totalSegmentos = segmentos.size();
        System.out.println("[Emissor] Arquivo dividido em " + totalSegmentos + " segmentos de até "
                + Pacote.MAX_DADOS + " bytes.");

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        for (byte[] seg : segmentos) {
            md5.update(seg);
        }
        String hashOriginal = bytesParaHex(md5.digest());
        System.out.println("[Emissor] Hash MD5 do arquivo original: " + hashOriginal);

        // Pré-monta todos os pacotes DATA (índice = numSeq)
        List<Pacote> todosPacotes = new ArrayList<>(totalSegmentos);
        for (int i = 0; i < totalSegmentos; i++) {
            todosPacotes.add(Pacote.criarData(i, segmentos.get(i)));
        }

        // ---------- 3) Thread de recepção de ACKs ----------
        Thread threadAcks = new Thread(() -> escutarAcks(totalSegmentos));
        threadAcks.setDaemon(true);
        threadAcks.start();

        // ---------- 4) FSM do emissor: envia respeitando a janela ----------
        long inicioMs = System.currentTimeMillis();

        int proximoASerEnviado = 0; // índice no array todosPacotes
        while (true) {
            synchronized (lock) {
                if (base >= totalSegmentos) {
                    break; // todos os pacotes foram confirmados
                }
                // Envia tudo que couber na janela e ainda não foi enviado
                while (proximoASerEnviado < totalSegmentos && nextSeqNum < base + windowSize) {
                    Pacote pacote = todosPacotes.get(proximoASerEnviado);
                    enviarPacoteBruto(pacote);
                    totalEnviados.incrementAndGet();

                    if (bufferEnvio.size() <= proximoASerEnviado) {
                        bufferEnvio.add(pacote);
                    } else {
                        bufferEnvio.set(proximoASerEnviado, pacote);
                    }

                    if (base == nextSeqNum) {
                        // único timer ativo, referente ao pacote 'base'
                        reiniciarTimer(todosPacotes, totalSegmentos);
                    }

                    nextSeqNum++;
                    proximoASerEnviado++;

                    if (totalEnviados.get() % 50 == 0) {
                        System.out.println("[Emissor] " + totalEnviados.get() + " pacotes enviados | "
                                + "ACKs recebidos=" + totalAcksRecebidos.get()
                                + " | retransmissões=" + totalRetransmissoes.get());
                    }
                }
            }

            try {
                Thread.sleep(2); // evita busy-waiting agressivo
            } catch (InterruptedException ignored) {
            }
        }

        // ---------- 5) Encerramento ----------
        cancelarTimer();
        transmissaoFinalizada.set(true);

        Pacote fin = Pacote.criarFin(totalSegmentos);
        enviarPacoteBruto(fin);
        System.out.println("[Emissor] Pacote FIN enviado.");

        long duracaoMs = System.currentTimeMillis() - inicioMs;
        double throughputKBps = duracaoMs > 0
                ? (tamanhoArquivo / 1024.0) / (duracaoMs / 1000.0)
                : 0.0;

        System.out.println();
        System.out.println("===== Estatísticas do Emissor =====");
        System.out.println("Arquivo: " + arquivoOrigem + " (" + tamanhoArquivo + " bytes)");
        System.out.println("Total de segmentos: " + totalSegmentos);
        System.out.println("Total de pacotes enviados (incluindo retransmissões): " + totalEnviados.get());
        System.out.println("Total de ACKs recebidos: " + totalAcksRecebidos.get());
        System.out.println("Total de retransmissões (timeouts): " + totalRetransmissoes.get());
        System.out.println("Tempo total de transferência: " + duracaoMs + " ms");
        System.out.printf("Throughput estimado: %.2f KB/s%n", throughputKBps);
        System.out.println("Hash MD5 original: " + hashOriginal);
        System.out.println("====================================");

        socket.close();
        scheduler.shutdownNow();
    }

    /**
     * Thread dedicada a escutar ACKs cumulativos vindos do Receptor e avançar a base da janela.
     */
    private void escutarAcks(int totalSegmentos) {
        byte[] buf = new byte[Pacote.TAMANHO_MAX_PACOTE];
        while (!transmissaoFinalizada.get()) {
            try {
                DatagramPacket dg = new DatagramPacket(buf, buf.length);
                socket.receive(dg);
                Pacote pacote = Pacote.deserializar(dg.getData(), dg.getLength());

                if (pacote.getTipo() != Pacote.TIPO_ACK) {
                    continue;
                }

                int numAck = pacote.getNumAck();
                totalAcksRecebidos.incrementAndGet();

                synchronized (lock) {
                    if (numAck >= base) {
                        // ACK cumulativo: avança a base para n+1
                        base = numAck + 1;

                        if (base >= totalSegmentos) {
                            cancelarTimer();
                        } else if (base == nextSeqNum) {
                            // não há mais pacotes não confirmados: cancela o timer
                            cancelarTimer();
                        } else {
                            // ainda há pacotes não confirmados: reinicia o timer
                            reiniciarTimerSemLer();
                        }
                    }
                    // ACKs antigos/duplicados (numAck < base) são ignorados, como manda a FSM do GBN
                }
            } catch (IOException e) {
                if (!transmissaoFinalizada.get()) {
                    // socket pode ter sido fechado ao final da transmissão; ignora nesse caso
                }
                break;
            }
        }
    }

    /**
     * Reinicia o temporizador associado ao pacote 'base', cancelando o anterior se existir.
     * Usada quando ainda não se conhece a lista de pacotes prontos (apenas reagenda).
     */
    private void reiniciarTimerSemLer() {
        cancelarTimerInterno();
        timerAtual = scheduler.schedule(this::onTimeout, TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void reiniciarTimer(List<Pacote> todosPacotes, int totalSegmentos) {
        cancelarTimerInterno();
        timerAtual = scheduler.schedule(this::onTimeout, TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void cancelarTimer() {
        synchronized (lock) {
            cancelarTimerInterno();
        }
    }

    private void cancelarTimerInterno() {
        if (timerAtual != null) {
            timerAtual.cancel(false);
            timerAtual = null;
        }
    }

    /**
     * Callback de timeout: retransmite todos os pacotes de base até nextSeqNum - 1
     * e reinicia o temporizador, exatamente como descrito na FSM do GBN.
     */
    private void onTimeout() {
        synchronized (lock) {
            if (transmissaoFinalizada.get() || base >= nextSeqNum) {
                return;
            }

            System.out.println("[Emissor] TIMEOUT! Retransmitindo pacotes " + base + " até " + (nextSeqNum - 1));

            for (int seq = base; seq < nextSeqNum; seq++) {
                if (seq < bufferEnvio.size() && bufferEnvio.get(seq) != null) {
                    try {
                        enviarPacoteBruto(bufferEnvio.get(seq));
                        totalEnviados.incrementAndGet();
                        totalRetransmissoes.incrementAndGet();
                    } catch (IOException e) {
                        System.err.println("[Emissor] Erro ao retransmitir pacote seq=" + seq + ": " + e.getMessage());
                    }
                }
            }

            // reinicia o timer para o (novo) pacote mais antigo não confirmado
            timerAtual = scheduler.schedule(this::onTimeout, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void enviarPacoteBruto(Pacote pacote) throws IOException {
        byte[] bytes = pacote.serializar();
        DatagramPacket dg = new DatagramPacket(bytes, bytes.length, enderecoDestino, portaDestino);
        socket.send(dg);
    }

    private List<byte[]> lerSegmentos(java.io.File arquivo) throws IOException {
        List<byte[]> segmentos = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(arquivo)) {
            byte[] buffer = new byte[Pacote.MAX_DADOS];
            int lidos;
            while ((lidos = fis.read(buffer)) != -1) {
                byte[] segmento = new byte[lidos];
                System.arraycopy(buffer, 0, segmento, 0, lidos);
                segmentos.add(segmento);
            }
        }
        return segmentos;
    }

    private String bytesParaHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
