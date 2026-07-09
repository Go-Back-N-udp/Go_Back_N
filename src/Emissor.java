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

public class Emissor {

    private static final int TIMEOUT_MS = 500;
    private static final int PORTA_PADRAO_RECEPTOR = 5000;
    private static final int PORTA_MINIMA = 1;
    private static final int PORTA_MAXIMA = 65535;

    private final Object lock = new Object();
    private int base = 0;
    private int nextSeqNum = 0;
    private int windowSize;
    private List<Pacote> bufferEnvio;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerAtual;

    private DatagramSocket socket;
    private InetAddress enderecoDestino;
    private int portaDestino;

    private final AtomicLong totalEnviados = new AtomicLong(0);
    private final AtomicLong totalAcksRecebidos = new AtomicLong(0);
    private final AtomicLong totalPacotesRetransmitidos = new AtomicLong(0);
    private final AtomicLong totalTimeouts = new AtomicLong(0);
    private final AtomicBoolean transmissaoFinalizada = new AtomicBoolean(false);

    public static void main(String[] args) {
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        if (args.length < 4 || args.length > 5) {
            imprimirUso();
            return;
        }

        String arquivoOrigem = args[0];
        String destinoCompleto = args[1];
        int tamanhoJanela;
        double probPerda;
        int porta = PORTA_PADRAO_RECEPTOR;

        try {
            tamanhoJanela = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("[Emissor] Tamanho da janela inválido: " + args[2]);
            System.err.println("[Emissor] Informe um número inteiro maior que zero.");
            imprimirUso();
            return;
        }

        if (tamanhoJanela <= 0) {
            System.err.println("[Emissor] Tamanho da janela inválido: " + tamanhoJanela);
            System.err.println("[Emissor] O tamanho da janela deve ser maior que zero.");
            imprimirUso();
            return;
        }

        try {
            probPerda = Double.parseDouble(args[3]);
        } catch (NumberFormatException e) {
            System.err.println("[Emissor] Probabilidade de perda inválida: " + args[3]);
            System.err.println("[Emissor] Informe um valor real entre 0.0 e 1.0.");
            imprimirUso();
            return;
        }

        if (Double.isNaN(probPerda) || Double.isInfinite(probPerda)
                || probPerda < 0.0 || probPerda > 1.0) {
            System.err.println("[Emissor] Probabilidade de perda fora do intervalo permitido: " + args[3]);
            System.err.println("[Emissor] O valor deve estar entre 0.0 e 1.0.");
            imprimirUso();
            return;
        }

        if (args.length == 5) {
            try {
                porta = Integer.parseInt(args[4]);
            } catch (NumberFormatException e) {
                System.err.println("[Emissor] Porta inválida: " + args[4]);
                System.err.println("[Emissor] Informe uma porta UDP entre 1 e 65535.");
                imprimirUso();
                return;
            }
            if (porta < PORTA_MINIMA || porta > PORTA_MAXIMA) {
                System.err.println("[Emissor] Porta fora do intervalo permitido: " + porta);
                System.err.println("[Emissor] Informe uma porta UDP entre 1 e 65535.");
                imprimirUso();
                return;
            }
        }

        int idxDoisPontos = destinoCompleto.indexOf(':');
        if (idxDoisPontos <= 0 || idxDoisPontos == destinoCompleto.length() - 1) {
            System.err.println("[Emissor] Destino inválido: " + destinoCompleto);
            System.err.println("[Emissor] Use o formato <IP_destino>:<path_destino>.");
            imprimirUso();
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

    private static void imprimirUso() {
        System.err.println("Uso: java Emissor <arquivo_origem> <IP_destino>:<path_destino> "
                + "<tamanho_janela> <prob_perda> [porta]");
        System.err.println("Exemplo: java Emissor /home/alice/foto.jpg "
                + "192.168.0.10:/tmp/foto_recebida.jpg 8 0.10");
    }

    public void transmitir(String arquivoOrigem, String ipDestino, String pathDestino,
                            int tamanhoJanela, double probPerda, int porta)
            throws IOException, NoSuchAlgorithmException {

        this.windowSize = tamanhoJanela;
        this.bufferEnvio = new ArrayList<>();

        java.io.File arquivo = new java.io.File(arquivoOrigem);
        if (!arquivo.exists()) {
            System.err.println("[Emissor] Arquivo de origem não encontrado: " + arquivoOrigem);
            return;
        }
        long tamanhoArquivo = arquivo.length();

        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        socket = new DatagramSocket();
        enderecoDestino = InetAddress.getByName(ipDestino);
        portaDestino = porta;

        System.out.println("[Emissor] Conectando a " + ipDestino + ":" + portaDestino
                + " | janela=" + windowSize + " | probPerda=" + probPerda);

        Pacote handshake = Pacote.criarHandshake(probPerda, pathDestino, tamanhoArquivo);
        enviarPacoteBruto(handshake);

        socket.setSoTimeout(5000);
        byte[] bufAck = new byte[Pacote.TAMANHO_MAX_PACOTE];
        DatagramPacket dgAck = new DatagramPacket(bufAck, bufAck.length);
        try {
            socket.receive(dgAck);
            System.out.println("[Emissor] Handshake confirmado pelo Receptor.");
        } catch (IOException e) {
            System.err.println("[Emissor] Timeout aguardando confirmação do handshake. Abortando.");
            socket.close();
            scheduler.shutdownNow();
            return;
        }
        socket.setSoTimeout(0);

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

        List<Pacote> todosPacotes = new ArrayList<>(totalSegmentos);
        for (int i = 0; i < totalSegmentos; i++) {
            todosPacotes.add(Pacote.criarData(i, segmentos.get(i)));
        }

        long inicioMs = System.currentTimeMillis();

        Thread threadAcks = new Thread(() -> escutarAcks(totalSegmentos, tamanhoArquivo, inicioMs));
        threadAcks.setDaemon(true);
        threadAcks.start();

        int proximoASerEnviado = 0;
        while (true) {
            synchronized (lock) {
                if (base >= totalSegmentos) {
                    break;
                }
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
                        reiniciarTimer(todosPacotes, totalSegmentos);
                    }

                    nextSeqNum++;
                    proximoASerEnviado++;

                    if (totalEnviados.get() % 50 == 0) {
                        imprimirProgresso(tamanhoArquivo, inicioMs);
                    }
                }
            }

            try {
                Thread.sleep(2);
            } catch (InterruptedException ignored) {
            }
        }

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
        System.out.println("Total de pacotes retransmitidos: " + totalPacotesRetransmitidos.get());
        System.out.println("Total de eventos de timeout: " + totalTimeouts.get());
        System.out.println("Tempo total de transferência: " + duracaoMs + " ms");
        System.out.printf("Throughput estimado: %.2f KB/s%n", throughputKBps);
        System.out.println("Hash MD5 original: " + hashOriginal);
        System.out.println("====================================");

        socket.close();
        scheduler.shutdownNow();
    }

    private void escutarAcks(int totalSegmentos, long tamanhoArquivo, long inicioMs) {
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
                        base = numAck + 1;

                        if (base >= totalSegmentos) {
                            cancelarTimer();
                        } else if (base == nextSeqNum) {
                            cancelarTimer();
                        } else {
                            reiniciarTimerSemLer();
                        }

                        if (numAck == 0 || base % 50 == 0 || base >= totalSegmentos) {
                            imprimirProgresso(tamanhoArquivo, inicioMs);
                        }
                    }
                }
            } catch (IOException e) {
                break;
            }
        }
    }

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

    private void onTimeout() {
        synchronized (lock) {
            if (transmissaoFinalizada.get() || base >= nextSeqNum) {
                return;
            }

            System.out.println("[Emissor] TIMEOUT! Retransmitindo pacotes " + base + " até " + (nextSeqNum - 1));
            totalTimeouts.incrementAndGet();

            for (int seq = base; seq < nextSeqNum; seq++) {
                if (seq < bufferEnvio.size() && bufferEnvio.get(seq) != null) {
                    try {
                        enviarPacoteBruto(bufferEnvio.get(seq));
                        totalEnviados.incrementAndGet();
                        totalPacotesRetransmitidos.incrementAndGet();
                    } catch (IOException e) {
                        System.err.println("[Emissor] Erro ao retransmitir pacote seq=" + seq + ": " + e.getMessage());
                    }
                }
            }

            timerAtual = scheduler.schedule(this::onTimeout, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void imprimirProgresso(long tamanhoArquivo, long inicioMs) {
        long duracaoMs = Math.max(1, System.currentTimeMillis() - inicioMs);
        long bytesConfirmados = Math.min((long) base * Pacote.MAX_DADOS, tamanhoArquivo);
        double throughputKBps = (bytesConfirmados / 1024.0) / (duracaoMs / 1000.0);

        System.out.printf("[Emissor] Progresso: enviados=%d | ACKs=%d | pacotes retransmitidos=%d "
                        + "| timeouts=%d | throughput estimado=%.2f KB/s%n",
                totalEnviados.get(),
                totalAcksRecebidos.get(),
                totalPacotesRetransmitidos.get(),
                totalTimeouts.get(),
                throughputKBps);
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
