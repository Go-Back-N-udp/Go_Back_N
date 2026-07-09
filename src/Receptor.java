import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class Receptor {

    private static final int TAMANHO_BUFFER = Pacote.TAMANHO_MAX_PACOTE;

    public static void main(String[] args) {
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        int porta = 5000;
        if (args.length >= 1) {
            try {
                porta = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Porta inválida, usando porta padrão 5000.");
            }
        }

        try {
            new Receptor().iniciar(porta);
        } catch (IOException e) {
            System.err.println("Erro de E/S no Receptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private final Random random = new Random();

    public void iniciar(int porta) throws IOException {
        try (DatagramSocket socket = new DatagramSocket(porta)) {
            System.out.println("[Receptor] Aguardando handshake na porta " + porta + "...");

            byte[] bufHandshake = new byte[TAMANHO_BUFFER];
            DatagramPacket dgHandshake = new DatagramPacket(bufHandshake, bufHandshake.length);
            socket.receive(dgHandshake);

            Pacote pacoteHandshake = Pacote.deserializar(dgHandshake.getData(), dgHandshake.getLength());
            if (pacoteHandshake.getTipo() != Pacote.TIPO_HANDSHAKE) {
                System.err.println("[Receptor] Primeiro pacote recebido não é HANDSHAKE. Abortando.");
                return;
            }

            String[] partes = pacoteHandshake.getPayloadComoTexto().split("\\|", 3);
            double probPerda = Double.parseDouble(partes[0]);
            String pathDestino = partes[1];
            long tamanhoArquivo = Long.parseLong(partes[2]);

            java.net.InetAddress enderecoEmissor = dgHandshake.getAddress();
            int portaEmissor = dgHandshake.getPort();

            System.out.println("[Receptor] Handshake recebido de " + enderecoEmissor + ":" + portaEmissor);
            System.out.println("[Receptor] probPerda=" + probPerda + " pathDestino=" + pathDestino
                    + " tamanhoArquivo=" + tamanhoArquivo + " bytes");

            enviarAck(socket, enderecoEmissor, portaEmissor, -1);

            int expectedSeqNum = 0;
            int ultimoAckEnviado = -1;

            long totalRecebidosEmOrdem = 0;
            long totalEntreguesComSucesso = 0;
            long totalForaDeOrdem = 0;
            long totalDescartadosSimulado = 0;

            try (FileOutputStream fos = new FileOutputStream(pathDestino)) {
                MessageDigest md5 = obterMd5();

                boolean transferenciaConcluida = false;
                byte[] buf = new byte[TAMANHO_BUFFER];

                while (!transferenciaConcluida) {
                    DatagramPacket dg = new DatagramPacket(buf, buf.length);
                    socket.receive(dg);

                    Pacote pacote = Pacote.deserializar(dg.getData(), dg.getLength());

                    if (pacote.getTipo() == Pacote.TIPO_FIN) {
                        System.out.println("[Receptor] Pacote FIN recebido. Encerrando transferência.");
                        transferenciaConcluida = true;
                        continue;
                    }

                    if (pacote.getTipo() != Pacote.TIPO_DATA) {
                        continue;
                    }

                    if (pacote.getNumSeq() != expectedSeqNum) {
                        totalForaDeOrdem++;
                        enviarAck(socket, dg.getAddress(), dg.getPort(), ultimoAckEnviado);
                        continue;
                    }

                    totalRecebidosEmOrdem++;

                    double r = random.nextDouble();
                    if (r < probPerda) {
                        totalDescartadosSimulado++;
                        System.out.println("[Receptor] Pacote seq=" + pacote.getNumSeq()
                                + " descartado (perda simulada, r=" + String.format("%.3f", r) + ")");
                        continue;
                    }

                    fos.write(pacote.getDados());
                    if (md5 != null) {
                        md5.update(pacote.getDados());
                    }
                    totalEntreguesComSucesso++;

                    ultimoAckEnviado = expectedSeqNum;
                    enviarAck(socket, dg.getAddress(), dg.getPort(), ultimoAckEnviado);
                    expectedSeqNum++;

                    if (totalEntreguesComSucesso % 50 == 0) {
                        System.out.println("[Receptor] " + totalEntreguesComSucesso + " pacotes recebidos com sucesso...");
                    }
                }

                fos.flush();

                long totalPacotesProcessados = totalRecebidosEmOrdem + totalForaDeOrdem;
                double taxaPerdaEfetiva = totalRecebidosEmOrdem == 0
                        ? 0.0
                        : (double) totalDescartadosSimulado / totalRecebidosEmOrdem;

                System.out.println();
                System.out.println("===== Estatísticas do Receptor =====");
                System.out.println("Arquivo salvo em: " + pathDestino);
                System.out.println("Total de pacotes DATA processados (em ordem + fora de ordem): " + totalPacotesProcessados);
                System.out.println("Total de pacotes recebidos em ordem: " + totalRecebidosEmOrdem);
                System.out.println("Total de pacotes fora de ordem (descartados pela FSM): " + totalForaDeOrdem);
                System.out.println("Total de pacotes entregues com sucesso: " + totalEntreguesComSucesso);
                System.out.println("Total de pacotes descartados (perda simulada): " + totalDescartadosSimulado);
                System.out.printf("Taxa de perda efetiva: %.4f (configurada: %.4f)%n", taxaPerdaEfetiva, probPerda);

                if (md5 != null) {
                    String hashHex = bytesParaHex(md5.digest());
                    System.out.println("Hash MD5 do arquivo recebido: " + hashHex);
                }
                System.out.println("=====================================");
            }

        } catch (SocketException e) {
            System.err.println("[Receptor] Erro ao abrir socket: " + e.getMessage());
        }
    }

    private void enviarAck(DatagramSocket socket, java.net.InetAddress endereco, int porta, int numAck) throws IOException {
        Pacote ack = Pacote.criarAck(numAck);
        byte[] bytes = ack.serializar();
        DatagramPacket dg = new DatagramPacket(bytes, bytes.length, endereco, porta);
        socket.send(dg);
    }

    private MessageDigest obterMd5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            System.err.println("[Receptor] MD5 não disponível, verificação de integridade desativada.");
            return null;
        }
    }

    private String bytesParaHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
