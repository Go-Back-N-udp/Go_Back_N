import java.nio.ByteBuffer;
import java.util.Arrays;

public class Pacote {

    public static final byte TIPO_DATA = 0;
    public static final byte TIPO_ACK = 1;
    public static final byte TIPO_HANDSHAKE = 2;
    public static final byte TIPO_FIN = 3;

    public static final int MAX_DADOS = 1024;
    public static final int TAMANHO_CABECALHO = 1 + 4 + 4 + 2;
    public static final int TAMANHO_MAX_PACOTE = TAMANHO_CABECALHO + MAX_DADOS;

    private byte tipo;
    private int numSeq;
    private int numAck;
    private short tamanhoDados;
    private byte[] dados;

    public Pacote(byte tipo, int numSeq, int numAck, byte[] dados) {
        this.tipo = tipo;
        this.numSeq = numSeq;
        this.numAck = numAck;
        this.dados = dados == null ? new byte[0] : dados;
        this.tamanhoDados = (short) this.dados.length;
    }

    public byte getTipo() {
        return tipo;
    }

    public int getNumSeq() {
        return numSeq;
    }

    public int getNumAck() {
        return numAck;
    }

    public short getTamanhoDados() {
        return tamanhoDados;
    }

    public byte[] getDados() {
        return dados;
    }

    public byte[] serializar() {
        ByteBuffer buffer = ByteBuffer.allocate(TAMANHO_CABECALHO + dados.length);
        buffer.put(tipo);
        buffer.putInt(numSeq);
        buffer.putInt(numAck);
        buffer.putShort(tamanhoDados);
        buffer.put(dados);
        return buffer.array();
    }

    public static Pacote deserializar(byte[] bytesRecebidos, int tamanho) {
        ByteBuffer buffer = ByteBuffer.wrap(bytesRecebidos, 0, tamanho);
        byte tipo = buffer.get();
        int numSeq = buffer.getInt();
        int numAck = buffer.getInt();
        short tamanhoDados = buffer.getShort();

        byte[] dados = new byte[tamanhoDados];
        if (tamanhoDados > 0) {
            buffer.get(dados, 0, tamanhoDados);
        }

        return new Pacote(tipo, numSeq, numAck, dados);
    }

    public static Pacote criarData(int numSeq, byte[] dados) {
        return new Pacote(TIPO_DATA, numSeq, -1, dados);
    }

    public static Pacote criarAck(int numAck) {
        return new Pacote(TIPO_ACK, -1, numAck, null);
    }

    public static Pacote criarFin(int numSeq) {
        return new Pacote(TIPO_FIN, numSeq, -1, null);
    }

    public static Pacote criarHandshake(double probPerda, String pathDestino, long tamanhoArquivo) {
        String payload = probPerda + "|" + pathDestino + "|" + tamanhoArquivo;
        return new Pacote(TIPO_HANDSHAKE, -1, -1, payload.getBytes());
    }

    public String getPayloadComoTexto() {
        return new String(dados, 0, tamanhoDados);
    }

    @Override
    public String toString() {
        return "Pacote{tipo=" + tipo + ", seq=" + numSeq + ", ack=" + numAck
                + ", tamanhoDados=" + tamanhoDados + "}";
    }
}
