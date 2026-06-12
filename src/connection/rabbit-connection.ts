import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { connect, type Options } from 'amqplib';
import type { RabbitConnection, RabbitConnectionSource } from './connection-source';

/** Callback executado a cada (re)conexao — usado por consumidores para re-assinar. */
export type OnReady = (connection: RabbitConnection) => void | Promise<void>;

/** Opcoes de comportamento do manager. */
export interface RabbitConnectionOptions {
    /** Espera BASE antes de reconectar (ms). Default 1000. Cresce exponencialmente. */
    reconnectMs?: number;
    /** Teto da espera de reconexao (ms). Default 30000. */
    reconnectMaxMs?: number;
}

/**
 * Gerencia UMA Connection do RabbitMQ (long-lived, com reconexao automatica) 
 *
 * - Para PUBLICAR: pegue a conexao com {@link get} e use o helper `publish()`.
 * - Para CONSUMIR: registre um setup com {@link onReady}; ele roda na conexao atual e
 *   a cada reconexao (re-declara topologia + reabre o consume), pois um channel/consume
 *   morre junto com a conexao.
 */
@Injectable()
export class RabbitConnectionManager implements OnModuleInit, OnModuleDestroy {
    private readonly logger: Logger;
    private readonly reconnectMs: number;
    private readonly reconnectMaxMs: number;

    private connection: RabbitConnection | null = null;
    private connecting: Promise<RabbitConnection> | null = null;
    private closing = false;
    private reconnectAttempts = 0;

    private readonly connectArg: string | Options.Connect;
    private readonly readyCbs: OnReady[] = [];

    /** Descricao do alvo SEM a senha — usada nas mensagens de erro/log. */
    private readonly alvo: string;

    constructor(
        private readonly vhost: string,
        source: RabbitConnectionSource,
        options: RabbitConnectionOptions = {},
    ) {
        this.logger = new Logger(`${RabbitConnectionManager.name}[${vhost}]`);
        this.reconnectMs = options.reconnectMs ?? 1000;
        this.reconnectMaxMs = options.reconnectMaxMs ?? 30_000;

        const invalido = !source || (typeof source !== 'string' && !source.host);
        if (invalido) {
            throw new Error(`Conexao RabbitMQ ausente/incompleta para o vhost "${vhost}"`);
        }
        // Falha cedo em vez de tentar logar como guest e estourar ACCESS_REFUSED em runtime.
        if (typeof source !== 'string' && (!source.username || !source.password)) {
            throw new Error(
                `Credenciais RabbitMQ ausentes para o vhost "${vhost}" — ` +
                    'defina RABBITMQ__<NOME>__USERNAME e __PASSWORD.',
            );
        }

        this.connectArg =
            typeof source === 'string'
                ? source
                : {
                      protocol: source.protocol,
                      hostname: source.host,
                      port: source.port,
                      username: source.username,
                      password: source.password,
                      vhost: source.vhost,
                      heartbeat: source.heartbeat,
                  };

        this.alvo = descreverAlvo(this.connectArg);
    }

    async onModuleInit(): Promise<void> {
        await this.open().catch((e) => {
            this.logger.error(`Falha inicial ao conectar (${this.alvo}):\n${explicarErro(e)}`);
            // Sem isso, broker fora do ar no boot deixaria consumidores offline pra sempre.
            if (!this.closing) this.scheduleReconnect();
        });
    }

    async onModuleDestroy(): Promise<void> {
        this.closing = true;
        await this.connection?.close().catch(() => undefined);
        this.connection = null;
    }

    /** Conexao pronta para uso; (re)conecta sob demanda. */
    async get(): Promise<RabbitConnection> {
        return this.connection ?? this.open();
    }

    /**
     * Registra um setup que roda na conexao atual (se ja conectada) e a cada
     * (re)conexao. Use em consumidores para (re)declarar topologia e reabrir o consume.
     */
    onReady(cb: OnReady): void {
        this.readyCbs.push(cb);
        if (this.connection) void this.runReady(cb, this.connection);
    }

    private async runReady(cb: OnReady, conn: RabbitConnection): Promise<void> {
        try {
            await cb(conn);
        } catch (e) {
            this.logger.error(`Erro no setup onReady: ${String(e)}`);
        }
    }

    private open(): Promise<RabbitConnection> {
        // Evita conexao duplicada (ex.: timer de reconexao disparando depois que um
        // get() ja reconectou — a conexao antiga vazaria aberta).
        if (this.connection) return Promise.resolve(this.connection);
        if (this.connecting) return this.connecting;

        this.connecting = connect(this.connectArg)
            .then(async (conn) => {
                this.connection = conn;
                this.connecting = null;
                this.reconnectAttempts = 0;
                this.logger.log('Conectado ao RabbitMQ');

                conn.on('error', (err: Error) => this.logger.warn(`Erro na conexao: ${err?.message}`));
                conn.on('close', () => {
                    this.connection = null;
                    if (!this.closing) this.scheduleReconnect();
                });

                // Re-roda os setups (consumidores re-declaram topologia e reabrem consume).
                for (const cb of this.readyCbs) await this.runReady(cb, conn);

                return conn;
            })
            .catch((err) => {
                this.connecting = null;
                throw err;
            });

        return this.connecting;
    }

    /**
     * Agenda nova tentativa com backoff exponencial + jitter (evita thundering herd
     * quando o broker volta e N instancias reconectam juntas). Se a tentativa falhar,
     * REAGENDA — sem isso, uma unica falha de reconexao mataria o loop e consumidores
     * ficariam offline ate o restart do processo.
     */
    private scheduleReconnect(): void {
        const delay = this.nextReconnectDelay();
        this.logger.warn(`Conexao fechada; reconectando em ${delay}ms`);
        setTimeout(() => {
            if (this.closing) return;
            void this.open().catch((e) => {
                this.logger.error(`Falha ao reconectar (${this.alvo}):\n${explicarErro(e)}`);
                if (!this.closing) this.scheduleReconnect();
            });
        }, delay);
    }

    /** Backoff exponencial com teto + jitter de ate 20%. */
    private nextReconnectDelay(): number {
        const base = Math.min(this.reconnectMaxMs, this.reconnectMs * 2 ** this.reconnectAttempts);
        this.reconnectAttempts = Math.min(this.reconnectAttempts + 1, 10);
        const withJitter = base * (1 + 0.2 * Math.random());
        return Math.min(this.reconnectMaxMs, Math.round(withJitter));
    }
}

/**
 * Descreve o alvo da conexao SEM expor a senha — ex.:
 * `amqp://stripe-ingress@rabbitmq.railway.internal:5672 vhost="stripe"`.
 * Usado nas mensagens de erro pra deixar claro PARA ONDE a app tentou conectar.
 */
function descreverAlvo(arg: string | Options.Connect): string {
    try {
        if (typeof arg === 'string') {
            const u = new URL(arg);
            const vhost = decodeURIComponent(u.pathname.replace(/^\//, '')) || '/';
            const porta = u.port || (u.protocol === 'amqps:' ? '5671' : '5672');
            return `${u.protocol}//${u.username || 'guest'}@${u.hostname}:${porta} vhost="${vhost}"`;
        }
        const proto = arg.protocol ?? 'amqp';
        const porta = arg.port ?? (proto === 'amqps' ? 5671 : 5672);
        return `${proto}://${arg.username ?? 'guest'}@${arg.hostname}:${porta} vhost="${arg.vhost ?? '/'}"`;
    } catch {
        return 'alvo desconhecido';
    }
}

/**
 * Traduz o erro cru do `amqplib`/Node em uma mensagem com CAUSA PROVAVEL e
 * O QUE CHECAR. Cobre os casos mais comuns (vhost/permissao, credencial, host/porta,
 * DNS, timeout, TLS). Sempre anexa o erro original na ultima linha pra nao perder detalhe.
 */
function explicarErro(e: unknown): string {
    const err = e as { code?: string; replyCode?: number; replyText?: string; message?: string };
    const code = err?.code ?? '';
    const msg = err?.message ?? String(e);
    const replyText = err?.replyText ?? '';
    const original = `↳ erro original: ${[code, err?.replyCode, replyText].filter(Boolean).join(' ') || msg}`;

    // Handshake AMQP: servidor fechou no channel 0 ao abrir o vhost.
    if (/Expected ConnectionOpenOk/i.test(msg) || /NOT_ALLOWED/i.test(replyText) || err?.replyCode === 530) {
        return [
            'Login OK, mas o broker RECUSOU o vhost (fechou no handshake, channel 0).',
            'Causa provavel: o vhost nao existe OU o usuario nao tem permissao nele.',
            'Checar: 1) o vhost existe? (Admin -> Virtual Hosts)',
            '        2) o usuario tem Set permission NESSE vhost? (Admin -> Users -> usuario)',
            '        3) RABBITMQ__<NOME>__VHOST bate exatamente com o nome criado.',
            original,
        ].join('\n');
    }

    // Credencial errada (login recusado).
    if (/ACCESS_REFUSED/i.test(msg) || /ACCESS_REFUSED/i.test(replyText) || err?.replyCode === 403 || /Handshake terminated by server/i.test(msg)) {
        return [
            'Broker RECUSOU o login (usuario ou senha invalidos).',
            'Checar: RABBITMQ__<NOME>__USERNAME e __PASSWORD batem com o usuario do broker.',
            '        O usuario nao foi removido/desabilitado.',
            original,
        ].join('\n');
    }

    // Conexao recusada — broker fora do ar ou porta errada.
    if (code === 'ECONNREFUSED') {
        return [
            'Conexao RECUSADA (ninguem escutando no host:porta).',
            'Causa provavel: broker fora do ar OU porta errada (AMQP=5672, NAO use a 15672 do painel).',
            'Checar: RABBITMQ__<NOME>__HOST e __PORT; o servico RabbitMQ esta de pe.',
            original,
        ].join('\n');
    }

    // DNS nao resolve o host.
    if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') {
        return [
            'Nao foi possivel RESOLVER o host (DNS).',
            'Causa provavel: host errado OU host interno (*.railway.internal) usado fora da rede privada.',
            'Checar: RABBITMQ__<NOME>__HOST; rodando no mesmo projeto/rede que o broker.',
            original,
        ].join('\n');
    }

    // Timeout — firewall/porta bloqueada ou host inalcancavel.
    if (code === 'ETIMEDOUT' || code === 'ECONNTIMEOUT') {
        return [
            'TIMEOUT ao conectar (host inalcancavel).',
            'Causa provavel: firewall/security group bloqueando a porta, ou host/porta errados.',
            'Checar: rede entre a app e o broker; a porta AMQP esta liberada.',
            original,
        ].join('\n');
    }

    // Reset — frequentemente mismatch de TLS (amqp x amqps).
    if (code === 'ECONNRESET') {
        return [
            'Conexao RESETADA pelo servidor.',
            'Causa provavel: incompatibilidade de TLS — usando amqp:// numa porta TLS (ou amqps:// numa porta sem TLS).',
            'Checar: RABBITMQ__<NOME>__PROTOCOL (amqp vs amqps) e a porta correspondente.',
            original,
        ].join('\n');
    }

    // Fallback: erro nao mapeado.
    return `Falha ao conectar no RabbitMQ.\n${original}`;
}
