import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { connect, type Options } from 'amqplib';
import type { RabbitConnection, RabbitConnectionSource } from './connection-source';

/** Callback executado a cada (re)conexao — usado por consumidores para re-assinar. */
export type OnReady = (connection: RabbitConnection) => void | Promise<void>;

/** Opcoes de comportamento do manager. */
export interface RabbitConnectionOptions {
    /** Tempo de espera antes de tentar reconectar (ms). Default 5000. */
    reconnectMs?: number;
}

/**
 * Gerencia UMA Connection do RabbitMQ (long-lived, com reconexao automatica) — o
 * equivalente em Nest de uma Connection injetada por vhost (`@VHostX`) do projeto
 * Java. O {@link RabbitMQModule} cria uma instancia por vhost.
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

    private connection: RabbitConnection | null = null;
    private connecting: Promise<RabbitConnection> | null = null;
    private closing = false;

    private readonly connectArg: string | Options.Connect;
    private readonly readyCbs: OnReady[] = [];

    constructor(
        private readonly vhost: string,
        source: RabbitConnectionSource,
        options: RabbitConnectionOptions = {},
    ) {
        this.logger = new Logger(`${RabbitConnectionManager.name}[${vhost}]`);
        this.reconnectMs = options.reconnectMs ?? 5000;

        const invalido = !source || (typeof source !== 'string' && !source.host);
        if (invalido) {
            throw new Error(`Conexao RabbitMQ ausente/incompleta para o vhost "${vhost}"`);
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
    }

    async onModuleInit(): Promise<void> {
        await this.open().catch((e) => this.logger.error(`Falha inicial ao conectar: ${String(e)}`));
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
        if (this.connecting) return this.connecting;

        this.connecting = connect(this.connectArg)
            .then(async (conn) => {
                this.connection = conn;
                this.connecting = null;
                this.logger.log('Conectado ao RabbitMQ');

                conn.on('error', (err: Error) => this.logger.warn(`Erro na conexao: ${err?.message}`));
                conn.on('close', () => {
                    this.connection = null;
                    if (!this.closing) {
                        this.logger.warn(`Conexao fechada; reconectando em ${this.reconnectMs}ms`);
                        setTimeout(() => void this.open().catch(() => undefined), this.reconnectMs);
                    }
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
}
