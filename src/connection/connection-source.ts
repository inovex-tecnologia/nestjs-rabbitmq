import type { ChannelModel } from 'amqplib';

/**
 * A Connection do amqplib (o que `connect()` devolve). Mesmo papel do
 * `com.rabbitmq.client.Connection` do projeto Java.
 */
export type RabbitConnection = ChannelModel;

/**
 * Bloco de conexao RabbitMQ (host/port/vhost/user/pass) — espelha o
 * `vHostX { host, port, vhost, username, password }` do HOCON do projeto Java.
 */
export interface RabbitConnectionConfig {
    protocol: 'amqp' | 'amqps';
    host: string;
    port: number;
    vhost: string;
    username: string;
    password: string;
    /** Heartbeat em segundos (0 desativa). Default sugerido: 15. */
    heartbeat: number;
}

/** Aceita URL crua (`amqp://user:pass@host:5672/vhost`) ou o bloco acima. */
export type RabbitConnectionSource = string | RabbitConnectionConfig;
