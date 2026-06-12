import type { RabbitConnectionConfig, RabbitConnectionSource } from '../connection/connection-source';

/** Parse numerico com fallback (rejeita <= 0 e NaN). */
export function num(value: string | undefined, fallback: number): number {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : fallback;
}

/** Opcoes do {@link collectConnections}. */
export interface CollectConnectionsOptions {
    /** Vhost que recebe a `RABBITMQ_URL` (sem sufixo). Ex.: `'stripe'`. Sem isso, ela e ignorada. */
    primaryVhost?: string;
    /** Fonte de variaveis. Default: `process.env`. */
    env?: NodeJS.ProcessEnv;
}

/**
 * Conexoes RabbitMQ por vhost a partir do ambiente (cada uma vira uma Connection
 * injetavel via `@VHost...`). Suporta dois estilos.
 *
 * Estilo BLOCO (recomendado, tem prioridade):
 *   RABBITMQ__<VHOST>__HOST / __PORT / __VHOST / __USERNAME / __PASSWORD
 *   (opcionais __PROTOCOL=amqps, __HEARTBEAT=15)
 *
 * Estilo URL crua:
 *   RABBITMQ_URL=amqp://user:pass@host:5672/vhost        -> vhost `primaryVhost`
 *   RABBITMQ_URL__<VHOST>=amqp://...                     -> vhost `<vhost>`
 */
export function collectConnections(
    options: CollectConnectionsOptions = {},
): Record<string, RabbitConnectionSource> {
    const env = options.env ?? process.env;
    const out: Record<string, RabbitConnectionSource> = {};

    // 1) URL crua.
    const urlPrefix = 'RABBITMQ_URL__';
    for (const [key, value] of Object.entries(env)) {
        if (key.startsWith(urlPrefix) && value?.trim()) {
            out[key.slice(urlPrefix.length).toLowerCase()] = value.trim();
        }
    }
    const primaryUrl = (env.RABBITMQ_URL ?? '').trim();
    if (primaryUrl && options.primaryVhost) {
        out[options.primaryVhost.toLowerCase()] = primaryUrl;
    }

    // 2) Blocos host/port/vhost/user/pass: RABBITMQ__<NOME>__<CAMPO> (tem prioridade).
    const blockPrefix = 'RABBITMQ__';
    const blocos: Record<string, Record<string, string>> = {};
    for (const [key, value] of Object.entries(env)) {
        if (!key.startsWith(blockPrefix) || value == null) continue;
        const [nome, campo] = key.slice(blockPrefix.length).split('__');
        if (!nome || !campo) continue;
        (blocos[nome.toLowerCase()] ??= {})[campo.toLowerCase()] = value.trim();
    }
    for (const [nome, f] of Object.entries(blocos)) {
        const config: RabbitConnectionConfig = {
            protocol: f.protocol === 'amqps' ? 'amqps' : 'amqp',
            host: f.host || 'localhost',
            port: num(f.port, 5672),
            vhost: f.vhost || '/',
            username: f.username || 'guest',
            password: f.password || 'guest',
            heartbeat: num(f.heartbeat, 15),
        };
        out[nome] = config;
    }

    return out;
}
