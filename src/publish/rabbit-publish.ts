import type { ConfirmChannel, Options } from 'amqplib';
import type { RabbitConnection } from '../connection/connection-source';
import type { RabbitExchange } from './rabbit-exchange';

/** Opcoes do {@link publish}. */
export interface PublishOptions extends Options.Publish {
    /**
     * Injeta `{ exchange, routingKey }` no corpo da mensagem (como o helper Java faz).
     * Default: `true` (mantem o contrato historico). Passe `false` para corpo puro.
     */
    embedRouting?: boolean;
}

/**
 * Espelha `infra.rabbitmq.RabbitMQPublish.enviar` do projeto Java:
 *   valida -> abre um canal -> publica persistente (deliveryMode 2, json) -> fecha o canal.
 *
 * Usa um CONFIRM channel e espera o confirm do broker (`waitForConfirms`) antes de
 * retornar — quem publica so prossegue quando a mensagem esta garantida no broker;
 * caso contrario lanca (ex.: o webhook do Stripe devolve 5xx para reenviar).
 */
export async function publish(
    connection: RabbitConnection,
    target: RabbitExchange,
    payload: Record<string, unknown>,
    options?: PublishOptions,
): Promise<void> {
    if (!connection) throw new Error('É necessário Connection para publicar mensagem');
    if (!target?.exchange) throw new Error('É necessário definir nome da exchange');
    if (!target?.routingKey) throw new Error('É necessário definir nome da routingKey');

    const { embedRouting = true, ...publishOptions } = options ?? {};

    const channel: ConfirmChannel = await connection.createConfirmChannel();
    try {
        const body = embedRouting
            ? { ...payload, exchange: target.exchange, routingKey: target.routingKey }
            : payload;
        const corpo = Buffer.from(JSON.stringify(body));
        channel.publish(target.exchange, target.routingKey, corpo, {
            contentType: 'application/json',
            persistent: true, // deliveryMode 2
            ...publishOptions,
        });
        await channel.waitForConfirms();
    } finally {
        await channel.close().catch(() => undefined);
    }
}

/** Alias historico (estilo Java `enviar`). */
export const enviar = publish;
