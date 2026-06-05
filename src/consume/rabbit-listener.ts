import { Logger, type OnModuleInit } from '@nestjs/common';
import type { Channel, ConsumeMessage } from 'amqplib';
import { RabbitConnectionManager } from '../connection/rabbit-connection';
import type { RabbitConnection } from '../connection/connection-source';
import type { ListenerOptions, MessageContext } from './listener-options';

/**
 * Base abstrato de consumidor RabbitMQ — o equivalente em Nest do
 * `infra.rabbitmq.impl.RabbitMQBaseListener` do projeto Java.
 *
 * O subclass declara a fila/topologia em {@link options} e trata a mensagem em
 * {@link handle}. O base cuida de tudo o que era boilerplate:
 *
 *  - registra um setup no {@link RabbitConnectionManager.onReady} (roda agora e a cada
 *    reconexao: re-declara topologia + reabre o consume — channel/consume morrem com a conexao);
 *  - declara exchange(s), fila, bindings e DLQ (idempotente);
 *  - aplica `prefetch` (basicQos) para back-pressure;
 *  - ACK automatico apos o handler resolver; em erro, NACK -> DLQ (sem hot-loop).
 *
 * O handler pode chamar `ctx.ack()` / `ctx.nack()` manualmente; nesse caso o base
 * respeita a decisao e nao re-confirma.
 *
 * Uso:
 *   @Injectable()
 *   export class StripeEventsConsumer extends RabbitMQBaseListener {
 *       constructor(@VHostStripe() rabbit: RabbitConnectionManager, private p: Processor) {
 *           super(rabbit);
 *       }
 *       protected options(): ListenerOptions {
 *           return { queue: 'billing.stripe-events', prefetch: 10,
 *               bindings: [{ exchange: 'stripe.events', routingKeys: ['payment_intent.*'] }] };
 *       }
 *       protected async handle(ctx: MessageContext): Promise<void> {
 *           await this.p.process(ctx.json<{ event: unknown }>().event);
 *       }
 *   }
 */
export abstract class RabbitMQBaseListener implements OnModuleInit {
    protected readonly logger: Logger;

    protected constructor(private readonly connectionManager: RabbitConnectionManager) {
        this.logger = new Logger(this.constructor.name);
    }

    /** Declara a fila/topologia consumida. Chamado a cada (re)conexao. */
    protected abstract options(): ListenerOptions;

    /** Trata uma mensagem. Lancar (throw) provoca NACK -> DLQ. */
    protected abstract handle(ctx: MessageContext): void | Promise<void>;

    onModuleInit(): void {
        // Roda agora (se ja conectado) e a cada reconexao: (re)declara topologia + consume.
        this.connectionManager.onReady((connection) => this.setup(connection));
    }

    private async setup(connection: RabbitConnection): Promise<void> {
        const opts = this.options();
        const assert = opts.assert ?? true;
        const prefetch = Math.max(1, opts.prefetch ?? 10);

        const channel = await connection.createChannel();

        // Resolve DLQ (a menos que desativada explicitamente).
        let queueOptions = { durable: true, ...opts.queueOptions };
        if (opts.deadLetter !== false) {
            const dl = opts.deadLetter ?? {};
            const dlq = dl.queue ?? `${opts.queue}.dlq`;
            const dlx = dl.exchange ?? ''; // exchange default (roteia por nome de fila)
            const dlrk = dl.routingKey ?? dlq;
            if (assert) await channel.assertQueue(dlq, { durable: true });
            queueOptions = { deadLetterExchange: dlx, deadLetterRoutingKey: dlrk, ...queueOptions };
        }

        if (assert) {
            for (const b of opts.bindings ?? []) {
                await channel.assertExchange(b.exchange, b.type ?? 'topic', { durable: true, ...b.options });
            }
            await channel.assertQueue(opts.queue, queueOptions);
            for (const b of opts.bindings ?? []) {
                for (const rk of b.routingKeys) {
                    await channel.bindQueue(opts.queue, b.exchange, rk);
                }
            }
        }

        await channel.prefetch(prefetch);
        await channel.consume(opts.queue, (msg) => void this.dispatch(channel, msg, opts), { noAck: false });

        const rks = (opts.bindings ?? []).flatMap((b) => b.routingKeys);
        this.logger.log(`Consumindo "${opts.queue}" [${rks.join(', ')}] prefetch=${prefetch}`);
    }

    private async dispatch(channel: Channel, msg: ConsumeMessage | null, opts: ListenerOptions): Promise<void> {
        if (!msg) return; // consumidor cancelado pelo broker

        let settled = false;
        const ctx: MessageContext = {
            channel,
            message: msg,
            messageId: msg.properties.messageId,
            ack: () => {
                if (settled) return;
                settled = true;
                channel.ack(msg);
            },
            nack: (requeue = false) => {
                if (settled) return;
                settled = true;
                channel.nack(msg, false, requeue);
            },
            json: <T = unknown>() => JSON.parse(msg.content.toString()) as T,
            text: () => msg.content.toString(),
        };

        try {
            await this.handle(ctx);
            ctx.ack(); // ACK automatico se o handler nao confirmou manualmente
        } catch (err) {
            this.logger.error(`Falha ao processar ${ctx.messageId ?? '(sem id)'}: ${String(err)} -> DLQ`);
            ctx.nack(opts.requeueOnError ?? false);
        }
    }
}
