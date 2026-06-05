import type { Channel, ConsumeMessage, Options } from 'amqplib';

/** Tipo de exchange suportado pelo `assertExchange`. */
export type ExchangeType = 'topic' | 'direct' | 'fanout' | 'headers';

/** Binding de uma fila a um exchange por uma ou mais routing keys. */
export interface ListenerBinding {
    exchange: string;
    /** Default: `'topic'`. */
    type?: ExchangeType;
    /** Use `['#']` para receber TODAS as mensagens (topic). */
    routingKeys: string[];
    /** Opcoes do `assertExchange`. Default: `{ durable: true }`. */
    options?: Options.AssertExchange;
}

/** Configuracao da dead-letter queue (mensagens que falharam ao processar). */
export interface DeadLetterOptions {
    /** Nome da DLQ. Default: `${queue}.dlq`. */
    queue?: string;
    /** Exchange de dead-letter. Default: `''` (exchange default, roteia por nome de fila). */
    exchange?: string;
    /** Routing key de dead-letter. Default: nome da DLQ. */
    routingKey?: string;
}

/** Configuracao declarativa de um listener (retornada por `options()`). */
export interface ListenerOptions {
    /** Nome da fila a consumir. */
    queue: string;
    /**
     * Nº de mensagens nao-confirmadas entregues por vez (back-pressure / `basicQos`).
     * Default: 10. Minimo aplicado: 1.
     */
    prefetch?: number;
    /**
     * Declara a topologia (exchange/queue/bindings/DLQ) no setup. Default: `true`.
     * Use `false` para apenas consumir uma fila pre-existente.
     */
    assert?: boolean;
    /** Opcoes do `assertQueue` da fila principal. Default: `{ durable: true }`. */
    queueOptions?: Options.AssertQueue;
    /** Exchanges/routing keys a vincular a fila. */
    bindings?: ListenerBinding[];
    /**
     * Dead-letter da fila principal. Default: cria `${queue}.dlq`.
     * Passe `false` para desativar (mensagens com erro somem se `requeueOnError` for false).
     */
    deadLetter?: DeadLetterOptions | false;
    /**
     * Em caso de erro no handler (sem ack/nack manual), reenfileira a mensagem?
     * Default: `false` (vai para a DLQ; evita hot-loop).
     */
    requeueOnError?: boolean;
}

/** Contexto entregue ao handler de cada mensagem. */
export interface MessageContext {
    /** O canal AMQP (caso precise de operacoes avancadas). */
    readonly channel: Channel;
    /** A mensagem bruta do amqplib. */
    readonly message: ConsumeMessage;
    /** `messageId` da mensagem (ex.: id de evento usado para dedup). */
    readonly messageId: string | undefined;
    /** Confirma a mensagem. Sem chamada manual, o base faz ack automatico no sucesso. */
    ack(): void;
    /** Rejeita a mensagem. `requeue=false` (default) envia para a DLQ. */
    nack(requeue?: boolean): void;
    /** Parseia o corpo como JSON. */
    json<T = unknown>(): T;
    /** Corpo como texto cru. */
    text(): string;
}
