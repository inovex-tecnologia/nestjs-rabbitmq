
export interface RabbitExchange {
    readonly exchange: string;
    readonly routingKey: string;
}

/** Acucar para criar um {@link RabbitExchange}. */
export function exchange(name: string, routingKey: string): RabbitExchange {
    return { exchange: name, routingKey };
}
