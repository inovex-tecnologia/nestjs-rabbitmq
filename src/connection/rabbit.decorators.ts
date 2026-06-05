import { Inject } from '@nestjs/common';

/**
 * Token DI de uma conexao por vhost (1 provider por nome no {@link RabbitMQModule}).
 * `rabbitToken('stripe')` -> `'RABBIT_CONNECTION_STRIPE'`.
 */
export const rabbitToken = (vhost: string): string => `RABBIT_CONNECTION_${vhost.toUpperCase()}`;

/** Injeta o {@link RabbitConnectionManager} de um vhost por nome (uso generico). */
export const InjectRabbit = (vhost: string): ParameterDecorator => Inject(rabbitToken(vhost));

/**
 * Fabrica um decorator no estilo `@VHostX` do projeto Java, fixando o nome do vhost.
 *
 *   export const VHostStripe = createVhostInject('stripe');
 *   constructor(@VHostStripe() private readonly rabbit: RabbitConnectionManager) {}
 */
export const createVhostInject = (vhost: string): (() => ParameterDecorator) => {
    return () => InjectRabbit(vhost);
};
