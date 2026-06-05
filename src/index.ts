// Conexao
export type {
    RabbitConnection,
    RabbitConnectionConfig,
    RabbitConnectionSource,
} from './connection/connection-source';
export {
    RabbitConnectionManager,
    type OnReady,
    type RabbitConnectionOptions,
} from './connection/rabbit-connection';
export { rabbitToken, InjectRabbit, createVhostInject } from './connection/rabbit.decorators';
export {
    RabbitMQModule,
    type RegisterOptions,
    type RegisterAsyncOptions,
} from './connection/rabbitmq.module';

// Publicacao
export { type RabbitExchange, exchange } from './publish/rabbit-exchange';
export { publish, enviar, type PublishOptions } from './publish/rabbit-publish';

// Consumo
export { RabbitMQBaseListener } from './consume/rabbit-listener';
export type {
    ListenerOptions,
    ListenerBinding,
    DeadLetterOptions,
    MessageContext,
    ExchangeType,
} from './consume/listener-options';

// Config helpers
export { collectConnections, num, type CollectConnectionsOptions } from './config/env';

